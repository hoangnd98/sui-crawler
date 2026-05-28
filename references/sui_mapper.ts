/**
 * ═══════════════════════════════════════════════════════════════
 * SUI MAPPERS
 * ═══════════════════════════════════════════════════════════════
 *
 * STAGE 1 (Currently Used):
 * - toSuiCheckpointCH()
 * - toSuiTransactionCH()
 *
 * FUTURE (Object Pipeline):
 * - toSuiTransactionObjectCH()
 * - getObjectStatus()
 */

/**
 * Maps Checkpoint to ClickHouse SuiCheckpointCH model
 * @stage Stage 1 - Currently used
 */
export function toSuiCheckpointCH(checkpoint: Checkpoint): SuiCheckpointCH {
	return {
		sequence_number: checkpoint.sequenceNumber,
		digest: checkpoint.digest,
		// Convert ISO string to Unix timestamp (seconds) for ClickHouse DateTime
		timestamp: Math.floor(new Date(checkpoint.timestamp).getTime() / 1000),
		previous_checkpoint_digest: checkpoint.previousCheckpointDigest,
		network_total_transactions: checkpoint.networkTotalTransactions,
	};
}

/**
 * Maps GraphQL Transaction to ClickHouse SuiTransactionCH model.
 *
 * Stage 1: Raw Data Ingestion
 * - Stores only minimal, essential data as JSON
 * - No classification or parsing
 * - Optimized for storage efficiency
 *
 * @stage Stage 1 - Currently used
 */
export function toSuiTransactionCH(transaction: Transaction): SuiTransactionCH {
	const {
		digest,
		sender,
		kind,
		effects: {
			status: effectStatus,
			timestamp,
			checkpoint: { sequenceNumber },
			effectsJson,
			events,
			balanceChanges,
			objectChanges,
		},
	} = transaction;

	const status =
		effectStatus === 'SUCCESS'
			? SuiTransactionStatus.SUCCESS
			: SuiTransactionStatus.FAILURE;

	// Calculate gas fee
	let gasFee = 0;
	if (effectsJson?.gasUsed) {
		const { computationCost, storageCost, storageRebate } = effectsJson.gasUsed;
		gasFee =
			Number(computationCost) + Number(storageCost) - Number(storageRebate);
		if (gasFee < 0) gasFee = 0;
	}

	return {
		digest,
		checkpoint_sequence_number: sequenceNumber,
		timestamp: Math.floor(new Date(timestamp).getTime() / 1000),
		sender: sender ? sender.address : null,
		status,
		kind_typename: kind?.__typename || 'Unknown',

		// Store minimal JSON data (only fields needed for classification)
		commands_json: JSON.stringify(
			extractCommandsForClassification(kind?.commands),
		),
		inputs_json: JSON.stringify(extractInputsForClassification(kind?.inputs)),
		events_json: JSON.stringify(extractEventsForClassification(events)),
		balance_changes_json: JSON.stringify(
			extractBalanceChangesForClassification(balanceChanges),
		),
		// object_changes_json removed - data stored in SuiTransactionObjectCH table

		// Calculated gas fee
		gas_fee: gasFee,
	};
}

function toCommandInputRef(ref: any): CommandInputRef {
	const result: CommandInputRef = { __typename: ref.__typename };
	if (ref.ix !== undefined) result.ix = ref.ix;
	if (ref.cmd !== undefined) result.cmd = ref.cmd;

	return result;
}

/**
 * Extract only fields needed for classification from commands
 * @private
 */
function extractCommandsForClassification(
	commands: any,
): CommandForClassification[] {
	if (!commands?.nodes) {
		return [];
	}

	return commands.nodes.map((cmd: any) => {
		const result: CommandForClassification = {};

		// For wallet management detection
		if (cmd.__typename) {
			result.__typename = cmd.__typename;
		}

		// For function-based classification (MoveCallCommand)
		if (cmd.function) {
			result.function_name = cmd.function.name || '';
			result.package_address = cmd.function.module?.package?.address || '';
		}

		// For transfer classification (TransferObjectsCommand)
		if (cmd.__typename === 'TransferObjectsCommand') {
			if (cmd.inputs?.length) {
				result.transfer_inputs = cmd.inputs.map(toCommandInputRef);
			}
			if (cmd.address) {
				result.transfer_address = toCommandInputRef(cmd.address);
			}
		}

		return result;
	});
}

/**
 * Extract only fields needed for classification from inputs
 * @private
 */
function extractInputsForClassification(inputs: any): InputForClassification[] {
	if (!inputs?.nodes) return [];

	return inputs.nodes.map((input: any) => {
		const result: InputForClassification = { __typename: input.__typename };
		if (input.__typename === 'Pure' && input.bytes) {
			result.bytes = input.bytes;
		} else if (
			input.__typename === 'OwnedOrImmutable' &&
			input.object?.address
		) {
			result.object_address = input.object.address;
		} else if (input.__typename === 'SharedInput') {
			if (input.address) result.address = input.address;
			if (input.mutable !== undefined) result.mutable = input.mutable;
		}

		return result;
	});
}

/**
 * Extract only fields needed for classification from events
 * @private
 */
function extractEventsForClassification(events: any): EventForClassification[] {
	if (!events?.nodes) {
		return [];
	}

	return events.nodes
		.filter((event: any) => event.contents?.type?.repr)
		.map((event: any) => ({
			type: event.contents.type.repr,
			...(event.contents.json !== undefined && { json: event.contents.json }),
		}));
}

/**
 * Extract only fields needed for classification from balance changes
 * @private
 */
function extractBalanceChangesForClassification(
	balanceChanges: any,
): BalanceChangeForClassification[] {
	if (!balanceChanges?.nodes) {
		return [];
	}

	return balanceChanges.nodes.map((change: any) => ({
		coin_type: change.coinType?.repr || '',
		amount: change.amount || '0',
		owner_address: change.owner?.address || null,
	}));
}

// ═══════════════════════════════════════════════════════════════
// FUTURE: Object Pipeline (Not Currently Used in Stage 1)
// ═══════════════════════════════════════════════════════════════

/**
 * Maps Transaction to array of SuiTransactionObjectCH
 * Stores raw object change data - object_status can be calculated later from these fields
 * @stage Future - For object pipeline
 */
export function toSuiTransactionObjectCH(
	transaction: Transaction,
): SuiTransactionObjectCH[] {
	const {
		digest,
		effects: { timestamp, objectChanges },
	} = transaction;

	const results: SuiTransactionObjectCH[] = [];
	if (objectChanges?.nodes) {
		for (const node of objectChanges.nodes) {
			const { inputState, outputState, idCreated, idDeleted, address } = node;

			const version = outputState?.version ?? inputState?.version;
			const finalVersion = version ?? 0;

			// Skip Read-Only objects (version 0)
			if (finalVersion === 0) continue;

			// Skip Unchanged objects (same digest)
			if (
				inputState?.digest &&
				outputState?.digest &&
				inputState.digest === outputState.digest
			) {
				continue;
			}

			results.push({
				object_id: address,
				version: finalVersion,
				transaction_digest: digest,

				// Input state
				input_version: inputState?.version ?? 0,
				input_owner: inputState?.address ?? null,
				input_digest: inputState?.digest ?? null,

				// Output state
				output_version: outputState?.version ?? 0,
				output_owner: outputState?.address ?? null,
				output_digest: outputState?.digest ?? null,

				// Lifecycle flags (convert boolean to UInt8: 1 = true, 0 = false)
				id_created: idCreated ? 1 : 0,
				id_deleted: idDeleted ? 1 : 0,

				timestamp: Math.floor(new Date(timestamp).getTime() / 1000),
			});
		}
	}

	return results;
}

/**
 * Maps SuiObject to SuiObjectCH
 * @stage Future - For object pipeline
 */
export function toSuiObjectCH(object: SuiObject): SuiObjectCH {
	// Extract type
	const type_ = object.asMoveObject?.contents?.type?.repr || '';

	// Extract coin information
	let coinBalance = BigInt(0);

	const contents = object.asMoveObject?.contents;
	if (contents?.json && 'balance' in contents.json) {
		const raw = contents.json.balance;
		// Sui GraphQL returns balance as { value: "..." } for Coin objects
		const balanceValue =
			typeof raw === 'object' && raw !== null ? raw.value : raw;
		coinBalance = BigInt(balanceValue ?? 0);
	}

	// Extract display information (NFTs)
	let displayName: string | null = null;
	let displayImageUrl: string | null = null;

	if (contents?.display?.output) {
		// output is generic JSON, usually has name and image_url
		const output = contents.display.output;
		if (output && typeof output === 'object') {
			if ('name' in output) displayName = String(output.name);
			if ('image_url' in output) displayImageUrl = String(output.image_url);
		}
	}

	// Extract owner information
	let ownerType = 'Unknown';
	let ownerAddress: string | null = null;

	if (object.owner) {
		// Keep raw typename (AddressOwner, ObjectOwner, Shared, Immutable, ConsensusAddressOwner)
		ownerType = object.owner.__typename;

		if (
			ownerType === 'AddressOwner' ||
			ownerType === 'ObjectOwner' ||
			ownerType === 'ConsensusAddressOwner'
		) {
			ownerAddress = object.owner.address?.address || null;
		}
		// Shared and Immutable have no specific address in this context
	}

	return {
		object_id: object.address,
		version: object.version,
		type_,
		coin_balance: Number(coinBalance),
		display_name: displayName,
		display_image_url: displayImageUrl,
		owner_type: ownerType,
		owner_address: ownerAddress,
		timestamp: Math.floor(Date.now() / 1000),
	};
}
