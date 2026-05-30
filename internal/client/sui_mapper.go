package client

import (
	"encoding/json"
	"sort"
	"time"

	v2 "github.com/open-move/sui-go-sdk/proto/sui/rpc/v2"

	"sui-crawler/internal/models"
)

const zeroSuiAddress = "0x0000000000000000000000000000000000000000000000000000000000000000"

func transactionKindTypename(tx *v2.Transaction) string {
	if tx == nil || tx.Kind == nil {
		return "Unknown"
	}
	if tx.GetSender() == zeroSuiAddress && tx.Kind.GetProgrammableTransaction() != nil {
		return "ProgrammableSystemTransaction"
	}

	switch tx.Kind.GetData().(type) {
	case *v2.TransactionKind_ProgrammableTransaction:
		return "ProgrammableTransaction"
	case *v2.TransactionKind_ChangeEpoch:
		return "ChangeEpochTransaction"
	case *v2.TransactionKind_Genesis:
		return "GenesisTransaction"
	case *v2.TransactionKind_ConsensusCommitPrologue:
		return "ConsensusCommitPrologueTransaction"
	case *v2.TransactionKind_AuthenticatorStateUpdate:
		return "AuthenticatorStateUpdateTransaction"
	case *v2.TransactionKind_EndOfEpoch:
		return "EndOfEpochTransaction"
	case *v2.TransactionKind_RandomnessStateUpdate:
		return "RandomnessStateUpdateTransaction"
	default:
		return "Unknown"
	}
}

func transactionCommandsJSON(tx *v2.Transaction) string {
	pt := programmableTransactionForClassification(tx)
	if pt == nil {
		return "[]"
	}

	rows := make([]map[string]any, 0, len(pt.GetCommands()))
	for _, command := range pt.GetCommands() {
		rows = append(rows, commandForClassification(command))
	}
	return mustJSON(rows)
}

func transactionInputsJSON(tx *v2.Transaction) string {
	pt := programmableTransactionForClassification(tx)
	if pt == nil {
		return "[]"
	}

	rows := make([]map[string]any, 0, len(pt.GetInputs()))
	for _, input := range pt.GetInputs() {
		rows = append(rows, inputForClassification(input))
	}
	return mustJSON(rows)
}

func programmableTransactionForClassification(tx *v2.Transaction) *v2.ProgrammableTransaction {
	if tx == nil || tx.Kind == nil || tx.GetSender() == zeroSuiAddress {
		return nil
	}
	return tx.Kind.GetProgrammableTransaction()
}

func commandForClassification(command *v2.Command) map[string]any {
	if command == nil {
		return map[string]any{"__typename": "UnknownCommand"}
	}

	switch c := command.GetCommand().(type) {
	case *v2.Command_MoveCall:
		return map[string]any{
			"__typename":      "MoveCallCommand",
			"function_name":   c.MoveCall.GetFunction(),
			"package_address": c.MoveCall.GetPackage(),
		}
	case *v2.Command_TransferObjects:
		entry := map[string]any{"__typename": "TransferObjectsCommand"}
		inputs := make([]map[string]any, 0, len(c.TransferObjects.GetObjects()))
		for _, object := range c.TransferObjects.GetObjects() {
			inputs = append(inputs, commandInputRef(object))
		}
		if len(inputs) > 0 {
			entry["transfer_inputs"] = inputs
		}
		if c.TransferObjects.GetAddress() != nil {
			entry["transfer_address"] = commandInputRef(c.TransferObjects.GetAddress())
		}
		return entry
	case *v2.Command_SplitCoins:
		return map[string]any{"__typename": "SplitCoinsCommand"}
	case *v2.Command_MergeCoins:
		return map[string]any{"__typename": "MergeCoinsCommand"}
	case *v2.Command_Publish:
		return map[string]any{"__typename": "PublishCommand"}
	case *v2.Command_MakeMoveVector:
		return map[string]any{"__typename": "MakeMoveVecCommand"}
	case *v2.Command_Upgrade:
		return map[string]any{"__typename": "UpgradeCommand"}
	default:
		return map[string]any{"__typename": "UnknownCommand"}
	}
}

func commandInputRef(arg *v2.Argument) map[string]any {
	if arg == nil {
		return map[string]any{"__typename": "Unknown"}
	}

	switch arg.GetKind() {
	case v2.Argument_GAS:
		return map[string]any{"__typename": "GasCoin"}
	case v2.Argument_INPUT:
		entry := map[string]any{"__typename": "Input"}
		if arg.Input != nil {
			entry["ix"] = arg.GetInput()
		}
		return entry
	case v2.Argument_RESULT:
		entry := map[string]any{"__typename": "TxResult"}
		if arg.Result != nil {
			entry["cmd"] = arg.GetResult()
		}
		if arg.Subresult != nil {
			entry["ix"] = arg.GetSubresult()
		}
		return entry
	default:
		return map[string]any{"__typename": "Unknown"}
	}
}

func inputForClassification(input *v2.Input) map[string]any {
	if input == nil {
		return map[string]any{"__typename": "Unknown"}
	}

	switch input.GetKind() {
	case v2.Input_PURE:
		return map[string]any{"__typename": "MoveValue"}
	case v2.Input_IMMUTABLE_OR_OWNED:
		entry := map[string]any{"__typename": "OwnedOrImmutable"}
		if input.GetObjectId() != "" {
			entry["object_address"] = input.GetObjectId()
		}
		return entry
	case v2.Input_SHARED:
		entry := map[string]any{"__typename": "SharedInput"}
		if input.GetObjectId() != "" {
			entry["address"] = input.GetObjectId()
		}
		if input.Mutable != nil {
			entry["mutable"] = input.GetMutable()
		}
		return entry
	default:
		return map[string]any{"__typename": "BalanceWithdraw"}
	}
}

func transactionEventsJSON(events *v2.TransactionEvents) string {
	if events == nil || len(events.GetEvents()) == 0 {
		return "[]"
	}

	rows := make([]map[string]any, 0, len(events.GetEvents()))
	for _, event := range events.GetEvents() {
		if event == nil || event.GetEventType() == "" {
			continue
		}
		entry := map[string]any{"type": event.GetEventType()}
		if event.Json != nil {
			entry["json"] = event.Json.AsInterface()
		}
		rows = append(rows, entry)
	}
	sortMapsByStableJSON(rows)
	return mustJSON(rows)
}

func transactionBalanceChangesJSON(balanceChanges []*v2.BalanceChange) string {
	if len(balanceChanges) == 0 {
		return "[]"
	}

	rows := make([]map[string]any, 0, len(balanceChanges))
	for _, change := range balanceChanges {
		if change == nil {
			continue
		}
		entry := map[string]any{
			"coin_type":     change.GetCoinType(),
			"amount":        change.GetAmount(),
			"owner_address": nil,
		}
		if change.Address != nil {
			entry["owner_address"] = change.GetAddress()
		}
		rows = append(rows, entry)
	}
	sortMapsByStableJSON(rows)
	return mustJSON(rows)
}

func transactionObjectsFromChanges(txDigest string, ts time.Time, changes []*v2.ChangedObject) []models.SuiTransactionObject {
	if len(changes) == 0 {
		return nil
	}

	result := make([]models.SuiTransactionObject, 0, len(changes))
	for _, change := range changes {
		if change == nil {
			continue
		}

		inputVersion := uint64(0)
		inputDigest := ""
		inputOwner := ""
		if change.GetInputState() == v2.ChangedObject_INPUT_OBJECT_STATE_EXISTS {
			inputVersion = change.GetInputVersion()
			inputDigest = change.GetInputDigest()
			inputOwner = addressBearingOwner(change.GetInputOwner())
		}

		outputVersion := uint64(0)
		outputDigest := ""
		outputOwner := ""
		if change.GetOutputState() == v2.ChangedObject_OUTPUT_OBJECT_STATE_OBJECT_WRITE ||
			change.GetOutputState() == v2.ChangedObject_OUTPUT_OBJECT_STATE_PACKAGE_WRITE {
			outputVersion = change.GetOutputVersion()
			outputDigest = change.GetOutputDigest()
			outputOwner = addressBearingOwner(change.GetOutputOwner())
		}

		version := outputVersion
		if version == 0 {
			version = inputVersion
		}
		if version == 0 {
			continue
		}
		if inputDigest != "" && outputDigest != "" && inputDigest == outputDigest {
			continue
		}

		result = append(result, models.SuiTransactionObject{
			ObjectID:          change.GetObjectId(),
			Version:           version,
			TransactionDigest: txDigest,
			InputVersion:      inputVersion,
			InputOwner:        nullableString(inputOwner),
			InputDigest:       nullableString(inputDigest),
			OutputVersion:     outputVersion,
			OutputOwner:       nullableString(outputOwner),
			OutputDigest:      nullableString(outputDigest),
			IsCreated:         boolToUint8(change.GetIdOperation() == v2.ChangedObject_CREATED),
			IsDeleted:         boolToUint8(change.GetIdOperation() == v2.ChangedObject_DELETED),
			Timestamp:         ts,
		})
	}

	return result
}

func addressBearingOwner(owner *v2.Owner) string {
	if owner == nil {
		return ""
	}
	switch owner.GetKind() {
	case v2.Owner_ADDRESS, v2.Owner_OBJECT, v2.Owner_CONSENSUS_ADDRESS:
		return owner.GetAddress()
	default:
		return ""
	}
}

func boolToUint8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

func sortMapsByStableJSON(rows []map[string]any) {
	sort.Slice(rows, func(i, j int) bool {
		return stableJSON(rows[i]) < stableJSON(rows[j])
	})
}

func stableJSON(value any) string {
	return mustJSON(value)
}

func mustJSON(value any) string {
	b, err := json.Marshal(value)
	if err != nil {
		return "[]"
	}
	return string(b)
}
