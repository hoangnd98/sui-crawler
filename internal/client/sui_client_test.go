package client

import (
	"errors"
	"fmt"
	"testing"
	"time"

	v2 "github.com/open-move/sui-go-sdk/proto/sui/rpc/v2"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestNormalizeGRPCEndpointStripsSecureDefaultPort(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     string
	}{
		{
			name:     "archive https default port",
			endpoint: "https://archive.mainnet.sui.io:443",
			want:     "https://archive.mainnet.sui.io",
		},
		{
			name:     "fullnode https default port",
			endpoint: "https://fullnode.mainnet.sui.io:443",
			want:     "https://fullnode.mainnet.sui.io",
		},
		{
			name:     "grpcs default port",
			endpoint: "grpcs://archive.mainnet.sui.io:443",
			want:     "grpcs://archive.mainnet.sui.io",
		},
		{
			name:     "non default secure port",
			endpoint: "https://archive.mainnet.sui.io:8443",
			want:     "https://archive.mainnet.sui.io:8443",
		},
		{
			name:     "bare host port",
			endpoint: "archive.mainnet.sui.io:443",
			want:     "archive.mainnet.sui.io:443",
		},
		{
			name:     "trims whitespace",
			endpoint: "  https://archive.mainnet.sui.io:443  ",
			want:     "https://archive.mainnet.sui.io",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeGRPCEndpoint(tt.endpoint); got != tt.want {
				t.Fatalf("normalizeGRPCEndpoint(%q) = %q, want %q", tt.endpoint, got, tt.want)
			}
		})
	}
}

func TestBatchGetTransactionDigestChunksSplitsArchiveSafeBatches(t *testing.T) {
	digests := make([]string, MaxBatchGetTransactionsDigests*2+3)
	for i := range digests {
		digests[i] = fmt.Sprintf("tx-%02d", i)
	}

	chunks := batchGetTransactionDigestChunks(digests)

	wantSizes := []int{
		MaxBatchGetTransactionsDigests,
		MaxBatchGetTransactionsDigests,
		3,
	}
	if got := len(chunks); got != len(wantSizes) {
		t.Fatalf("chunk count = %d, want %d", got, len(wantSizes))
	}
	for i, wantSize := range wantSizes {
		if got := chunks[i].start; got != i*MaxBatchGetTransactionsDigests {
			t.Fatalf("chunk %d start = %d, want %d", i, got, i*MaxBatchGetTransactionsDigests)
		}
		if got := len(chunks[i].digests); got != wantSize {
			t.Fatalf("chunk %d size = %d, want %d", i, got, wantSize)
		}
		if got := chunks[i].digests[0]; got != digests[chunks[i].start] {
			t.Fatalf("chunk %d first digest = %q, want %q", i, got, digests[chunks[i].start])
		}
	}
}

func TestBatchGetTransactionDigestChunksEmptyInput(t *testing.T) {
	if got := batchGetTransactionDigestChunks(nil); got != nil {
		t.Fatalf("nil input chunks = %#v, want nil", got)
	}
}

func TestArchiveReadRowsRequestTooLargeErrorIsDetected(t *testing.T) {
	err := errors.New(`rpc error: code = Internal desc = code: 'Client specified an invalid argument', message: "Received ReadRowsRequest message too large (1599460, maximum allowed: 524288); Filter size 2; RowSet size 1599360"`)

	if !isArchiveReadRowsRequestTooLargeError(err) {
		t.Fatal("isArchiveReadRowsRequestTooLargeError = false, want true")
	}
}

func TestGRPCMessageTooLargeErrorIsDetected(t *testing.T) {
	err := errors.New(`rpc error: code = ResourceExhausted desc = grpc: received message larger than max (4570720 vs. 4194304)`)

	if !isGRPCMessageTooLargeError(err) {
		t.Fatal("isGRPCMessageTooLargeError = false, want true")
	}
	if !isAdaptiveBatchSplitError(err) {
		t.Fatal("isAdaptiveBatchSplitError = false, want true")
	}
}

func TestWithRetryContextDoesNotRetryAdaptiveBatchSplitErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "archive read rows request too large",
			err:  errors.New(`Received ReadRowsRequest message too large (1599460, maximum allowed: 524288)`),
		},
		{
			name: "grpc response message too large",
			err:  errors.New(`grpc: received message larger than max (4570720 vs. 4194304)`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attempts := 0

			err := withRetryContext(t.Context(), "test", func() error {
				attempts++
				return tt.err
			})

			if !errors.Is(err, tt.err) {
				t.Fatalf("withRetryContext error = %v, want %v", err, tt.err)
			}
			if attempts != 1 {
				t.Fatalf("attempts = %d, want 1", attempts)
			}
		})
	}
}

func TestGRPCMessageSizeLimitIsAboveDefault(t *testing.T) {
	const defaultGRPCMessageSize = 4 * 1024 * 1024

	if maxGRPCMessageSize <= defaultGRPCMessageSize {
		t.Fatalf("maxGRPCMessageSize = %d, want > %d", maxGRPCMessageSize, defaultGRPCMessageSize)
	}
}

func TestMapTransactionNormalizesGrpcFieldsLikeGraphQLMapper(t *testing.T) {
	tx := parityFixtureTransaction("tx-digest", "0xsender")

	row, objects, err := mapTransaction(tx, 100, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("mapTransaction returned error: %v", err)
	}

	if row.Sender == nil || *row.Sender != "0xsender" {
		t.Fatalf("sender = %v, want 0xsender", row.Sender)
	}
	if row.KindTypename != "ProgrammableTransaction" {
		t.Fatalf("kind = %q, want ProgrammableTransaction", row.KindTypename)
	}
	if row.CommandsJSON != `[{"__typename":"MoveCallCommand","function_name":"swap","package_address":"0xpkg"},{"__typename":"MakeMoveVecCommand"},{"__typename":"TransferObjectsCommand","transfer_address":{"__typename":"Input","ix":2},"transfer_inputs":[{"__typename":"GasCoin"},{"__typename":"TxResult","cmd":1}]}]` {
		t.Fatalf("commands_json = %s", row.CommandsJSON)
	}
	if row.InputsJSON != `[{"__typename":"MoveValue"},{"__typename":"SharedInput","address":"0xshared","mutable":true},{"__typename":"OwnedOrImmutable","object_address":"0xowned"}]` {
		t.Fatalf("inputs_json = %s", row.InputsJSON)
	}
	if row.EventsJSON != `[{"json":{"a":1},"type":"eventA"},{"json":{"b":2},"type":"eventB"}]` {
		t.Fatalf("events_json = %s", row.EventsJSON)
	}
	if row.BalanceChangesJSON != `[{"amount":"1","coin_type":"coinA","owner_address":"0x1"},{"amount":"2","coin_type":"coinB","owner_address":"0x2"}]` {
		t.Fatalf("balance_changes_json = %s", row.BalanceChangesJSON)
	}
	if row.GasFee != 2500 {
		t.Fatalf("gas_fee = %d, want 2500", row.GasFee)
	}
	if len(objects) != 1 {
		t.Fatalf("transaction objects = %d, want 1", len(objects))
	}
	if objects[0].InputOwner != nil {
		t.Fatalf("input owner = %v, want nil for shared owner", *objects[0].InputOwner)
	}
	if objects[0].OutputOwner == nil || *objects[0].OutputOwner != "0xowner" {
		t.Fatalf("output owner = %v, want 0xowner", objects[0].OutputOwner)
	}
}

func TestMapTransactionNormalizesZeroSenderProgrammableAsSystem(t *testing.T) {
	tx := parityFixtureTransaction("system-tx", zeroSuiAddress)

	row, _, err := mapTransaction(tx, 100, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("mapTransaction returned error: %v", err)
	}

	if row.Sender != nil {
		t.Fatalf("sender = %v, want nil", *row.Sender)
	}
	if row.KindTypename != "ProgrammableSystemTransaction" {
		t.Fatalf("kind = %q, want ProgrammableSystemTransaction", row.KindTypename)
	}
	if row.CommandsJSON != "[]" {
		t.Fatalf("commands_json = %s, want []", row.CommandsJSON)
	}
	if row.InputsJSON != "[]" {
		t.Fatalf("inputs_json = %s, want []", row.InputsJSON)
	}
}

func parityFixtureTransaction(digest, sender string) *v2.ExecutedTransaction {
	argGasKind := v2.Argument_GAS
	argInputKind := v2.Argument_INPUT
	argResultKind := v2.Argument_RESULT
	inputPureKind := v2.Input_PURE
	inputSharedKind := v2.Input_SHARED
	inputOwnedKind := v2.Input_IMMUTABLE_OR_OWNED
	inputExists := v2.ChangedObject_INPUT_OBJECT_STATE_EXISTS
	outputWrite := v2.ChangedObject_OUTPUT_OBJECT_STATE_OBJECT_WRITE
	idNone := v2.ChangedObject_NONE
	ownerShared := v2.Owner_SHARED
	ownerAddress := v2.Owner_ADDRESS
	success := true

	eventA, err := structpb.NewStruct(map[string]any{"a": 1})
	if err != nil {
		panic(err)
	}
	eventB, err := structpb.NewStruct(map[string]any{"b": 2})
	if err != nil {
		panic(err)
	}

	return &v2.ExecutedTransaction{
		Digest: ptr(digest),
		Transaction: &v2.Transaction{
			Sender: ptr(sender),
			Kind: &v2.TransactionKind{
				Data: &v2.TransactionKind_ProgrammableTransaction{
					ProgrammableTransaction: &v2.ProgrammableTransaction{
						Commands: []*v2.Command{
							{
								Command: &v2.Command_MoveCall{
									MoveCall: &v2.MoveCall{
										Package:  ptr("0xpkg"),
										Module:   ptr("amm"),
										Function: ptr("swap"),
									},
								},
							},
							{Command: &v2.Command_MakeMoveVector{MakeMoveVector: &v2.MakeMoveVector{}}},
							{
								Command: &v2.Command_TransferObjects{
									TransferObjects: &v2.TransferObjects{
										Objects: []*v2.Argument{
											{Kind: &argGasKind},
											{Kind: &argResultKind, Result: ptr(uint32(1))},
										},
										Address: &v2.Argument{Kind: &argInputKind, Input: ptr(uint32(2))},
									},
								},
							},
						},
						Inputs: []*v2.Input{
							{Kind: &inputPureKind, Pure: []byte{1, 2, 3}},
							{Kind: &inputSharedKind, ObjectId: ptr("0xshared"), Mutable: ptr(true)},
							{Kind: &inputOwnedKind, ObjectId: ptr("0xowned")},
						},
					},
				},
			},
		},
		Effects: &v2.TransactionEffects{
			Status: &v2.ExecutionStatus{Success: &success},
			GasUsed: &v2.GasCostSummary{
				ComputationCost: ptr(uint64(1000)),
				StorageCost:     ptr(uint64(2000)),
				StorageRebate:   ptr(uint64(500)),
			},
			ChangedObjects: []*v2.ChangedObject{
				{
					ObjectId:      ptr("0xobject"),
					InputState:    &inputExists,
					InputVersion:  ptr(uint64(1)),
					InputDigest:   ptr("inDigest"),
					InputOwner:    &v2.Owner{Kind: &ownerShared, Version: ptr(uint64(1))},
					OutputState:   &outputWrite,
					OutputVersion: ptr(uint64(2)),
					OutputDigest:  ptr("outDigest"),
					OutputOwner:   &v2.Owner{Kind: &ownerAddress, Address: ptr("0xowner")},
					IdOperation:   &idNone,
				},
			},
		},
		Events: &v2.TransactionEvents{
			Events: []*v2.Event{
				{EventType: ptr("eventB"), Json: structpb.NewStructValue(eventB)},
				{EventType: ptr("eventA"), Json: structpb.NewStructValue(eventA)},
			},
		},
		BalanceChanges: []*v2.BalanceChange{
			{Address: ptr("0x2"), CoinType: ptr("coinB"), Amount: ptr("2")},
			{Address: ptr("0x1"), CoinType: ptr("coinA"), Amount: ptr("1")},
		},
	}
}

func ptr[T any](v T) *T {
	return &v
}
