package client

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestIsUnparseableTypeError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "archive item error",
			err:  fmt.Errorf(`batch transaction 9SB failed: code=13 message=unable to parse type "LPCoin<0x5c45"`),
			want: true,
		},
		{
			name: "wrapped",
			err:  fmt.Errorf("hydrate: %w", errors.New("unable to parse type \"X\"")),
			want: true,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "unrelated transient",
			err:  errors.New("rpc timeout after 2m0s: context deadline exceeded"),
			want: false,
		},
		{
			name: "adaptive split error is not unparseable",
			err:  errors.New("grpc: received message larger than max"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUnparseableTypeError(tt.err); got != tt.want {
				t.Fatalf("isUnparseableTypeError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestBatchTransactionErrorIsUnparseable(t *testing.T) {
	err := newBatchTransactionError("digestX", 13, `unable to parse type "LPCoin<0x5c45"`)
	if !isUnparseableTypeError(err) {
		t.Fatal("typed batch error with parse message should be unparseable")
	}
	if got, want := err.Error(), `batch transaction digestX failed: code=13 message=unable to parse type "LPCoin<0x5c45"`; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}

	notParse := newBatchTransactionError("digestY", 5, "object not found")
	if isUnparseableTypeError(notParse) {
		t.Fatal("typed batch error without parse message should not be unparseable")
	}
}

func TestUnparseableDigestIndex(t *testing.T) {
	chunk := []string{"a", "b", "poison", "d"}
	err := newBatchTransactionError("poison", 13, `unable to parse type "X"`)
	if got := unparseableDigestIndex(chunk, err); got != 2 {
		t.Fatalf("unparseableDigestIndex = %d, want 2", got)
	}

	// Digest not in chunk -> -1.
	if got := unparseableDigestIndex(chunk, newBatchTransactionError("missing", 13, "unable to parse type")); got != -1 {
		t.Fatalf("unparseableDigestIndex(missing) = %d, want -1", got)
	}

	// Untyped error -> -1 (caller falls back to halving).
	if got := unparseableDigestIndex(chunk, errors.New("unable to parse type")); got != -1 {
		t.Fatalf("unparseableDigestIndex(untyped) = %d, want -1", got)
	}
}

// TestWithRetryContextSkipsUnparseableTypeError verifies the deterministic
// malformed-type error is returned on the first attempt without burning retries.
func TestWithRetryContextSkipsUnparseableTypeError(t *testing.T) {
	calls := 0
	start := time.Now()
	err := withRetryContext(context.Background(), "test", func() error {
		calls++
		return errors.New(`code=13 message=unable to parse type "LPCoin<0x5c45"`)
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls != 1 {
		t.Fatalf("expected 1 attempt (no retries), got %d", calls)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("expected immediate return, took %s", elapsed)
	}
}
