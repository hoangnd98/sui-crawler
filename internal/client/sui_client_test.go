package client

import (
	"fmt"
	"testing"
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
	digests := make([]string, MaxBatchGetTransactionsDigests*2+5)
	for i := range digests {
		digests[i] = fmt.Sprintf("tx-%02d", i)
	}

	chunks := batchGetTransactionDigestChunks(digests)

	wantSizes := []int{
		MaxBatchGetTransactionsDigests,
		MaxBatchGetTransactionsDigests,
		5,
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
