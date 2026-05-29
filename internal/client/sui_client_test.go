package client

import "testing"

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
