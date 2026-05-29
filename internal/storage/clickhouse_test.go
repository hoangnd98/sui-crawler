package storage

import (
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestNewClickHouseOptionsSupportsHTTPSURL(t *testing.T) {
	opts, err := newClickHouseOptions(
		"https://ivokpgz3y9.ap-southeast-1.aws.clickhouse.cloud:8443",
		"default",
		"user",
		"password",
	)
	if err != nil {
		t.Fatalf("newClickHouseOptions returned error: %v", err)
	}

	if opts.Protocol != clickhouse.HTTP {
		t.Fatalf("Protocol = %v, want HTTP", opts.Protocol)
	}
	if got := opts.Addr; len(got) != 1 || got[0] != "ivokpgz3y9.ap-southeast-1.aws.clickhouse.cloud:8443" {
		t.Fatalf("Addr = %#v, want ClickHouse Cloud host:port", got)
	}
	if opts.TLS == nil {
		t.Fatal("TLS is nil, want TLS config for https URL")
	}
	if opts.TLS.ServerName != "ivokpgz3y9.ap-southeast-1.aws.clickhouse.cloud" {
		t.Fatalf("TLS.ServerName = %q, want ClickHouse Cloud hostname", opts.TLS.ServerName)
	}
	if opts.Auth.Database != "default" || opts.Auth.Username != "user" || opts.Auth.Password != "password" {
		t.Fatalf("Auth = %#v, want supplied credentials", opts.Auth)
	}
}

func TestNewClickHouseOptionsKeepsNativeAddress(t *testing.T) {
	opts, err := newClickHouseOptions("localhost:9000", "default", "default", "")
	if err != nil {
		t.Fatalf("newClickHouseOptions returned error: %v", err)
	}

	if opts.Protocol != clickhouse.Native {
		t.Fatalf("Protocol = %v, want Native", opts.Protocol)
	}
	if got := opts.Addr; len(got) != 1 || got[0] != "localhost:9000" {
		t.Fatalf("Addr = %#v, want localhost:9000", got)
	}
	if opts.TLS != nil {
		t.Fatalf("TLS = %#v, want nil for native address", opts.TLS)
	}
}

func TestInsertQueriesDoNotUseSemicolonTerminators(t *testing.T) {
	queries := map[string]string{
		"checkpoints":         insertCheckpointsQuery,
		"transactions":        insertTransactionsQuery,
		"transaction_objects": insertTransactionObjectsQuery,
	}

	for name, query := range queries {
		if strings.Contains(query, ";") {
			t.Fatalf("%s query contains semicolon: %q", name, query)
		}
		if !strings.HasPrefix(query, "INSERT INTO ") {
			t.Fatalf("%s query = %q, want INSERT statement", name, query)
		}
	}
}
