package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all configuration for the SUI crawler.
type Config struct {
	// MongoDB
	MongoURI string
	MongoDB  string

	// API server
	APIPort string

	// SUI RPC
	SuiRPCURL       string
	SuiRateLimitRPS float64
	SuiRPCTimeout   time.Duration

	// SUI JSON-RPC endpoint (used for event queries, separate from gRPC)
	SuiJSONRPCURL string

	// ClickHouse
	ClickHouseAddr     string
	ClickHouseDatabase string
	ClickHouseUsername string
	ClickHousePassword string
}

// Load reads configuration from environment variables with sensible defaults.
func Load() (*Config, error) {
	cfg := &Config{
		MongoURI:           "mongodb://localhost:27019",
		MongoDB:            "sui_crawler",
		APIPort:            "8080",
		SuiRPCURL:          "https://archive.mainnet.sui.io",
		SuiRateLimitRPS:    10.0,
		SuiRPCTimeout:      2 * time.Minute,
		SuiJSONRPCURL:      "",
		ClickHouseAddr:     "localhost:9000",
		ClickHouseDatabase: "default",
		ClickHouseUsername: "default",
		ClickHousePassword: "",
	}

	if v := os.Getenv("MONGO_URI"); v != "" {
		cfg.MongoURI = v
	}

	if v := os.Getenv("MONGO_DB"); v != "" {
		cfg.MongoDB = v
	}

	if v := os.Getenv("API_PORT"); v != "" {
		cfg.APIPort = v
	}

	if v := os.Getenv("SUI_RPC_URL"); v != "" {
		cfg.SuiRPCURL = v
	}

	if v := os.Getenv("SUI_JSON_RPC_URL"); v != "" {
		cfg.SuiJSONRPCURL = v
	}

	if v := os.Getenv("SUI_RATE_LIMIT_RPS"); v != "" {
		rps, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid SUI_RATE_LIMIT_RPS: %w", err)
		}
		cfg.SuiRateLimitRPS = rps
	}

	if v := os.Getenv("SUI_RPC_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid SUI_RPC_TIMEOUT: %w", err)
		}
		cfg.SuiRPCTimeout = d
	}

	if v := os.Getenv("CLICK_HOUSE_ADDR"); v != "" {
		cfg.ClickHouseAddr = v
	}

	if v := os.Getenv("CLICK_HOUSE_DB"); v != "" {
		cfg.ClickHouseDatabase = v
	}

	if v := os.Getenv("CLICK_HOUSE_USR"); v != "" {
		cfg.ClickHouseUsername = v
	}

	if v := os.Getenv("CLICK_HOUSE_PWD"); v != "" {
		cfg.ClickHousePassword = v
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.MongoURI == "" {
		return fmt.Errorf("MONGO_URI must not be empty")
	}
	if c.MongoDB == "" {
		return fmt.Errorf("MONGO_DB must not be empty")
	}
	if c.APIPort == "" {
		return fmt.Errorf("API_PORT must not be empty")
	}
	if c.SuiRPCURL == "" {
		return fmt.Errorf("SUI_RPC_URL must not be empty")
	}
	if c.SuiRPCTimeout <= 0 {
		return fmt.Errorf("SUI_RPC_TIMEOUT must be > 0")
	}
	return nil
}
