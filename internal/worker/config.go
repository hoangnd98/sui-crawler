package worker

import (
	"time"

	"golang.org/x/time/rate"
)

// WorkerConfig holds configuration for the crawler worker.
type WorkerConfig struct {
	SuiRPCURL      string
	SuiRateRPS     float64
	SuiGraphQLURL  string
	SuiGraphQLRPS  float64
	CHAddr         string
	CHDatabase     string
	CHUsername     string
	CHPassword     string
	RateLimiter    *rate.Limiter
	GraphQLLimiter *rate.Limiter
	RPCTimeout     time.Duration
}
