package limiter

import "errors"

// Sentinel errors returned by the package.
var (
	// ErrInvalidLimit is returned when the bucket limit is zero, negative, NaN or infinite.
	ErrInvalidLimit = errors.New("limit must be a positive finite number")

	// ErrInvalidRate is returned when the refill rate is zero, negative, NaN or infinite.
	ErrInvalidRate = errors.New("rate must be a positive finite number")

	// ErrInvalidRequest is returned when the request has an empty key or a cost
	// that is not a positive finite number.
	ErrInvalidRequest = errors.New("request key must be non-empty and cost must be a positive finite number")

	// ErrCostExceedsLimit is returned when the request cost is greater than the bucket limit
	// and therefore can never be satisfied.
	ErrCostExceedsLimit = errors.New("cost exceeds bucket limit")

	// ErrRefillTooSlow is returned by NewTokenBucket when a full refill
	// (limit/rate seconds) would take longer than time.Duration can represent
	// (about 292 years), so RetryAfter could not be computed.
	ErrRefillTooSlow = errors.New("limit/rate too large: full refill must take less than ~292 years")

	// ErrInvalidSweepInterval is returned by NewTokenBucket when the interval
	// set with WithSweepInterval is zero or negative.
	ErrInvalidSweepInterval = errors.New("sweep interval must be positive")
)
