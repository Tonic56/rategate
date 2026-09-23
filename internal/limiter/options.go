package limiter

import "time"

// config collects the optional settings of a limiter. It is filled by the
// options before the limiter itself is built, so that nothing has to be
// changed after construction.
type config struct {
	clock         clock
	sweepInterval time.Duration
}

// Option changes one optional setting of a limiter. Options are applied by
// NewTokenBucket in the order they are given, on top of the defaults.
type Option func(*config)

// withClock replaces the source of time. It stays unexported because only the
// tests of this package need a clock they can move by hand.
func withClock(c clock) Option {
	return func(cfg *config) {
		cfg.clock = c
	}
}

// WithSweepInterval sets how often Run removes idle buckets. A shorter
// interval frees memory sooner, a longer one locks the shards less often.
// The default is one minute; d must be positive.
func WithSweepInterval(d time.Duration) Option {
	return func(cfg *config) {
		cfg.sweepInterval = d
	}
}
