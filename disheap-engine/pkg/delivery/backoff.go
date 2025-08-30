package delivery

import (
	"math"
	"math/rand"
	"time"

	"go.uber.org/zap"
)

// BackoffStrategy defines the interface for calculating backoff delays
type BackoffStrategy interface {
	// Calculate returns the backoff duration for the given attempt number
	Calculate(attempts uint32) time.Duration
	// WithJitter adds jitter to the calculated backoff
	WithJitter(attempts uint32) time.Duration
	// Reset resets any internal state (useful for testing)
	Reset()
}

// ExponentialBackoff implements exponential backoff with configurable parameters
type ExponentialBackoff struct {
	baseDelay  time.Duration
	maxDelay   time.Duration
	multiplier float64
	jitter     JitterStrategy
	logger     *zap.Logger
}

// BackoffConfig holds configuration for exponential backoff
type BackoffConfig struct {
	BaseDelay  time.Duration  `json:"base_delay"`
	MaxDelay   time.Duration  `json:"max_delay"`
	Multiplier float64        `json:"multiplier"`
	Jitter     JitterStrategy `json:"jitter"`
}

// JitterStrategy defines different jitter strategies
type JitterStrategy int

const (
	NoJitter JitterStrategy = iota
	FullJitter
	EqualJitter
	DecorrelatedJitter
)

// DefaultBackoffConfig returns a sensible default configuration
func DefaultBackoffConfig() *BackoffConfig {
	return &BackoffConfig{
		BaseDelay:  1 * time.Second,
		MaxDelay:   5 * time.Minute,
		Multiplier: 2.0,
		Jitter:     FullJitter,
	}
}

// NewExponentialBackoff creates a new exponential backoff strategy
func NewExponentialBackoff(config *BackoffConfig, logger *zap.Logger) *ExponentialBackoff {
	if config == nil {
		config = DefaultBackoffConfig()
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	// Validate config
	if config.BaseDelay <= 0 {
		config.BaseDelay = 1 * time.Second
	}
	if config.MaxDelay <= 0 {
		config.MaxDelay = 5 * time.Minute
	}
	if config.Multiplier <= 1.0 {
		config.Multiplier = 2.0
	}

	return &ExponentialBackoff{
		baseDelay:  config.BaseDelay,
		maxDelay:   config.MaxDelay,
		multiplier: config.Multiplier,
		jitter:     config.Jitter,
		logger:     logger,
	}
}

// Calculate returns the exponential backoff duration without jitter
func (eb *ExponentialBackoff) Calculate(attempts uint32) time.Duration {
	if attempts == 0 {
		return 0
	}

	// Calculate exponential delay: baseDelay * multiplier^(attempts-1)
	delay := float64(eb.baseDelay) * math.Pow(eb.multiplier, float64(attempts-1))

	// Cap at max delay
	if delay > float64(eb.maxDelay) {
		delay = float64(eb.maxDelay)
	}

	return time.Duration(delay)
}

// WithJitter adds jitter to the calculated backoff duration
func (eb *ExponentialBackoff) WithJitter(attempts uint32) time.Duration {
	baseDelay := eb.Calculate(attempts)

	switch eb.jitter {
	case NoJitter:
		return baseDelay
	case FullJitter:
		return eb.fullJitter(baseDelay)
	case EqualJitter:
		return eb.equalJitter(baseDelay)
	case DecorrelatedJitter:
		return eb.decorrelatedJitter(baseDelay)
	default:
		return baseDelay
	}
}

// fullJitter implements full jitter: random value between 0 and calculated delay
func (eb *ExponentialBackoff) fullJitter(baseDelay time.Duration) time.Duration {
	if baseDelay <= 0 {
		return 0
	}

	jitteredDelay := time.Duration(rand.Int63n(int64(baseDelay)))

	eb.logger.Debug("Applied full jitter",
		zap.String("component", "backoff"),
		zap.Duration("base_delay", baseDelay),
		zap.Duration("jittered_delay", jitteredDelay))

	return jitteredDelay
}

// equalJitter implements equal jitter: half base delay + random half
func (eb *ExponentialBackoff) equalJitter(baseDelay time.Duration) time.Duration {
	if baseDelay <= 0 {
		return 0
	}

	halfDelay := baseDelay / 2
	jitteredHalf := time.Duration(rand.Int63n(int64(halfDelay)))
	jitteredDelay := halfDelay + jitteredHalf

	eb.logger.Debug("Applied equal jitter",
		zap.String("component", "backoff"),
		zap.Duration("base_delay", baseDelay),
		zap.Duration("jittered_delay", jitteredDelay))

	return jitteredDelay
}

// decorrelatedJitter implements decorrelated jitter
func (eb *ExponentialBackoff) decorrelatedJitter(baseDelay time.Duration) time.Duration {
	if baseDelay <= 0 {
		return 0
	}

	// Decorrelated jitter: random between baseDelay and 3*baseDelay
	minDelay := baseDelay
	maxDelay := baseDelay * 3

	if maxDelay > eb.maxDelay {
		maxDelay = eb.maxDelay
	}

	jitteredDelay := minDelay + time.Duration(rand.Int63n(int64(maxDelay-minDelay)))

	eb.logger.Debug("Applied decorrelated jitter",
		zap.String("component", "backoff"),
		zap.Duration("base_delay", baseDelay),
		zap.Duration("min_delay", minDelay),
		zap.Duration("max_delay", maxDelay),
		zap.Duration("jittered_delay", jitteredDelay))

	return jitteredDelay
}

// Reset resets any internal state (no-op for exponential backoff)
func (eb *ExponentialBackoff) Reset() {
	// No internal state to reset
}

// BackoffManager manages backoff strategies for message retries
type BackoffManager struct {
	strategy BackoffStrategy
	logger   *zap.Logger
}

// NewBackoffManager creates a new backoff manager
func NewBackoffManager(strategy BackoffStrategy, logger *zap.Logger) *BackoffManager {
	if strategy == nil {
		strategy = NewExponentialBackoff(DefaultBackoffConfig(), logger)
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	return &BackoffManager{
		strategy: strategy,
		logger:   logger,
	}
}

// CalculateBackoff calculates the backoff duration for a message retry
func (bm *BackoffManager) CalculateBackoff(attempts uint32, useJitter bool) time.Duration {
	var delay time.Duration

	if useJitter {
		delay = bm.strategy.WithJitter(attempts)
	} else {
		delay = bm.strategy.Calculate(attempts)
	}

	bm.logger.Debug("Calculated backoff",
		zap.String("component", "backoff_manager"),
		zap.Uint32("attempts", attempts),
		zap.Bool("use_jitter", useJitter),
		zap.Duration("delay", delay))

	return delay
}

// ScheduleRetry returns the time when a message should be retried
func (bm *BackoffManager) ScheduleRetry(attempts uint32, useJitter bool) time.Time {
	delay := bm.CalculateBackoff(attempts, useJitter)
	retryAt := time.Now().Add(delay)

	bm.logger.Debug("Scheduled retry",
		zap.String("component", "backoff_manager"),
		zap.Uint32("attempts", attempts),
		zap.Duration("delay", delay),
		zap.Time("retry_at", retryAt))

	return retryAt
}

// ShouldRetry determines if a message should be retried based on attempts and max retries
func (bm *BackoffManager) ShouldRetry(attempts uint32, maxRetries uint32) bool {
	should := attempts < maxRetries

	bm.logger.Debug("Retry decision",
		zap.String("component", "backoff_manager"),
		zap.Uint32("attempts", attempts),
		zap.Uint32("max_retries", maxRetries),
		zap.Bool("should_retry", should))

	return should
}

// LinearBackoff implements linear backoff strategy
type LinearBackoff struct {
	baseDelay time.Duration
	maxDelay  time.Duration
	increment time.Duration
	jitter    JitterStrategy
	logger    *zap.Logger
}

// NewLinearBackoff creates a linear backoff strategy
func NewLinearBackoff(baseDelay, maxDelay, increment time.Duration, jitter JitterStrategy, logger *zap.Logger) *LinearBackoff {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &LinearBackoff{
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		increment: increment,
		jitter:    jitter,
		logger:    logger,
	}
}

// Calculate returns the linear backoff duration
func (lb *LinearBackoff) Calculate(attempts uint32) time.Duration {
	if attempts == 0 {
		return 0
	}

	delay := lb.baseDelay + time.Duration(attempts-1)*lb.increment

	if delay > lb.maxDelay {
		delay = lb.maxDelay
	}

	return delay
}

// WithJitter adds jitter to the linear backoff
func (lb *LinearBackoff) WithJitter(attempts uint32) time.Duration {
	baseDelay := lb.Calculate(attempts)

	// Reuse the exponential backoff jitter logic
	eb := &ExponentialBackoff{jitter: lb.jitter, logger: lb.logger}

	switch lb.jitter {
	case NoJitter:
		return baseDelay
	case FullJitter:
		return eb.fullJitter(baseDelay)
	case EqualJitter:
		return eb.equalJitter(baseDelay)
	case DecorrelatedJitter:
		return eb.decorrelatedJitter(baseDelay)
	default:
		return baseDelay
	}
}

// Reset resets any internal state (no-op for linear backoff)
func (lb *LinearBackoff) Reset() {
	// No internal state to reset
}
