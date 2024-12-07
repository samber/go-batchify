package batchify

import (
	"time"

	"github.com/samber/go-batchify/pkg/hasher"
	"github.com/samber/lo"
)

func assertValue(ok bool, msg string) {
	if !ok {
		panic(msg)
	}
}

// BatchConfig is a builder for Batch.
func NewBatchConfig[I comparable, O any](bufferSize int, do func([]I) (map[I]O, error)) BatchConfig[I, O] {
	assertValue(bufferSize >= 1, "buffer size must be a positive value")
	return BatchConfig[I, O]{
		bufferSize: bufferSize,
		do:         do,
	}
}

type BatchConfig[I comparable, O any] struct {
	bufferSize int
	do         func([]I) (map[I]O, error)

	// max buffer duration
	ttl time.Duration

	shards     int
	shardingFn hasher.Hasher[I]
}

// WithTimer sets the max time for a batch buffer
func (cfg BatchConfig[I, O]) WithTimer(ttl time.Duration) BatchConfig[I, O] {
	assertValue(ttl >= 0, "ttl must be a positive value")

	cfg.ttl = ttl
	return cfg
}

// WithSharding enables cache sharding.
func (cfg BatchConfig[I, O]) WithSharding(nbr int, fn hasher.Hasher[I]) BatchConfig[I, O] {
	assertValue(nbr > 1, "shards must be greater than 1")
	assertValue(fn != nil, "hasher must be greater not nil")

	cfg.shards = nbr
	cfg.shardingFn = fn
	return cfg
}

// Build creates a new Batch instance.
func (cfg BatchConfig[I, O]) Build() Batch[I, O] {
	build := func(_ int) Batch[I, O] {
		return newBatch(
			cfg.bufferSize,
			cfg.ttl,
			cfg.do,
		)
	}

	if cfg.shards > 1 {
		batches := lo.RepeatBy(cfg.shards, build)
		return newShardedBatch(batches, cfg.shardingFn)
	}

	return build(0)
}

/**
 * Shortcuts
 */

// NewBatch creates a new Batch instance with fixed size and no timer.
func NewBatch[I comparable, O any](bufferSize int, do func([]I) (map[I]O, error)) Batch[I, O] {
	return NewBatchConfig(bufferSize, do).
		Build()
}

func NewBatchWithTimer[I comparable, O any](bufferSize int, do func([]I) (map[I]O, error), ttl time.Duration) Batch[I, O] {
	return NewBatchConfig(bufferSize, do).
		WithTimer(ttl).
		Build()
}

func NewShardedBatch[I comparable, O any](shards int, hasher hasher.Hasher[I], bufferSize int, do func([]I) (map[I]O, error)) Batch[I, O] {
	return NewBatchConfig(bufferSize, do).
		WithSharding(shards, hasher).
		Build()
}

func NewShardedBatchWithTimer[I comparable, O any](shards int, hasher hasher.Hasher[I], bufferSize int, do func([]I) (map[I]O, error), ttl time.Duration) Batch[I, O] {
	return NewBatchConfig(bufferSize, do).
		WithTimer(ttl).
		WithSharding(shards, hasher).
		Build()
}
