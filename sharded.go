package batchify

import (
	"sync"

	"github.com/samber/go-batchify/internal"
	"github.com/samber/go-batchify/pkg/hasher"
)

func newShardedBatch[I comparable, O any](
	batches []Batch[I, O],
	shardingFn hasher.Hasher[I],
) *shardedBatchImpl[I, O] {
	return &shardedBatchImpl[I, O]{
		shards:     uint64(len(batches)),
		batches:    batches,
		shardingFn: shardingFn,
	}
}

var _ Batch[string, int] = (*shardedBatchImpl[string, int])(nil)

type shardedBatchImpl[I comparable, O any] struct {
	_ internal.NoCopy

	shards     uint64
	batches    []Batch[I, O]
	shardingFn hasher.Hasher[I]
}

func (b *shardedBatchImpl[I, O]) Do(input I) (output O, err error) {
	shardIdx := b.shardingFn.ComputeHash(input, b.shards)
	return b.batches[shardIdx].Do(input)
}

func (b *shardedBatchImpl[I, O]) Flush() {
	var wg sync.WaitGroup
	wg.Add(len(b.batches))

	for _, batch := range b.batches {
		go func(b Batch[I, O]) {
			defer wg.Done()
			b.Flush()
		}(batch)
	}

	wg.Wait()
}

func (b *shardedBatchImpl[I, O]) Stop() {
	var wg sync.WaitGroup
	wg.Add(len(b.batches))

	for _, batch := range b.batches {
		go func(b Batch[I, O]) {
			defer wg.Done()
			b.Stop()
		}(batch)
	}

	wg.Wait()
}
