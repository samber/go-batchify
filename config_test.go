package batchify

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAssertValue(t *testing.T) {
	is := assert.New(t)

	is.NotPanics(func() {
		assertValue(true, "error")
	})
	is.PanicsWithValue("error", func() {
		assertValue(false, "error")
	})
}

func TestNewBatchConfig(t *testing.T) {
	is := assert.New(t)

	opts := NewBatchConfig(42, mockDoOk)
	is.Equal(42, opts.bufferSize)
	is.NotNil(opts.do)
	is.EqualValues(0, opts.ttl)
	is.Equal(0, opts.shards)
	is.Nil(opts.shardingFn)

	is.Panics(func() {
		opts = opts.WithTimer(-42 * time.Second)
	})
	opts = opts.WithTimer(21 * time.Second)
	is.Equal(42, opts.bufferSize)
	is.NotNil(opts.do)
	is.EqualValues(21*time.Second, opts.ttl)
	is.Equal(0, opts.shards)
	is.Nil(opts.shardingFn)

	is.Panics(func() {
		opts = opts.WithSharding(1, func(key string) uint64 { return 0 })
	})
	is.Panics(func() {
		opts = opts.WithSharding(2, nil)
	})

	opts = opts.WithSharding(2, func(key string) uint64 { return 0 })
	is.Equal(42, opts.bufferSize)
	is.NotNil(opts.do)
	is.EqualValues(21*time.Second, opts.ttl)
	is.Equal(2, opts.shards)
	is.NotNil(opts.shardingFn)

	is.NotPanics(func() {
		opts.Build()
	})

	is.Panics(func() {
		_ = NewBatchConfig(-42, mockDoOk)
	})
}

func TestHelperNewBatch(t *testing.T) {
	is := assert.New(t)

	batch := NewBatch(42, mockDoOk)
	b, ok := batch.(*batchImpl[string, string])
	is.True(ok)
	is.Nil(b.timer)
	// is.NotNil(b.mu)
	is.Equal(42, b.bufferSize)
	is.EqualValues(0, b.ttl)
	is.NotNil(b.do)
	is.NotNil(b.buffer)
}

func TestHelperNewBatchWithTimer(t *testing.T) {
	is := assert.New(t)

	batch := NewBatchWithTimer(42, mockDoOk, 21*time.Second)
	b, ok := batch.(*batchImpl[string, string])
	is.True(ok)
	is.NotNil(b.timer)
	// is.NotNil(b.mu)
	is.Equal(42, b.bufferSize)
	is.Equal(21*time.Second, b.ttl)
	is.NotNil(b.do)
	is.NotNil(b.buffer)
}

func TestHelperNewShardedBatch(t *testing.T) {
	is := assert.New(t)

	batch := NewShardedBatch(2, mockHasher, 42, mockDoOk)
	b, ok := batch.(*shardedBatchImpl[string, string])
	is.True(ok)
	is.Len(b.batches, 2)
	is.NotNil(b.shardingFn)
	for i := range b.batches {
		bb := b.batches[i].(*batchImpl[string, string])
		is.Nil(bb.timer)
		// is.NotNil(bb.mu)
		is.Equal(42, bb.bufferSize)
		is.EqualValues(0, bb.ttl)
		is.NotNil(bb.do)
		is.NotNil(bb.buffer)
	}
}

func TestHelperNewShardedBatchWithTimer(t *testing.T) {
	is := assert.New(t)

	batch := NewShardedBatchWithTimer(2, mockHasher, 42, mockDoOk, 21*time.Second)
	b, ok := batch.(*shardedBatchImpl[string, string])
	is.True(ok)
	is.Len(b.batches, 2)
	is.NotNil(b.shardingFn)
	for i := range b.batches {
		bb := b.batches[i].(*batchImpl[string, string])
		is.NotNil(bb.timer)
		// is.NotNil(bb.mu)
		is.Equal(42, bb.bufferSize)
		is.Equal(21*time.Second, bb.ttl)
		is.NotNil(bb.do)
		is.NotNil(bb.buffer)
	}
}
