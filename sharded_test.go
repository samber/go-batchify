package batchify

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewShardedBatch(t *testing.T) {
	is := assert.New(t)

	batches := []Batch[string, string]{
		newBatch(42, 0, mockDoOk),
		newBatch(42, 0, mockDoOk),
	}
	b := newShardedBatch(batches, mockHasher)
	is.Len(b.batches, 2)
	is.NotNil(b.shardingFn)
}

func TestNewShardedBatch_Flush(t *testing.T) {
	is := assert.New(t)

	batches := []Batch[string, string]{
		newBatch(42, 5*time.Millisecond, mockDoOk),
		newBatch(42, 5*time.Millisecond, mockDoOk),
	}
	b := newShardedBatch(batches, mockHasher)
	is.Len(b.batches, 2)
	is.NotNil(b.shardingFn)

	batches[0].(*batchImpl[string, string]).mu.Lock()
	batches[0].(*batchImpl[string, string]).buffer.values["key"] = ""
	batches[0].(*batchImpl[string, string]).buffer.size++
	batches[0].(*batchImpl[string, string]).mu.Unlock()
	is.Len(batches[0].(*batchImpl[string, string]).buffer.values, 1)
	is.Equal(1, batches[0].(*batchImpl[string, string]).buffer.size)

	batches[1].(*batchImpl[string, string]).mu.Lock()
	batches[1].(*batchImpl[string, string]).buffer.values["key"] = ""
	batches[1].(*batchImpl[string, string]).buffer.size++
	batches[1].(*batchImpl[string, string]).mu.Unlock()
	is.Len(batches[1].(*batchImpl[string, string]).buffer.values, 1)
	is.Equal(1, batches[1].(*batchImpl[string, string]).buffer.size)

	b.Flush()
	is.Len(batches[0].(*batchImpl[string, string]).buffer.values, 0)
	is.Equal(0, batches[0].(*batchImpl[string, string]).buffer.size)
	is.Len(batches[1].(*batchImpl[string, string]).buffer.values, 0)
	is.Equal(0, batches[1].(*batchImpl[string, string]).buffer.size)
}

func TestNewShardedBatch_Stop(t *testing.T) {
	is := assert.New(t)

	batches := []Batch[string, string]{
		newBatch(42, 5*time.Millisecond, mockDoOk),
		newBatch(42, 5*time.Millisecond, mockDoOk),
	}
	b := newShardedBatch(batches, mockHasher)
	is.Len(b.batches, 2)
	is.NotNil(b.shardingFn)

	batches[0].(*batchImpl[string, string]).mu.Lock()
	batches[0].(*batchImpl[string, string]).buffer.values["key"] = ""
	batches[0].(*batchImpl[string, string]).buffer.size++
	batches[0].(*batchImpl[string, string]).mu.Unlock()
	is.Len(batches[0].(*batchImpl[string, string]).buffer.values, 1)
	is.Equal(1, batches[0].(*batchImpl[string, string]).buffer.size)

	batches[1].(*batchImpl[string, string]).mu.Lock()
	batches[1].(*batchImpl[string, string]).buffer.values["key"] = ""
	batches[1].(*batchImpl[string, string]).buffer.size++
	batches[1].(*batchImpl[string, string]).mu.Unlock()
	is.Len(batches[1].(*batchImpl[string, string]).buffer.values, 1)
	is.Equal(1, batches[1].(*batchImpl[string, string]).buffer.size)

	b.Stop()
	is.Len(batches[0].(*batchImpl[string, string]).buffer.values, 0)
	is.Equal(0, batches[0].(*batchImpl[string, string]).buffer.size)
	is.Len(batches[1].(*batchImpl[string, string]).buffer.values, 0)
	is.Equal(0, batches[1].(*batchImpl[string, string]).buffer.size)
}
