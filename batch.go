package batchify

import (
	"sync"
	"time"

	"github.com/samber/lo"
)

func newBatch[I comparable, O any](
	bufferSize int,
	ttl time.Duration,
	do func([]I) (map[I]O, error),
) *batchImpl[I, O] {
	b := &batchImpl[I, O]{
		timer: nil,
		mu:    sync.RWMutex{},

		// read-only
		bufferSize: bufferSize,
		ttl:        ttl,
		do:         do,

		buffer: newBuffer[I, O](bufferSize),
	}

	b.resetTimer()
	return b
}

var _ Batch[string, int] = (*batchImpl[string, int])(nil)

type batchImpl[I comparable, O any] struct {
	timer *time.Timer
	mu    sync.RWMutex

	bufferSize int
	ttl        time.Duration
	do         func([]I) (map[I]O, error)

	buffer *buffer[I, O]
}

func (b *batchImpl[I, O]) Do(input I) (output O, err error) {
	b.mu.Lock()

	currentBuffer := b.buffer
	if _, ok := currentBuffer.values[input]; !ok {
		currentBuffer.values[input] = lo.Empty[O]()
		currentBuffer.size++
	}

	bufferIsFull := currentBuffer.size == b.bufferSize

	if bufferIsFull {
		b.buffer = newBuffer[I, O](b.bufferSize)
		b.resetTimer()
	}

	b.mu.Unlock()

	if bufferIsFull {
		b.execCallback(currentBuffer)
	}

	// do not call wg.Wait() if `input` is the last element of the buffer
	currentBuffer.wg.Wait()

	// outputs[input] might be empty
	return currentBuffer.values[input], currentBuffer.err
}

func (b *batchImpl[I, O]) Stop() {
	if b.timer != nil {
		b.timer.Stop()
	}

	b.mu.Lock()
	b.timer = nil
	currentBuffer := b.buffer
	b.buffer = newBuffer[I, O](b.bufferSize)
	b.mu.Unlock()

	b.execCallback(currentBuffer)
	currentBuffer.wg.Wait()
}

func (b *batchImpl[I, O]) Flush() {
	b.mu.Lock()

	currentBuffer := b.buffer
	if currentBuffer.size == 0 {
		b.resetTimer()
		b.mu.Unlock()
		return
	}

	b.buffer = newBuffer[I, O](b.bufferSize)
	b.resetTimer()

	b.mu.Unlock()

	b.execCallback(currentBuffer)
}

// execCallback must be called out of mutex lock to prevent slowdown due to long-running callback.
func (b *batchImpl[I, O]) execCallback(buffer *buffer[I, O]) {
	go buffer.once.Do(func() {
		if buffer.size > 0 {
			buffer.values, buffer.err = b.do(lo.Keys(buffer.values))
		}

		buffer.wg.Done()
	})
}

func (b *batchImpl[I, O]) resetTimer() {
	if b.ttl == 0 {
		return
	}

	if b.timer != nil {
		b.timer.Reset(b.ttl)
	} else {
		b.timer = time.AfterFunc(b.ttl, func() {
			b.Flush()
		})
	}
}
