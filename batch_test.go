package batchify

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewBatch(t *testing.T) {
	is := assert.New(t)

	b := newBatch(42, 0, mockDoOk)
	// is.NotNil(b.ticker)
	// is.NotNil(b.mu)
	is.Equal(42, b.bufferSize)
	is.EqualValues(0, b.ttl)
	is.NotNil(b.do)
	is.NotNil(b.buffer)
}

func TestBatchImpl_Stop_noTimer(t *testing.T) {
	is := assert.New(t)

	b := newBatch(42, 0, mockDoOk)
	is.Nil(b.timer)
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)

	b.buffer.values["key"] = "42"
	b.buffer.size++
	is.Nil(b.timer)
	is.Len(b.buffer.values, 1)
	is.Equal(1, b.buffer.size)

	b.Stop()
	is.Nil(b.timer)
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)
}

func TestBatchImpl_Stop_withTimer(t *testing.T) {
	is := assert.New(t)

	b := newBatch(42, 5*time.Millisecond, mockDoOk)
	is.NotNil(b.timer)
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)

	b.buffer.values["key"] = "42"
	b.buffer.size++
	is.NotNil(b.timer)
	is.Len(b.buffer.values, 1)
	is.Equal(1, b.buffer.size)

	b.Stop()
	is.Nil(b.timer)
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)
}

func TestBatchImpl_Flush_noTimer(t *testing.T) {
	is := assert.New(t)

	b := newBatch(42, 0, mockDoOk)
	is.Nil(b.timer)
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)

	b.buffer.values["key"] = "42"
	b.buffer.size++
	is.Nil(b.timer)
	is.Len(b.buffer.values, 1)
	is.Equal(1, b.buffer.size)

	b.Flush()
	is.Nil(b.timer)
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)
}

func TestBatchImpl_Flush_withTimer(t *testing.T) {
	is := assert.New(t)

	b := newBatch(42, 5*time.Millisecond, mockDoOk)
	defer b.Stop()
	is.NotNil(b.timer)
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)

	b.buffer.values["key"] = "42"
	b.buffer.size++
	is.NotNil(b.timer)
	is.Len(b.buffer.values, 1)
	is.Equal(1, b.buffer.size)

	b.Flush()
	is.NotNil(b.timer)
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)
}

func TestBatchImpl_Do_noTimer(t *testing.T) {
	is := assert.New(t)

	b := newBatch(3, 0, mockDoOk)
	defer b.Stop()
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)

	start := time.Now()
	go func() {
		time.Sleep(5 * time.Millisecond)
		b.mu.Lock()
		is.Len(b.buffer.values, 2)
		is.Equal(2, b.buffer.size)
		b.mu.Unlock()
		result, err := b.Do("1")
		is.Nil(err)
		is.Equal("11", result)
	}()
	go func() {
		result, err := b.Do("2")
		is.Nil(err)
		is.Equal("22", result)
	}()
	result, err := b.Do("42")
	is.InEpsilon(5*time.Millisecond, time.Since(start), float64(1*time.Millisecond))
	is.Nil(err)
	is.Equal("4242", result)
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)
}

func TestBatchImpl_Do_withTimer(t *testing.T) {
	is := assert.New(t)

	b := newBatch(42, 5*time.Millisecond, mockDoOk)
	defer b.Stop()
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)

	start := time.Now()
	result, err := b.Do("42")
	is.InEpsilon(5*time.Millisecond, time.Since(start), float64(1*time.Millisecond))
	is.Nil(err)
	is.Equal("4242", result)
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)
}

func TestBatchImpl_Do_dedup(t *testing.T) {
	is := assert.New(t)

	b := newBatch(2, 0, mockDoOk)
	defer b.Stop()
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)

	go func() {
		time.Sleep(5 * time.Millisecond)
		b.mu.Lock()
		is.Len(b.buffer.values, 1)
		is.Equal(1, b.buffer.size)
		b.mu.Unlock()
		result, err := b.Do("1")
		is.Nil(err)
		is.Equal("11", result)
	}()
	go func() {
		result, err := b.Do("42")
		is.Nil(err)
		is.Equal("4242", result)
	}()
	result, err := b.Do("42")
	is.Nil(err)
	is.Equal("4242", result)
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)
}

func TestBatchImpl_Do_error(t *testing.T) {
	is := assert.New(t)

	b := newBatch(2, 0, mockDoKo)
	defer b.Stop()
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)

	go func() {
		time.Sleep(5 * time.Millisecond)
		b.mu.Lock()
		is.Len(b.buffer.values, 1)
		is.Equal(1, b.buffer.size)
		b.mu.Unlock()
		result, err := b.Do("1")
		is.Error(err)
		is.ErrorIs(err, assert.AnError)
		is.Equal("11", result)

	}()
	go func() {
		result, err := b.Do("42")
		is.Error(err)
		is.ErrorIs(err, assert.AnError)
		is.Equal("4242", result)
	}()
	result, err := b.Do("42")
	is.Error(err)
	is.ErrorIs(err, assert.AnError)
	is.Equal("4242", result)
	is.Len(b.buffer.values, 0)
	is.Equal(0, b.buffer.size)
}
