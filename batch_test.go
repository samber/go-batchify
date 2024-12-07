package batchify

import (
	"testing"

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
