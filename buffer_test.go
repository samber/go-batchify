package batchify

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBuffer(t *testing.T) {
	is := assert.New(t)

	bufferSize := 10
	buf := newBuffer[int, string](bufferSize)

	// is.Equal(bufferSize, cap(buf.values))
	is.Equal(0, buf.size)
}
