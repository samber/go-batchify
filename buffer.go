package batchify

import (
	"sync"

	"github.com/samber/go-batchify/internal"
)

func newBuffer[I comparable, O any](bufferSize int) *buffer[I, O] {
	b := &buffer[I, O]{
		values: make(map[I]O, bufferSize),
		err:    nil,
		size:   0,
		once:   sync.Once{},
		wg:     sync.WaitGroup{},
	}
	b.wg.Add(1)
	return b
}

type buffer[I comparable, O any] struct {
	_ internal.NoCopy

	values map[I]O
	err    error
	size   int
	once   sync.Once
	wg     sync.WaitGroup
}
