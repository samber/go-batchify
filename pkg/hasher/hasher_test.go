package hasher

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasher(t *testing.T) {
	is := assert.New(t)

	hasher := Hasher[int](func(i int) uint64 {
		return uint64(i * 2)
	})
	is.Equal(uint64(0), hasher.ComputeHash(0, 42))
	is.Equal(uint64(40), hasher.ComputeHash(20, 42))
	is.Equal(uint64(0), hasher.ComputeHash(21, 42))
	is.Equal(uint64(2), hasher.ComputeHash(22, 42))
}
