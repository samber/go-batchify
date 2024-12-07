package batchify

import (
	"os"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

// https://github.com/stretchr/testify/issues/1101
func testWithTimeout(t *testing.T, timeout time.Duration) { //nolint:unused
	t.Helper()

	testFinished := make(chan struct{})
	t.Cleanup(func() { close(testFinished) })

	go func() {
		select {
		case <-testFinished:
		case <-time.After(timeout):
			t.Errorf("test timed out after %s", timeout)
			os.Exit(1)
		}
	}()
}

func mockDoOk(keys []string) (map[string]string, error) {
	return lo.SliceToMap(keys, func(key string) (string, string) {
		return key, key + key
	}), nil
}

func mockDoKo(keys []string) (map[string]string, error) {
	return lo.SliceToMap(keys, func(key string) (string, string) {
		return key, key + key
	}), assert.AnError
}

func mockHasher(key string) uint64 {
	return uint64(len(key))
}
