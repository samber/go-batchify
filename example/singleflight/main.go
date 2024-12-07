package main

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/samber/go-batchify"
	"github.com/samber/lo"
)

func mockSQL(ids []int) (map[int]string, error) {
	time.Sleep(1 * time.Second) // simulate long-running processing
	return lo.SliceToMap(ids, func(item int) (int, string) {
		return item, fmt.Sprintf("item %d", item)
	}), nil
}

// seq 1 10000 | xargs -P 100 -I {} curl http://localhost:4242/
func main() {
	var group singleflight.Group

	batch := batchify.NewBatchWithTimer(
		10,
		mockSQL,
		2*time.Second,
	)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		key := rand.IntN(30)
		keyStr := strconv.Itoa(key)
		_, _, _ = group.Do(keyStr, func() (interface{}, error) {
			return batch.Do(key)
		})

		fmt.Println("Elapsed time:", time.Since(start))
		fmt.Fprintf(w, "Hello, World!\n")
	})

	fmt.Println("Starting server at port 4242")
	if err := http.ListenAndServe(":4240", nil); err != nil {
		fmt.Println("Error starting server:", err)
	}
}
