package batchify

type Batch[I comparable, O any] interface {
	Do(input I) (output O, err error)
	Flush()
	Stop()
}
