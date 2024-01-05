package txn

import (
	"bytes"
)

type Batch struct {
	pairs []Pair[[]byte, []byte]
}

func (batch *Batch) Add(key, value []byte) error {
	if batch.Contains(key) {
		return BatchDuplicateKeyErr
	}
	batch.pairs = append(batch.pairs, Pair[[]byte, []byte]{key, value})
	return nil
}

func (batch *Batch) Get(key []byte) ([]byte, bool) {
	for _, pair := range batch.pairs {
		if bytes.Compare(pair.Key, key) == 0 {
			return pair.Val, true
		}
	}
	return nil, false
}

func (batch *Batch) Contains(key []byte) bool {
	_, ok := batch.Get(key)
	return ok
}

func (batch *Batch) IsEmpty() bool {
	return len(batch.pairs) == 0
}
