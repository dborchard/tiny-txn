package pkg

type ExecutorRequest struct {
	commitTs uint64
	batch    *Batch

	doneCh chan struct{}
}

func (batch *Batch) ToExecutorReq(commitTs uint64) ExecutorRequest {
	return ExecutorRequest{
		commitTs: commitTs,
		batch:    batch,

		doneCh: make(chan struct{}),
	}
}

func (tsBatch ExecutorRequest) AllPairs() []Pair[[]byte, []byte] {
	return tsBatch.batch.pairs
}
