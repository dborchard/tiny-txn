package txn

import "sync"

type Executor struct {
	sync.Mutex // writeCh lock.
	writeCh    chan ExecutorRequest
	stopCh     chan struct{}
	mvStore    *MvStore
}

func NewTransactionExecutor(mvStore *MvStore) *Executor {
	exec := &Executor{
		writeCh: make(chan ExecutorRequest),
		stopCh:  make(chan struct{}),
		mvStore: mvStore,
	}
	go exec.run()
	return exec
}

func (e *Executor) sendToWriteCh(req ExecutorRequest) <-chan struct{} {
	e.Lock()
	defer e.Unlock()
	e.writeCh <- req
	return req.doneCh
}

func (e *Executor) Stop() {
	e.stopCh <- struct{}{}
}

func (e *Executor) run() {
	for {
		select {
		case req := <-e.writeCh:
			e.apply(req)
			e.markApplied(req)
		case <-e.stopCh:
			close(e.writeCh)
			return
		}
	}
}

func (e *Executor) apply(req ExecutorRequest) {
	for _, keyValuePair := range req.AllPairs() {
		e.mvStore.PutOrUpdate(
			NewVersionedKey(keyValuePair.Key, req.commitTs),
			NewValue(keyValuePair.Val),
		)
	}
}

func (e *Executor) markApplied(batch ExecutorRequest) {
	batch.doneCh <- struct{}{}
	close(batch.doneCh)
}
