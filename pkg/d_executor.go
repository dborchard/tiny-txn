package pkg

type TsBatch struct {
	batch          map[string][]byte
	ts             uint64
	doneCh         chan struct{}
	commitCallback func()
}

type TxnExecutor struct {
	batchCh   chan TsBatch
	stopCh    chan struct{}
	workSpace *Workspace
}

func NewTransactionExecutor(workspace *Workspace) *TxnExecutor {
	txnExecutor := &TxnExecutor{
		batchCh:   make(chan TsBatch),
		stopCh:    make(chan struct{}),
		workSpace: workspace,
	}
	go txnExecutor.run()
	return txnExecutor
}

func (executor *TxnExecutor) Submit(batch TsBatch) <-chan struct{} {
	executor.batchCh <- batch
	return batch.doneCh
}

func (executor *TxnExecutor) Stop() {
	executor.stopCh <- struct{}{}
}

func (executor *TxnExecutor) run() {
	for {
		select {
		case tsBatch := <-executor.batchCh:
			executor.workSpace.put(tsBatch.batch, tsBatch.ts)
			tsBatch.commitCallback()

			tsBatch.doneCh <- struct{}{}
			close(tsBatch.doneCh)
		case <-executor.stopCh:
			close(executor.batchCh)
			return
		}
	}
}
