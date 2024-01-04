package pkg

type Txn struct {
	update    bool
	beginTs   uint64
	snapshot  *Snapshot
	oracle    *Oracle
	executor  *Executor
	discarded bool

	// workspace
	writeSet *Batch
	readSet  [][]byte
}

func NewTxn(update bool, beginTs uint64, snap *Snapshot, oracle *Oracle, executor *Executor) *Txn {
	return &Txn{
		update:   update,
		beginTs:  beginTs,
		snapshot: snap,
		oracle:   oracle,
		executor: executor,
		readSet:  make([][]byte, 0),
		writeSet: &Batch{},
	}
}

func (txn *Txn) Discard() {
	if txn.discarded { // Avoid a re-Run.
		return
	}
	txn.discarded = true
	txn.oracle.DoneRead(txn)
}

func (txn *Txn) Get(key []byte) (Value, bool) {
	if txn.update {
		if value, ok := txn.writeSet.Get(key); ok {
			return NewValue(value), true
		}
		txn.readSet = append(txn.readSet, key)
	}

	return txn.snapshot.Get(key)
}

func (txn *Txn) Set(key []byte, value []byte) error {
	if !txn.update {
		return ReadOnlyTxnErr
	}

	err := txn.writeSet.Add(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (txn *Txn) Commit() error {
	if txn.writeSet.IsEmpty() {
		return EmptyTxnErr
	}

	commitTs, err := txn.oracle.NewCommitTs(txn)
	if err != nil {
		return err
	}
	doneCh := txn.executor.sendToWriteCh(txn.writeSet.ToExecutorReq(commitTs))
	{
		<-doneCh                        // wait here for write to be done.
		txn.oracle.DoneCommit(commitTs) // update the oracle's commitTs.
	}

	return nil
}
