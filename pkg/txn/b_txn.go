package txn

type Txn struct {
	rw        bool
	beginTs   uint64
	oracle    *Oracle
	executor  *Executor
	discarded bool

	// workspace
	writeSet *Batch
	readSet  [][]byte
	snapshot *Snapshot
}

func NewTxn(rw bool, beginTs uint64, snap *Snapshot, scheduler *Oracle, executor *Executor) *Txn {
	return &Txn{
		rw:       rw,
		beginTs:  beginTs,
		snapshot: snap,
		oracle:   scheduler,
		executor: executor,
		readSet:  make([][]byte, 0),
		writeSet: &Batch{},
	}
}

func (txn *Txn) Rollback() {
	if txn.discarded { // Avoid a re-Run.
		return
	}
	txn.discarded = true
	txn.oracle.DoneRead(txn)
}

func (txn *Txn) Get(key []byte) (Value, bool) {
	if txn.rw {
		if value, ok := txn.writeSet.Get(key); ok {
			return NewValue(value), true
		}
		txn.readSet = append(txn.readSet, key)
	}

	return txn.snapshot.Get(key)
}

func (txn *Txn) Set(key []byte, value []byte) error {
	if !txn.rw {
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

	{
		// WAL start entry | START
		// WAL write entry | DATA
	}

	doneCh := txn.executor.sendToWriteCh(txn.writeSet.ToExecutorReq(commitTs))

	{
		// WAL commit entry | END
	}
	{
		<-doneCh                        // wait here for write to be done.
		txn.oracle.DoneCommit(commitTs) // update the oracle's commitTs.
	}

	return nil
}
