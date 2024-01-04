package pkg

import (
	"sync/atomic"
)

type Db struct {
	stopped  atomic.Bool
	oracle   *Oracle
	executor *Executor
	mvStore  *MvStore
}

func New() *Db {
	mvStore := NewMVStore()

	return &Db{
		oracle:   NewOracle(),
		executor: NewTransactionExecutor(mvStore),
		mvStore:  mvStore,
	}
}

func (db *Db) View(fn func(txn *Txn) error) error {
	if db.stopped.Load() {
		return DbAlreadyStoppedErr
	}

	readTs := db.oracle.NewReadTs()
	snapshot := db.mvStore.Snapshot(readTs)
	txn := NewTxn(false, readTs, snapshot, db.oracle, db.executor)
	defer txn.Discard()

	return fn(txn)
}

func (db *Db) Update(fn func(txn *Txn) error) error {
	if db.stopped.Load() {
		return DbAlreadyStoppedErr
	}

	readTs := db.oracle.NewReadTs()
	snapshot := db.mvStore.Snapshot(readTs)
	txn := NewTxn(true, readTs, snapshot, db.oracle, db.executor)
	defer txn.Discard() // defer txn.Discard() to remove the txn from the oracle's activeTxns in case of failure.

	if err := fn(txn); err != nil {
		return err
	}
	return txn.Commit()
}

func (db *Db) Stop() {
	if db.stopped.CompareAndSwap(false, true) {
		db.oracle.Stop()
		db.executor.Stop()
	}
}
