package db

import (
	"sync/atomic"
	"tiny_txn/pkg/txn"
)

type Db struct {
	stopped  atomic.Bool
	oracle   *txn.Oracle
	executor *txn.Executor
	mvStore  *txn.MvStore
}

func New() *Db {
	mvStore := txn.NewMVStore()

	return &Db{
		oracle:   txn.NewOracle(),
		executor: txn.NewTransactionExecutor(mvStore),
		mvStore:  mvStore,
	}
}

func (db *Db) View(fn func(txn *txn.Txn) error) error {
	if db.stopped.Load() {
		return txn.DbAlreadyStoppedErr
	}

	readTs := db.oracle.NewReadTs()
	snapshot := db.mvStore.Snapshot(readTs)
	txn := txn.NewTxn(false, readTs, snapshot, db.oracle, db.executor)
	defer txn.Discard()

	return fn(txn)
}

func (db *Db) Update(fn func(txn *txn.Txn) error) error {
	if db.stopped.Load() {
		return txn.DbAlreadyStoppedErr
	}

	readTs := db.oracle.NewReadTs()
	snapshot := db.mvStore.Snapshot(readTs)
	txn := txn.NewTxn(true, readTs, snapshot, db.oracle, db.executor)
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
