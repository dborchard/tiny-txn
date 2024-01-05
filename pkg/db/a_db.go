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
	newTxn := txn.NewTxn(false, readTs, snapshot, db.oracle, db.executor)
	defer newTxn.Discard()

	return fn(newTxn)
}

func (db *Db) Update(fn func(txn *txn.Txn) error) error {
	if db.stopped.Load() {
		return txn.DbAlreadyStoppedErr
	}

	readTs := db.oracle.NewReadTs()
	snapshot := db.mvStore.Snapshot(readTs)
	newTxn := txn.NewTxn(true, readTs, snapshot, db.oracle, db.executor)
	defer newTxn.Discard() // defer newTxn.Discard() to remove the newTxn from the oracle's activeTxns in case of failure.

	if err := fn(newTxn); err != nil {
		return err
	}
	return newTxn.Commit()
}

func (db *Db) Stop() {
	if db.stopped.CompareAndSwap(false, true) {
		db.oracle.Stop()
		db.executor.Stop()
	}
}
