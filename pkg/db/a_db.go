package db

import (
	"sync/atomic"
	"tiny_txn/pkg/txn"
)

type Db struct {
	stopped   atomic.Bool
	scheduler *txn.Oracle
	executor  *txn.Executor
	mvStore   *txn.MvStore
}

func New() *Db {
	mvStore := txn.NewMVStore()

	return &Db{
		scheduler: txn.NewScheduler(),
		executor:  txn.NewTransactionExecutor(mvStore),
		mvStore:   mvStore,
	}
}

func (db *Db) View(fn func(txn *txn.Txn) error) error {
	if db.stopped.Load() {
		return txn.DbAlreadyStoppedErr
	}

	readTs := db.scheduler.NewReadTs()
	snapshot := db.mvStore.Snapshot(readTs)
	newTxn := txn.NewTxn(false, readTs, snapshot, db.scheduler, db.executor)
	defer newTxn.Rollback()

	return fn(newTxn)
}

func (db *Db) Update(fn func(txn *txn.Txn) error) error {
	if db.stopped.Load() {
		return txn.DbAlreadyStoppedErr
	}

	readTs := db.scheduler.NewReadTs()
	snapshot := db.mvStore.Snapshot(readTs)
	newTxn := txn.NewTxn(true, readTs, snapshot, db.scheduler, db.executor)
	defer newTxn.Rollback() // defer newTxn.Rollback() to remove the newTxn from the scheduler's activeTxns in case of failure.

	if err := fn(newTxn); err != nil {
		return err
	}
	return newTxn.Commit()
}

func (db *Db) Stop() {
	if db.stopped.CompareAndSwap(false, true) {
		db.scheduler.Stop()
		db.executor.Stop()
	}
}
