package mvstorage

import (
	mvcc "tiny_txn/pkg/b_mvcc"
)

// MvStorage is Multi-Version Storage
type MvStorage interface {
	Get(key string, ts uint64) ([]byte, uint64, error)
	Set(key string, ts uint64, val []byte) error
	Del(key string, ts uint64) error

	Prepare(txn mvcc.Txn) (uint64, error)
	Committing(txn mvcc.Txn) error
	Commit(txn mvcc.Txn) (uint64, error)
	Rollback(txn mvcc.Txn) error

	Start()
	StartRecovery()
	Close()
}
