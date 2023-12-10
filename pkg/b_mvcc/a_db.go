package mvcc

import (
	mvstorage "tiny_txn/pkg/e_mv_storage"
)

type DB struct {
	manager *TxnManager
}

func Open() *DB {

	mvStorage := mvstorage.New()
	txnExecutor := NewTransactionExecutor(mvStorage)
	txnManager := New(txnExecutor)

	return &DB{manager: txnManager}
}

func (db *DB) Get(key string) (val []byte, err error) {
	transaction := NewTxn(true, db.manager)
	defer func(transaction Transaction) {
		_ = transaction.Commit()
	}(transaction)

	val, err = transaction.Get(key)
	return
}

func (db *DB) Set(key string, val []byte) (err error) {
	transaction := NewTxn(false, db.manager)

	defer func(transaction Transaction) {
		err := transaction.Commit()
		if err != nil {
			_ = transaction.Rollback()
		}
	}(transaction)

	err = transaction.Set(key, val)
	return
}
