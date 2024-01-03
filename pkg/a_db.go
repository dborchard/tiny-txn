package pkg

type DB struct {
	manager *Oracle
}

func Open() *DB {

	mvStorage := &Workspace{memtable: &MV{}}
	txnExecutor := NewTransactionExecutor(mvStorage)
	txnManager := New(txnExecutor)

	return &DB{manager: txnManager}
}

func (db *DB) Get(key string) (val []byte, err error) {
	transaction := NewTxn(true, db.manager)
	defer func(transaction Txn) {
		_ = transaction.Commit()
	}(transaction)

	val, err = transaction.Get(key)
	return
}

func (db *DB) Set(key string, val []byte) (err error) {
	transaction := NewTxn(false, db.manager)

	defer func(transaction Txn) {
		err := transaction.Commit()
		if err != nil {
			_ = transaction.Rollback()
		}
	}(transaction)

	err = transaction.Set(key, val)
	return
}
