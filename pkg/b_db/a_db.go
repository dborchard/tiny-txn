package client

import (
	scheduler "tiny_txn/pkg/c_scheduler"
	mvcc "tiny_txn/pkg/f_mvcc"
	wal "tiny_txn/pkg/h_wal"
)

var _ Client = new(DB)

type DB struct {
	schd scheduler.Scheduler
	mvcc mvcc.MVCC
}

func Open() Client {

	mvccStore := mvcc.New()

	diskWal := wal.New()
	ts, err := diskWal.Recover()
	if err != nil {
		panic(err)
	}

	schd := scheduler.New(ts, diskWal)

	return &DB{
		mvcc: mvccStore,
		schd: schd,
	}
}

func (db *DB) NewTransaction() Transaction {
	//TODO implement me
	panic("implement me")
}

func (db *DB) GetLatestCommitTS() uint64 {
	//TODO implement me
	panic("implement me")
}

func (db *DB) Close() error {
	//TODO implement me
	panic("implement me")
}
