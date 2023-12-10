package client

import (
	scheduler "tiny_txn/pkg/c_scheduler"
	mvstorage "tiny_txn/pkg/f_mv_storage"
	wal "tiny_txn/pkg/h_wal"
)

var _ Client = new(DB)

type DB struct {
	schd      scheduler.Scheduler
	mvStorage mvstorage.MvStorage
}

func Open() Client {

	mvStorage := mvstorage.New()

	diskWal := wal.New()
	ts, err := diskWal.Recover()
	if err != nil {
		panic(err)
	}

	schd := scheduler.New(ts, diskWal)

	return &DB{
		mvStorage: mvStorage,
		schd:      schd,
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
