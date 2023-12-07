package client

import (
	scheduler "tiny_txn/pkg/c_scheduler"
	storage "tiny_txn/pkg/e_mvcc"
	"tiny_txn/pkg/h_wal"
)

var _ Transaction = new(Txn)

type Txn struct {
	readTs  uint64 // read timestamp
	writeTs uint64 // write timestamp

	readMap  map[string]uint64 // key -> read timestamp
	writeMap map[string][]byte // key -> value

	mvcc      storage.MVCC
	wal       wal.Writer
	scheduler scheduler.Scheduler
}

func (tx *Txn) Set(key string, val []byte) error {
	//TODO implement me
	panic("implement me")
}

func (tx *Txn) Del(key string) error {
	//TODO implement me
	panic("implement me")
}

func (tx *Txn) Get(key string) (item []byte, err error) {
	//TODO implement me
	panic("implement me")
}

func (tx *Txn) ApplySnapshot(data []byte) error {
	//TODO implement me
	panic("implement me")
}

func (tx *Txn) Snapshot() ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *Txn) Commit() error {
	var err error
	tx.writeTs, err = tx.scheduler.Commit(tx.readTs, tx.readMap, tx.writeMap)
	if err != nil {
		return err
	}

	return nil
}

func (tx *Txn) Rollback() error {
	//TODO implement me
	panic("implement me")
}
