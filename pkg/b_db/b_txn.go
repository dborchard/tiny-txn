package client

import (
	"tiny_txn/pkg/a_misc/errmsg"
	scheduler "tiny_txn/pkg/c_scheduler"
	storage "tiny_txn/pkg/e_mvcc"
	"tiny_txn/pkg/h_wal"
)

var _ Transaction = new(Txn)

type Txn struct {
	ro bool // read only

	readTs  uint64 // read timestamp
	writeTs uint64 // write timestamp

	readMap  map[string]uint64 // key -> read timestamp
	writeMap map[string][]byte // key -> value

	mvcc      storage.MVCC
	wal       wal.Writer
	scheduler scheduler.Scheduler
}

func (tx *Txn) Set(key string, val []byte) error {
	switch {
	case tx.ro:
		return errmsg.ReadOnlyTransaction
	case len(key) == 0:
		return errmsg.KeyIsEmpty
	}
	tx.writeMap[key] = val
	return nil
}

func (tx *Txn) Del(key string) error {
	switch {
	case tx.ro:
		return errmsg.ReadOnlyTransaction
	case len(key) == 0:
		return errmsg.KeyIsEmpty
	}
	tx.writeMap[key] = nil
	return nil
}

func (tx *Txn) Get(key string) (item []byte, err error) {
	if len(key) == 0 {
		return nil, errmsg.KeyIsEmpty
	}

	// check and read from write cache
	if !tx.ro {
		if v, ok := tx.writeMap[key]; ok {
			if v == nil {
				return nil, errmsg.NotExist
			}
			return v, nil
		}
	}

	// read from mvcc store
	val, rts, err := tx.mvcc.Get(key, tx.readTs)
	if err != nil {
		return nil, err
	}

	// update write cache
	if !tx.ro {
		tx.readMap[key] = rts
	}
	return val, nil
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
	if tx.ro {
		return errmsg.ReadOnlyTransaction
	}

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
