package client

import (
	"fmt"
	"tiny_txn/pkg/a_misc/errmsg"
	scheduler "tiny_txn/pkg/c_scheduler"
	mvstorage "tiny_txn/pkg/f_mv_storage"
	"tiny_txn/pkg/h_wal"
)

var _ Transaction = new(Txn)

type Txn struct {
	ro bool // read only
	sz int  // transaction size

	readTs  uint64 // read timestamp
	writeTs uint64 // write timestamp

	readMap  map[string]uint64 // key -> read timestamp
	writeMap map[string][]byte // key -> value

	mvStorage mvstorage.MvStorage
	wal       wal.Wal
	scheduler scheduler.TsoScheduler
}

func NewTxn(ro bool, mvStore mvstorage.MvStorage, wal wal.Wal, schdlr scheduler.Scheduler) Transaction {
	return &Txn{
		mvStorage: mvStore,
		wal:       wal,
		ro:        ro,
		//scheduler: schdlr,
		readTs:   schdlr.Begin(),
		readMap:  make(map[string]uint64),
		writeMap: make(map[string][]byte),
	}
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

	// read from mvStorage store
	val, rts, err := tx.mvStorage.Get(key, tx.readTs)
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

	// write to wal
	log := make([]byte, tx.sz)
	if err = tx.wal.Append(log); err != nil {
		fmt.Printf("transaction start failed: %v\n", err)
	}

	// write to mvStorage store
	for k, v := range tx.writeMap {
		if v == nil {
			if err = tx.mvStorage.Del(k, tx.writeTs); err != nil {
				return err
			}
		} else if err = tx.mvStorage.Set(k, tx.writeTs, v); err != nil {
			return err
		}
	}
	if err = tx.wal.Append(log); err != nil {
		fmt.Printf("transaction append record failed: %v\n", err)
	}

	// update cache
	err = tx.wal.SyncCache()
	if err != nil {
		return err
	}

	if err = tx.scheduler.Done(tx.writeTs); err != nil {
		return err
	}
	return nil
}

func (tx *Txn) Rollback() error {
	//TODO implement me
	panic("implement me")
}
