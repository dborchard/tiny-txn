package mvcc

import (
	"tiny_txn/pkg/a_misc/errmsg"
	mvstorage "tiny_txn/pkg/e_mv_storage"
)

var _ Transaction = new(Txn)

type Txn struct {
	ro       bool // read only
	beginTs  uint64
	commitTs uint64

	readSet  map[string]uint64 // key -> read timestamp
	writeSet map[string][]byte // key -> value

	mvStorage mvstorage.MvStorage
	mgr       *TxnManager
}

type CommittedTxn struct {
	commitTs uint64
	txn      *Txn
}

func NewTxn(ro bool, mgr *TxnManager) Transaction {
	return &Txn{
		mvStorage: mgr.txnExecutor.mvStorage,
		ro:        ro,
		beginTs:   mgr.Begin(),
		readSet:   make(map[string]uint64),
		writeSet:  make(map[string][]byte),
		mgr:       mgr,
	}
}

func (tx *Txn) Get(key string) (item []byte, err error) {
	if len(key) == 0 {
		return nil, errmsg.KeyIsEmpty
	}

	// check and read from write cache
	if !tx.ro {
		if v, ok := tx.writeSet[key]; ok {
			if v == nil {
				return nil, errmsg.NotExist
			}
			return v, nil
		}
	}

	// read from storage store
	val, rts, err := tx.mvStorage.Get(key, tx.beginTs)
	if err != nil {
		return nil, err
	}

	// update write cache
	if !tx.ro {
		tx.readSet[key] = rts
	}
	return val, nil
}

func (tx *Txn) Set(key string, val []byte) error {
	switch {
	case tx.ro:
		return errmsg.ReadOnlyTransaction
	case len(key) == 0:
		return errmsg.KeyIsEmpty
	}
	tx.writeSet[key] = val
	return nil
}

func (tx *Txn) Del(key string) error {
	switch {
	case tx.ro:
		return errmsg.ReadOnlyTransaction
	case len(key) == 0:
		return errmsg.KeyIsEmpty
	}
	tx.writeSet[key] = nil
	return nil
}

func (tx *Txn) Commit() error {
	if tx.ro {
		return nil
	}
	if len(tx.writeSet) == 0 {
		return errmsg.EmptyTransaction
	}

	tx.mgr.txnExecutor.Lock()
	defer tx.mgr.txnExecutor.Unlock()

	var err error
	tx.commitTs, err = tx.mgr.MayCommit(tx)
	if err != nil {
		return err
	}

	responseCh := make(chan *response)
	tx.mgr.txnExecutor.Submit(request{
		typ:        Commit,
		ts:         tx.commitTs,
		responseCh: responseCh,
		writeMap:   tx.writeSet,
	})
	<-responseCh

	tx.mgr.commitTsWaitMgr.Finish(tx.commitTs)
	return nil
}

func (tx *Txn) Rollback() error {
	//TODO implement me
	panic("implement me")
}
