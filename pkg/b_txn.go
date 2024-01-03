package pkg

import "fmt"

type Txn struct {
	ro       bool // read only
	beginTs  uint64
	commitTs uint64

	readSet  map[string]uint64 // key -> read ts
	writeSet map[string][]byte // key -> value

	workspace *Workspace
	oracle    *Oracle
}

func NewTxn(ro bool, mgr *Oracle) Txn {
	return Txn{
		workspace: mgr.txnExecutor.workSpace,
		ro:        ro,
		beginTs:   mgr.Begin(),
		readSet:   make(map[string]uint64),
		writeSet:  make(map[string][]byte),
		oracle:    mgr,
	}
}

func (tx *Txn) Get(key string) (item []byte, err error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key is empty")
	}

	// check and read from write cache
	if !tx.ro {
		if v, ok := tx.writeSet[key]; ok {
			if v == nil {
				return nil, fmt.Errorf("key not exist")
			}
			return v, nil
		}
	}

	// read from storage store
	val, rts, err := tx.workspace.get(key, tx.beginTs)
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
		return fmt.Errorf("read only transaction")
	case len(key) == 0:
		return fmt.Errorf("key is empty")
	}
	tx.writeSet[key] = val
	return nil
}

func (tx *Txn) Del(key string) error {
	switch {
	case tx.ro:
		return fmt.Errorf("read only transaction")
	case len(key) == 0:
		return fmt.Errorf("key is empty")
	}
	tx.writeSet[key] = nil
	return nil
}

func (tx *Txn) Commit() error {
	if tx.ro {
		return nil
	}
	if len(tx.writeSet) == 0 {
		return fmt.Errorf("write set is empty")
	}

	var err error
	tx.commitTs, err = tx.oracle.MayCommit(tx)
	if err != nil {
		return err
	}

	doneCh := make(chan struct{})
	tx.oracle.txnExecutor.Submit(TsBatch{
		ts:     tx.commitTs,
		doneCh: doneCh,
		batch:  tx.writeSet,
	})
	<-doneCh

	tx.oracle.commitTsWaitMgr.Finish(tx.commitTs)
	return nil
}

func (tx *Txn) Rollback() error {
	//TODO implement me
	panic("implement me")
}
