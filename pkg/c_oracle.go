package pkg

import (
	"context"
	"fmt"
	"sync"
)

type Oracle struct {
	clockLock *sync.Mutex
	nextTs    uint64

	mu              *sync.Mutex
	txnExecutor     *TxnExecutor
	beginTsWaitMgr  *Waiter
	commitTsWaitMgr *Waiter
	committedTxns   []CommittedTxn
}

type CommittedTxn struct {
	commitTs uint64
	txn      *Txn
}

func New(txnExecutor *TxnExecutor) *Oracle {
	mgr := &Oracle{
		nextTs: 1,

		txnExecutor:     txnExecutor,
		beginTsWaitMgr:  NewWaiter("begin"),
		commitTsWaitMgr: NewWaiter("commit"),
	}
	mgr.beginTsWaitMgr.Finish(0)
	mgr.commitTsWaitMgr.Finish(0)

	return mgr
}

func (s *Oracle) Begin() uint64 {
	s.clockLock.Lock()
	beginTs := s.nextTs - 1
	s.beginTsWaitMgr.Begin(beginTs)
	s.clockLock.Unlock()

	_ = s.commitTsWaitMgr.WaitForMark(context.Background(), beginTs)
	return beginTs
}

func (s *Oracle) MayCommit(txn *Txn) (uint64, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	// end beginTs
	if s.hasConflictFor(txn) {
		return 0, fmt.Errorf("txn conflict")
	}
	s.beginTsWaitMgr.Finish(txn.beginTs)

	s.gc()

	// start commitTs
	s.clockLock.Lock()
	defer s.clockLock.Unlock()
	commitTimestamp := s.nextTs
	s.nextTs = s.nextTs + 1
	s.committedTxns = append(s.committedTxns, CommittedTxn{
		commitTs: commitTimestamp,
		txn:      txn,
	})
	s.beginTsWaitMgr.Begin(commitTimestamp)
	return commitTimestamp, nil
}

func (s *Oracle) hasConflictFor(txn *Txn) bool {
	for _, committed := range s.committedTxns {
		if committed.commitTs <= txn.beginTs {
			continue
		}

		for readKey, _ := range txn.readSet {
			for committedKey, _ := range committed.txn.writeSet {
				if readKey == committedKey {
					return true
				}
			}

		}
	}
	return false
}

func (s *Oracle) gc() {
	doneTill := s.beginTsWaitMgr.DoneTill()
	updatedCommittedTxns := make([]CommittedTxn, 0)
	for _, txn := range s.committedTxns {
		if txn.commitTs <= doneTill {
			continue
		}
		updatedCommittedTxns = append(updatedCommittedTxns, txn)
	}
	s.committedTxns = updatedCommittedTxns
}

func (s *Oracle) Close() {
	s.beginTsWaitMgr.Stop()
	s.commitTsWaitMgr.Stop()
	s.txnExecutor.Stop()
}
