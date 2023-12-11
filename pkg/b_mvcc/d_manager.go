package mvcc

import (
	"context"
	"sync"
	"tiny_txn/pkg/a_misc/errmsg"
)

type TxnManager struct {
	clockLock *sync.Mutex
	nextTs    uint64

	mu              *sync.Mutex
	txnExecutor     *TxnExecutor
	beginTsWaitMgr  *WaitMgr
	commitTsWaitMgr *WaitMgr
	committedTxns   []CommittedTxn
}

func New(txnExecutor *TxnExecutor) *TxnManager {
	mgr := &TxnManager{
		nextTs: 1,

		txnExecutor:     txnExecutor,
		beginTsWaitMgr:  NewTxnSyncManager("begin"),
		commitTsWaitMgr: NewTxnSyncManager("commit"),
	}
	mgr.beginTsWaitMgr.Finish(0)
	mgr.commitTsWaitMgr.Finish(0)

	return mgr
}

func (s *TxnManager) Begin() uint64 {
	s.clockLock.Lock()
	beginTs := s.nextTs - 1
	s.beginTsWaitMgr.Begin(beginTs)
	s.clockLock.Unlock()

	_ = s.commitTsWaitMgr.WaitForMark(context.Background(), beginTs)
	return beginTs
}

func (s *TxnManager) MayCommit(txn *Txn) (uint64, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.hasConflictFor(txn) {
		return 0, errmsg.TransactionConflict
	}

	s.beginTsWaitMgr.Finish(txn.beginTs)
	s.gc()

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

func (s *TxnManager) hasConflictFor(txn *Txn) bool {
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

func (s *TxnManager) gc() {
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

func (s *TxnManager) Close() {
	s.beginTsWaitMgr.Stop()
	s.commitTsWaitMgr.Stop()
	s.txnExecutor.Stop()
}
