package scheduler

import (
	"context"
	"sync"
	"tiny_txn/pkg/a_misc/errmsg"
	waitmgr "tiny_txn/pkg/e_waitmgr"
)

type TsoScheduler struct {
	clockLock     sync.Mutex
	nextTimestamp uint64

	txnExecutor     *TxnExecutor
	beginTsWaitMgr  *waitmgr.WaitMgr
	commitTsWaitMgr *waitmgr.WaitMgr
	committedTxns   map[string]committedTxn
}

//	func New(ts uint64, w wal.Wal) Scheduler {
//		return &TsoScheduler{
//			ts:    ts,
//			minTs: ts,
//
//			stopCh:         make(chan struct{}),
//			beginTsWaiter:  waitmgr.NewBegin(),
//			commitTsWaiter: waitmgr.NewCommit(),
//			reqCh:          make(chan *request, 1024),
//			checkPointer: &checkpoint.DiskCheckPointer{
//				Started: true,
//				Ts:      time.Now(),
//
//				Wal:   w,
//				Cache: cache.New(),
//			},
//		}
//	}
func (s *TsoScheduler) gc() {
	// remove old transactions
	doneTill := s.beginTsWaitMgr.DoneTill()

	for _, txn := range s.committedTxns {
		if txn.ts <= doneTill {
			continue
		}
		// delete old transactions

	}
}

func (s *TsoScheduler) Stop() {
	s.beginTsWaitMgr.Stop()
	s.commitTsWaitMgr.Stop()
	s.txnExecutor.Stop()
}

func (s *TsoScheduler) Begin() uint64 {
	s.clockLock.Lock()
	beginTs := s.nextTimestamp - 1
	s.beginTsWaitMgr.Begin(beginTs)
	s.clockLock.Unlock()

	_ = s.commitTsWaitMgr.WaitForMark(context.Background(), beginTs)
	return beginTs
}

func (s *TsoScheduler) Commit(ts uint64, readCache map[string]uint64, writeCache map[string][]byte) (uint64, error) {

	if s.hasConflictFor(ts, readCache, writeCache) {
		return 0, errmsg.TransactionConflict
	}

	s.clockLock.Lock()
	defer s.clockLock.Unlock()

	s.beginTsWaitMgr.Finish(ts)

	commitTimestamp := s.nextTimestamp
	s.nextTimestamp = s.nextTimestamp + 1

	s.beginTsWaitMgr.Finish(commitTimestamp)
	return commitTimestamp, nil
}

func (s *TsoScheduler) hasConflictFor(txnTs uint64, readCache map[string]uint64, writeCache map[string][]byte) bool {
	if len(readCache) == 0 {
		return false
	}

	for _, committed := range s.committedTxns {
		if committed.ts <= txnTs {
			continue
		}

		for key, _ := range writeCache {
			for _, ckey := range committed.keys {
				if ckey == key {
					return true
				}
			}

		}
	}
	return false
}

func (s *TsoScheduler) Done(ts uint64) error {
	s.beginTsWaitMgr.Finish(ts)
	return nil
}
