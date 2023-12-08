package coordinator

import "sync/atomic"

type Event struct {
	ts uint64

	waitCh chan struct{}
	done   bool
}

type WaitMgr struct {
	Name     string
	eventCh  chan Event    // channel for incoming messages
	stopCh   chan struct{} // channel for stopping the waiter
	doneTill atomic.Uint64 // max ts of finished txns

	tsHeap        TsHeap         // min heap of txn timestamps
	pendingCounts map[uint64]int // ts -> txn count

	waiters map[uint64][]chan struct{} // ts -> waitChs
}
