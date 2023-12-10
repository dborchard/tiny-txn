package mvcc

import "sync/atomic"

type Transaction interface {
	Set(key string, val []byte) error
	Get(key string) (val []byte, err error)
	Del(key string) error

	Commit() error
	Rollback() error
}

type Manager interface {
	Close()

	Begin() uint64
	MayCommit(txn *Txn) (uint64, error)
}

type requestType int

const (
	Commit requestType = iota // commit
	Done                      // done
	Start                     // start
)

type request struct {
	typ        requestType
	ts         uint64
	responseCh chan *response
	reads      map[string]uint64 // key -> ts
	writeMap   map[string][]byte // key -> value
}

type response struct {
	err error
	ts  uint64
}

type Clock interface {
	Now() uint64
}

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
