package scheduler

import (
	"context"
	"sync/atomic"
	"time"
	checkpoint "tiny_txn/pkg/g_checkpoint"
)

type TxnScheduler struct {
	ts uint64

	ctx      context.Context
	cancelFn context.CancelFunc

	msgCh chan *message
	chkpt checkpoint.CheckPointer
}

var _ Scheduler = &TxnScheduler{}

func (s *TxnScheduler) Run() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-s.ctx.Done():
			return
		case m := <-s.msgCh:
			s.process(m)
		case <-ticker.C:
			s.gc()
		}
	}
}

func (s *TxnScheduler) process(m *message) {
	switch m.typ {
	case Start:
		// TODO implement me
	case Done:
		err := s.chkpt.End(m.ts)
		m.responseCh <- &result{err: err}
	case Commit:
		//TODO implement me
	}
}

func (s *TxnScheduler) gc() {
	// delete all the done transactions.
}

func (s *TxnScheduler) Stop() {
	s.cancelFn()
}

func (s *TxnScheduler) Start() uint64 {
	ts := atomic.LoadUint64(&s.ts)
	s.msgCh <- &message{typ: Start, ts: ts}
	return ts
}

func (s *TxnScheduler) Commit(ts uint64, readCache map[string]uint64, writeCache map[string][]byte) (uint64, error) {
	responseCh := make(chan *result)
	s.msgCh <- &message{Commit, ts, responseCh, readCache, writeCache}
	response := <-responseCh
	return response.ts, response.err
}

func (s *TxnScheduler) Done(u uint64) error {
	//TODO implement me
	panic("implement me")
}
