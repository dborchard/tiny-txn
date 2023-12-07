package scheduler

import (
	"context"
	"time"
	checkpoint "tiny_txn/pkg/g_checkpoint"
)

type TxnScheduler struct {
	ctx   context.Context
	msgCh chan *message
	chkpt checkpoint.Checkpoint
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
		m.resCh <- &result{err: err}
	case Commit:
		//TODO implement me
	}
}

func (s *TxnScheduler) gc() {
	// delete all the done transactions.
}

func (s *TxnScheduler) Stop() {
	//TODO implement me
	panic("implement me")
}

func (s *TxnScheduler) Start() uint64 {
	//TODO implement me
	panic("implement me")
}

func (s *TxnScheduler) Commit(u uint64, m map[string]uint64, m2 map[string][]byte) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (s *TxnScheduler) Done(u uint64) error {
	//TODO implement me
	panic("implement me")
}
