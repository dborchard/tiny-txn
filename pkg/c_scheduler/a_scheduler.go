package scheduler

import (
	"sync/atomic"
	"time"
	"tiny_txn/pkg/a_misc/errmsg"
	tsmarker "tiny_txn/pkg/d_ts_waiter"
	cache "tiny_txn/pkg/f_cache"
	checkpoint "tiny_txn/pkg/g_checkpoint"
	wal "tiny_txn/pkg/h_wal"
)

type TsoScheduler struct {
	ts    uint64
	minTs uint64 // min ts

	beginTsWaiter  tsmarker.BeginTsMarker
	commitTsWaiter tsmarker.CommitTsMarker

	stopCh       chan struct{} // for halting until the current transaction is done.
	reqCh        chan *request
	checkPointer checkpoint.CheckPointer
}

var _ Scheduler = &TsoScheduler{}

func New(ts uint64, w wal.Wal) Scheduler {
	return &TsoScheduler{
		ts:    ts,
		minTs: ts,

		stopCh:         make(chan struct{}),
		beginTsWaiter:  tsmarker.NewBegin(),
		commitTsWaiter: tsmarker.NewCommit(),
		reqCh:          make(chan *request, 1024),
		checkPointer: &checkpoint.DiskCheckPointer{
			Started: true,
			Ts:      time.Now(),

			Wal:   w,
			Cache: cache.New(),
		},
	}
}

func (s *TsoScheduler) Run() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-s.stopCh:
			s.stopCh <- struct{}{}
			return
		case m := <-s.reqCh:
			s.process(m)
		case <-ticker.C:
			s.gc()
		}
	}
}

func (s *TsoScheduler) process(m *request) {
	switch m.typ {
	case Start:
		s.beginTsWaiter.Add(m.ts)
	case Done:
		err := s.checkPointer.End(m.ts)
		m.responseCh <- &response{err: err}
	case Commit:
		var err error
		var nextTs uint64

		err = s.hasConflict(m)
		if err != nil {
			m.responseCh <- &response{err: err}
		}

		nextTs = atomic.AddUint64(&s.ts, 1)
		for k, _ := range m.writeMap {
			s.commitTsWaiter.Add(k, nextTs)
		}

		if s.beginTsWaiter.Del(m.ts) {
			s.minTs = m.ts
		}
		m.responseCh <- &response{err, nextTs}
	}
}

func (s *TsoScheduler) hasConflict(m *request) error {
	if len(m.reads) == 0 {
		return nil
	}

	for key, rts := range m.reads {
		if commit, has := s.commitTsWaiter.Get(key); has && commit.Ts > rts {
			return errmsg.TransactionConflict
		}
	}
	return nil
}

func (s *TsoScheduler) gc() {
	s.commitTsWaiter.Del(s.minTs)
}

func (s *TsoScheduler) Stop() {
	s.stopCh <- struct{}{}
	<-s.stopCh
}

func (s *TsoScheduler) Begin() uint64 {
	ts := atomic.LoadUint64(&s.ts)
	s.reqCh <- &request{typ: Start, ts: ts}
	return ts
}

func (s *TsoScheduler) Commit(ts uint64, readCache map[string]uint64, writeCache map[string][]byte) (uint64, error) {
	responseCh := make(chan *response)
	s.reqCh <- &request{Commit, ts, responseCh, readCache, writeCache}
	resp := <-responseCh
	return resp.ts, resp.err
}

func (s *TsoScheduler) Done(ts uint64) error {
	responseCh := make(chan *response)
	s.reqCh <- &request{typ: Done, ts: ts, responseCh: responseCh}
	resp := <-responseCh
	return resp.err
}
