package scheduler

import (
	"sync/atomic"
	"time"
	"tiny_txn/pkg/a_misc/errmsg"
	tsmarker "tiny_txn/pkg/d_ts_marker"
	cache "tiny_txn/pkg/f_cache"
	checkpoint "tiny_txn/pkg/g_checkpoint"
	wal "tiny_txn/pkg/h_wal"
)

type TxnScheduler struct {
	ts    uint64
	minTs uint64 // min ts

	beginTsMarker  tsmarker.BeginTsMarker
	commitTsMarker tsmarker.CommitTsMarker

	stopCh       chan struct{} // for halting until the current transaction is done.
	reqCh        chan *request
	checkPointer checkpoint.CheckPointer
}

var _ Scheduler = &TxnScheduler{}

func New(ts uint64, w wal.Wal) Scheduler {
	return &TxnScheduler{
		ts:    ts,
		minTs: ts,

		stopCh:         make(chan struct{}),
		beginTsMarker:  tsmarker.NewBegin(),
		commitTsMarker: tsmarker.NewCommit(),
		reqCh:          make(chan *request, 1024),
		checkPointer: &checkpoint.DiskCheckPointer{
			Started: true,
			Ts:      time.Now(),

			Wal:   w,
			Cache: cache.New(),
		},
	}
}

func (s *TxnScheduler) Run() {
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

func (s *TxnScheduler) process(m *request) {
	switch m.typ {
	case Start:
		s.beginTsMarker.Add(m.ts)
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
			s.commitTsMarker.Add(k, nextTs)
		}

		if s.beginTsMarker.Del(m.ts) {
			s.minTs = m.ts
		}
		m.responseCh <- &response{err, nextTs}
	}
}

func (s *TxnScheduler) hasConflict(m *request) error {
	if len(m.reads) == 0 {
		return nil
	}

	for key, rts := range m.reads {
		if commit, has := s.commitTsMarker.Get(key); has && commit.Ts > rts {
			return errmsg.TransactionConflict
		}
	}
	return nil
}

func (s *TxnScheduler) gc() {
	s.commitTsMarker.Del(s.minTs)
}

func (s *TxnScheduler) Stop() {
	s.stopCh <- struct{}{}
	<-s.stopCh
}

func (s *TxnScheduler) Start() uint64 {
	ts := atomic.LoadUint64(&s.ts)
	s.reqCh <- &request{typ: Start, ts: ts}
	return ts
}

func (s *TxnScheduler) Commit(ts uint64, readCache map[string]uint64, writeCache map[string][]byte) (uint64, error) {
	responseCh := make(chan *response)
	s.reqCh <- &request{Commit, ts, responseCh, readCache, writeCache}
	resp := <-responseCh
	return resp.ts, resp.err
}

func (s *TxnScheduler) Done(ts uint64) error {
	responseCh := make(chan *response)
	s.reqCh <- &request{typ: Done, ts: ts, responseCh: responseCh}
	resp := <-responseCh
	return resp.err
}
