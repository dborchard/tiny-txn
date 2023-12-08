package scheduler

import (
	"sync"
	mvcc "tiny_txn/pkg/f_mvcc"
)

type TxnExecutor struct {
	sync.Mutex
	reqCh  chan request
	stopCh chan struct{}
	mvcc   mvcc.MVCC
}

func (e *TxnExecutor) Submit(req request) <-chan *response {
	e.reqCh <- req
	return req.responseCh
}

func (e *TxnExecutor) Stop() {
	e.stopCh <- struct{}{}
}

func (e *TxnExecutor) Run() {
	for {
		select {
		case req := <-e.reqCh:
			err := e.apply(req)
			if err != nil {
				req.responseCh <- &response{err: err}
			} else {
				req.responseCh <- &response{ts: req.ts}
			}
			close(req.responseCh)
		case <-e.stopCh:
			close(e.reqCh)
			close(e.stopCh)
			return
		}
	}
}

func (e *TxnExecutor) apply(req request) error {
	for key, val := range req.writeMap {
		err := e.mvcc.Set(key, req.ts, val)
		if err != nil {
			return err
		}
	}
	return nil
}
