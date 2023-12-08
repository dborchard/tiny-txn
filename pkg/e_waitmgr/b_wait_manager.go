package coordinator

import (
	"container/heap"
	"context"
)

func NewTxnSyncManager(name string) *WaitMgr {
	w := &WaitMgr{
		Name:    name,
		eventCh: make(chan Event),
		stopCh:  make(chan struct{}),
	}

	heap.Init(&w.tsHeap)

	w.pendingCounts = make(map[uint64]int)
	w.waiters = make(map[uint64][]chan struct{})

	go w.Run()
	return w
}

func (w *WaitMgr) Begin(ts uint64) {
	w.eventCh <- Event{ts: ts, done: false}
}

func (w *WaitMgr) Finish(ts uint64) {
	w.eventCh <- Event{ts: ts, done: true}
}

func (w *WaitMgr) Stop() {
	w.stopCh <- struct{}{}
}

func (w *WaitMgr) Run() {
	for {
		select {
		case event := <-w.eventCh:
			if event.waitCh != nil {
				w.processWait(event)
			} else {
				w.processBeginFinish(event)
			}
		case <-w.stopCh:
			w.closeAll()
			return
		}
	}
}

func (w *WaitMgr) processWait(event Event) {
	doneTill := w.doneTill.Load()
	if doneTill >= event.ts {
		close(event.waitCh)
	} else {
		if _, ok := w.waiters[event.ts]; !ok {
			w.waiters[event.ts] = []chan struct{}{event.waitCh}
		} else {
			w.waiters[event.ts] = append(w.waiters[event.ts], event.waitCh)
		}
	}
}

func (w *WaitMgr) processBeginFinish(event Event) {
	{ // update pendingCounts & tsHeap
		_, ok := w.pendingCounts[event.ts]
		if !ok {
			heap.Push(&w.tsHeap, event.ts)
		}

		delta := 1
		if event.done {
			delta = -1
		}
		w.pendingCounts[event.ts] += delta
	}

	doneTill := w.DoneTill()
	globalDoneTill := doneTill
	for len(w.tsHeap) > 0 {
		localDoneTill := w.tsHeap[0]
		if done := w.pendingCounts[localDoneTill]; done > 0 {
			break
		}

		// update tsHeap & pendingCounts
		heap.Pop(&w.tsHeap)
		delete(w.pendingCounts, localDoneTill)

		// update globalDoneTill
		globalDoneTill = localDoneTill
	}

	if globalDoneTill != doneTill {
		w.doneTill.CompareAndSwap(doneTill, globalDoneTill)
	}

	for ts, waiter := range w.waiters {
		if ts <= globalDoneTill {
			for _, channel := range waiter {
				close(channel)
			}
			delete(w.waiters, ts)
		}
	}
}
func (w *WaitMgr) DoneTill() uint64 {
	return w.doneTill.Load()
}

func (w *WaitMgr) WaitForMark(ctx context.Context, ts uint64) error {
	if w.DoneTill() >= ts {
		return nil
	}

	waitCh := make(chan struct{})
	w.eventCh <- Event{ts: ts, waitCh: waitCh}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}

func (w *WaitMgr) closeAll() {
	close(w.eventCh)
	close(w.stopCh)

	for timestamp, waiter := range w.waiters {
		for _, channel := range waiter {
			close(channel)
		}
		delete(w.waiters, timestamp)
	}
}
