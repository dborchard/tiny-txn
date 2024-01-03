package pkg

import (
	"container/heap"
	"context"
	"sync/atomic"
)

type TsHeap []uint64

func (h *TsHeap) Len() int           { return len(*h) }
func (h *TsHeap) Less(i, j int) bool { return (*h)[i] < (*h)[j] }
func (h *TsHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }
func (h *TsHeap) Push(x any)         { *h = append(*h, x.(uint64)) }
func (h *TsHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Event represents a BeginTsEvent, FinishTsEvent or a WaitTsEvent
type Event struct {
	ts     uint64
	done   bool
	waitCh chan struct{}
}

type Waiter struct {
	Name     string
	eventCh  chan Event    // channel for incoming messages
	stopCh   chan struct{} // channel for stopping the waiter
	doneTill atomic.Uint64 // max ts of finished txns

	tsHeap          TsHeap                     // min heap of txn timestamps
	pendingTxnsAtTs map[uint64]int             // ts -> txn count
	waitersAtTs     map[uint64][]chan struct{} // ts -> waitChs
}

func NewWaiter(name string) *Waiter {
	w := &Waiter{
		Name:    name,
		eventCh: make(chan Event),
		stopCh:  make(chan struct{}),

		pendingTxnsAtTs: make(map[uint64]int),
		tsHeap:          make(TsHeap, 0),
		waitersAtTs:     make(map[uint64][]chan struct{}),
	}
	heap.Init(&w.tsHeap)

	go w.Run()
	return w
}

func (w *Waiter) Begin(ts uint64) {
	w.eventCh <- Event{ts: ts, done: false}
}

func (w *Waiter) WaitForMark(ctx context.Context, ts uint64) error {
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

func (w *Waiter) Finish(ts uint64) {
	w.eventCh <- Event{ts: ts, done: true}
}

func (w *Waiter) Stop() {
	w.stopCh <- struct{}{}
}

func (w *Waiter) Run() {
	for {
		select {
		case event := <-w.eventCh:
			if event.waitCh != nil {
				w.processWaitEvent(event)
			} else {
				w.processTsEvent(event)
			}
		case <-w.stopCh:
			w.processClose()
			return
		}
	}
}

func (w *Waiter) processWaitEvent(event Event) {
	doneTill := w.doneTill.Load()
	if doneTill >= event.ts {
		close(event.waitCh)
	} else {
		if _, ok := w.waitersAtTs[event.ts]; !ok {
			w.waitersAtTs[event.ts] = []chan struct{}{event.waitCh}
		} else {
			w.waitersAtTs[event.ts] = append(w.waitersAtTs[event.ts], event.waitCh)
		}
	}
}

func (w *Waiter) processTsEvent(event Event) {
	{ // 1. update pendingTxnsAtTs & tsHeap
		_, ok := w.pendingTxnsAtTs[event.ts]
		if !ok {
			heap.Push(&w.tsHeap, event.ts)
		}

		delta := 1
		if event.done {
			delta = -1
		}
		w.pendingTxnsAtTs[event.ts] += delta
	}

	// 2. recalculate globalDoneTill
	doneTill := w.DoneTill()
	globalDoneTill := doneTill
	for len(w.tsHeap) > 0 {
		localDoneTill := w.tsHeap[0]
		if pendingCount := w.pendingTxnsAtTs[localDoneTill]; pendingCount > 0 {
			break
		}

		// update tsHeap & pendingTxnsAtTs
		heap.Pop(&w.tsHeap)
		delete(w.pendingTxnsAtTs, localDoneTill)

		// update globalDoneTill
		globalDoneTill = localDoneTill
	}

	if globalDoneTill != doneTill {
		w.doneTill.CompareAndSwap(doneTill, globalDoneTill)
	}

	// 3. close waitersAtTs
	for ts, waiter := range w.waitersAtTs {
		if ts <= globalDoneTill {
			for _, channel := range waiter {
				close(channel)
			}
			delete(w.waitersAtTs, ts)
		}
	}
}
func (w *Waiter) DoneTill() uint64 {
	return w.doneTill.Load()
}

func (w *Waiter) processClose() {
	close(w.eventCh)
	close(w.stopCh)

	for timestamp, waiter := range w.waitersAtTs {
		for _, channel := range waiter {
			close(channel)
		}
		delete(w.waitersAtTs, timestamp)
	}
}
