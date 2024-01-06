package txn

import (
	"context"
)

type EventTyp int

const (
	BeginEvent EventTyp = iota
	DoneEvent
	WaitForEvent
)

// Event represents either BeginEvent, DoneEvent or WaitForEvent.
type Event struct {
	typ    EventTyp
	ts     uint64
	waitCh chan struct{}
}

type TsWaiter struct {
	eventCh chan Event
	stopCh  chan struct{}
	txnHeap *TransactionHeap
}

func NewTsWaiter() *TsWaiter {
	waiter := &TsWaiter{
		eventCh: make(chan Event),
		stopCh:  make(chan struct{}),
		txnHeap: NewTransactionHeap(),
	}
	go waiter.Run()
	return waiter
}

func (w *TsWaiter) Begin(timestamp uint64) {
	w.eventCh <- Event{typ: BeginEvent, ts: timestamp}
}

func (w *TsWaiter) Done(ts uint64) {
	w.eventCh <- Event{typ: DoneEvent, ts: ts}
}

func (w *TsWaiter) WaitFor(ctx context.Context, ts uint64) error {
	if w.DoneTill() >= ts {
		return nil
	}

	waitCh := make(chan struct{})
	w.eventCh <- Event{typ: WaitForEvent, ts: ts, waitCh: waitCh}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}

func (w *TsWaiter) Stop() {
	w.stopCh <- struct{}{}
}

func (w *TsWaiter) DoneTill() uint64 {
	return w.txnHeap.GlobalDoneTill()
}

func (w *TsWaiter) Run() {
	for {
		select {
		case event := <-w.eventCh:
			switch event.typ {
			case BeginEvent:
				w.txnHeap.AddBeginEvent(event.ts)
				globalDoneTill := w.txnHeap.RecalculateGlobalDoneTill()
				w.txnHeap.CloseWaitersUntil(globalDoneTill)
			case DoneEvent:
				w.txnHeap.AddDoneEvent(event.ts)
				globalDoneTill := w.txnHeap.RecalculateGlobalDoneTill()
				w.txnHeap.CloseWaitersUntil(globalDoneTill)
			case WaitForEvent:
				w.processWaitEvent(event)
			default:
				panic("unknown event type")
			}
		case <-w.stopCh:
			w.processClose()
			return
		}
	}
}
func (w *TsWaiter) processWaitEvent(event Event) {
	doneTill := w.DoneTill()
	if doneTill >= event.ts {
		close(event.waitCh)
	} else {
		w.txnHeap.AddWaiter(event.ts, event.waitCh)
	}
}

func (w *TsWaiter) processClose() {
	close(w.eventCh)
	close(w.stopCh)

	for timestamp, waiter := range w.txnHeap.waiters {
		for _, channel := range waiter {
			close(channel)
		}
		delete(w.txnHeap.waiters, timestamp)
	}
}
