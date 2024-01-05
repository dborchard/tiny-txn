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

type Waiter struct {
	eventCh chan Event
	stopCh  chan struct{}
	sHeap   *SHeap
}

func NewWaiter() *Waiter {
	waiter := &Waiter{
		eventCh: make(chan Event),
		stopCh:  make(chan struct{}),
		sHeap:   NewSHeap(),
	}
	go waiter.Run()
	return waiter
}

func (w *Waiter) Begin(timestamp uint64) {
	w.eventCh <- Event{typ: BeginEvent, ts: timestamp}
}

func (w *Waiter) Done(ts uint64) {
	w.eventCh <- Event{typ: DoneEvent, ts: ts}
}

func (w *Waiter) WaitFor(ctx context.Context, ts uint64) error {
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

func (w *Waiter) Stop() {
	w.stopCh <- struct{}{}
}

func (w *Waiter) DoneTill() uint64 {
	return w.sHeap.GlobalDoneTill()
}

func (w *Waiter) Run() {
	for {
		select {
		case event := <-w.eventCh:
			switch event.typ {
			case BeginEvent:
				w.sHeap.AddBeginEvent(event.ts)
				globalDoneTill := w.sHeap.RecalculateGlobalDoneTill()
				w.sHeap.CloseWaitersUntil(globalDoneTill)
			case DoneEvent:
				w.sHeap.AddDoneEvent(event.ts)
				globalDoneTill := w.sHeap.RecalculateGlobalDoneTill()
				w.sHeap.CloseWaitersUntil(globalDoneTill)
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
func (w *Waiter) processWaitEvent(event Event) {
	doneTill := w.DoneTill()
	if doneTill >= event.ts {
		close(event.waitCh)
	} else {
		w.sHeap.AddWaiter(event.ts, event.waitCh)
	}
}

func (w *Waiter) processClose() {
	close(w.eventCh)
	close(w.stopCh)

	for timestamp, waiter := range w.sHeap.waiters {
		for _, channel := range waiter {
			close(channel)
		}
		delete(w.sHeap.waiters, timestamp)
	}
}
