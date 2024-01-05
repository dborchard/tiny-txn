package txn

import (
	"context"
)

// Event represents either BeginEvent, DoneEvent or WaitForEvent.
type Event struct {
	ts     uint64
	done   bool
	waitCh chan struct{}
}

type CommitWaiter struct {
	eventCh chan Event
	stopCh  chan struct{}
	mHeap   *MarkerHeap
}

// NewCommitWaiter creates a new instance of CommitWaiter
func NewCommitWaiter() *CommitWaiter {
	waiter := &CommitWaiter{
		eventCh: make(chan Event),
		stopCh:  make(chan struct{}),
		mHeap:   NeMarkerHeap(),
	}
	go waiter.Run()
	return waiter
}

func (w *CommitWaiter) Begin(timestamp uint64) {
	w.eventCh <- Event{ts: timestamp, done: false}
}

func (w *CommitWaiter) Done(ts uint64) {
	w.eventCh <- Event{ts: ts, done: true}
}

func (w *CommitWaiter) WaitFor(
	ctx context.Context,
	timestamp uint64,
) error {
	if w.DoneTill() >= timestamp {
		return nil
	}
	waitCh := make(chan struct{})
	w.eventCh <- Event{ts: timestamp, waitCh: waitCh}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}

func (w *CommitWaiter) Stop() {
	w.stopCh <- struct{}{}
}

func (w *CommitWaiter) DoneTill() uint64 {
	return w.mHeap.GlobalDoneTill()
}

func (w *CommitWaiter) Run() {
	for {
		select {
		case event := <-w.eventCh:
			if event.waitCh != nil {
				w.processWaitEvent(event)
			} else {
				w.processBeginAndFinishEvent(event)
			}
		case <-w.stopCh:
			w.processClose()
			return
		}
	}
}
func (w *CommitWaiter) processWaitEvent(event Event) {
	doneTill := w.DoneTill()
	if doneTill >= event.ts {
		close(event.waitCh)
	} else {
		w.mHeap.AddWaiter(event.ts, event.waitCh)
	}
}

func (w *CommitWaiter) processBeginAndFinishEvent(event Event) {
	// 1. add eventTs to mHeap
	if event.done {
		w.mHeap.AddFinishEvent(event.ts)
	} else {
		w.mHeap.AddBeginEvent(event.ts)
	}

	// 2. recalculate globalDoneTill
	globalDoneTill := w.mHeap.RecalculateGlobalDoneTill()

	// 3. close waiters
	w.mHeap.CloseWaitersUntil(globalDoneTill)
}

func (w *CommitWaiter) processClose() {
	close(w.eventCh)
	close(w.stopCh)

	for timestamp, waiter := range w.mHeap.waiters {
		for _, channel := range waiter {
			close(channel)
		}
		delete(w.mHeap.waiters, timestamp)
	}
}
