package txn

import (
	"container/heap"
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

type SHeap struct {
	doneTillTs       atomic.Uint64
	tsHeap           TsHeap                     // min tsHeap of txn timestamps
	pendingTxnCounts map[uint64]int             // ts -> txn count
	waiters          map[uint64][]chan struct{} // ts -> waitChs
}

func NewSHeap() *SHeap {
	var tsHeap TsHeap
	heap.Init(&tsHeap)

	res := SHeap{
		tsHeap:           tsHeap,
		pendingTxnCounts: make(map[uint64]int),
		waiters:          make(map[uint64][]chan struct{}),
	}

	return &res
}

func (h *SHeap) AddBeginEvent(ts uint64) {
	if _, ok := h.pendingTxnCounts[ts]; !ok {
		heap.Push(&h.tsHeap, ts)
	}
	h.pendingTxnCounts[ts] += 1
}

func (h *SHeap) AddDoneEvent(ts uint64) {
	if _, ok := h.pendingTxnCounts[ts]; !ok {
		heap.Push(&h.tsHeap, ts)
	}
	h.pendingTxnCounts[ts] += -1
}

func (h *SHeap) AddWaiter(ts uint64, ch chan struct{}) {
	if _, ok := h.waiters[ts]; !ok {
		h.waiters[ts] = []chan struct{}{ch}
	} else {
		h.waiters[ts] = append(h.waiters[ts], ch)
	}
}

func (h *SHeap) CloseWaitersUntil(utilTs uint64) {
	for ts, waiter := range h.waiters {
		if ts <= utilTs {
			for _, channel := range waiter {
				close(channel)
			}
			delete(h.waiters, ts)
		}
	}
}

func (h *SHeap) GlobalDoneTill() uint64 {
	return h.doneTillTs.Load()
}

func (h *SHeap) RecalculateGlobalDoneTill() uint64 {
	doneTill := h.GlobalDoneTill()
	globalDoneTill := doneTill
	for len(h.tsHeap) > 0 {
		localDoneTill := h.tsHeap[0]
		if pendingCount := h.pendingTxnCounts[localDoneTill]; pendingCount > 0 {
			break
		}

		// update sHeap & pendingTxnCounts
		heap.Pop(&h.tsHeap)
		delete(h.pendingTxnCounts, localDoneTill)

		// update globalDoneTill
		globalDoneTill = localDoneTill
	}

	if globalDoneTill != doneTill {
		h.doneTillTs.CompareAndSwap(doneTill, globalDoneTill)
	}

	return h.doneTillTs.Load()
}
