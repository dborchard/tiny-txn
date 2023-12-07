package tsmarker

import "container/heap"

type BeginTsMarker struct {
	heap  elementHeap
	elMap map[uint64]*Element
}

func NewBegin() BeginTsMarker {
	m := BeginTsMarker{
		heap:  []*Element{},
		elMap: make(map[uint64]*Element),
	}
	heap.Init(&m.heap)
	return m
}

func (m *BeginTsMarker) Add(ts uint64) {
	if e, ok := m.elMap[ts]; ok {
		e.refCnt++
	} else {
		e = &Element{refCnt: 1, Ts: ts}
		m.elMap[ts] = e
		heap.Push(&m.heap, e)
	}
}

func (m *BeginTsMarker) Del(ts uint64) bool {
	if e, ok := m.elMap[ts]; ok {
		e.refCnt--
		if e.refCnt == 0 {
			if m.heap[0].Ts == ts {
				heap.Pop(&m.heap) // Pop the element if it's the minimum
			}
			delete(m.elMap, ts)
			return true
		}
	}
	return false
}
