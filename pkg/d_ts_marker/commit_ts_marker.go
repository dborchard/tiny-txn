package tsmarker

import "container/heap"

type CommitTsMarker struct {
	heap    elementHeap
	commits map[string]*Element
}

func NewCommit() CommitTsMarker {
	m := CommitTsMarker{
		heap:    []*Element{},
		commits: make(map[string]*Element),
	}
	heap.Init(&m.heap)
	return m
}

func (s *CommitTsMarker) Del(ts uint64) {
	for len(s.heap) > 0 && s.heap[0].Ts < ts {
		delete(s.commits, s.heap[0].Key)
		s.heap = s.heap[1:]
	}
}

func (s *CommitTsMarker) Get(key string) (*Element, bool) {
	if e, ok := s.commits[key]; ok {
		return e, true
	}
	return nil, false
}

func (s *CommitTsMarker) Add(k string, ts uint64) {
	if e, ok := s.commits[k]; ok {
		e.Ts = ts
	} else {
		e = &Element{Key: k, Ts: ts}

		s.commits[k] = e
		heap.Push(&s.heap, e)
	}
}
