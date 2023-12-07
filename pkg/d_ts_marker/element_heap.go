package tsmarker

type elementHeap []*Element

func (h *elementHeap) Len() int           { return len(*h) }
func (h *elementHeap) Less(i, j int) bool { return (*h)[i].Ts < (*h)[j].Ts } // Min-heap based on Ts
func (h *elementHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *elementHeap) Push(x interface{}) {
	*h = append(*h, x.(*Element))
}

func (h *elementHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
