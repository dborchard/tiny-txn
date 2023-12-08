package coordinator

// TsHeap
// https://pkg.go.dev/container/heap
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
