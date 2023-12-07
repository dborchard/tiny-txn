package tsmarker

type Element struct {
	refCnt int // reference count
	Key    string
	Ts     uint64
}
