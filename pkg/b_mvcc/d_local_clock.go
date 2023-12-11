package mvcc

import "sync"

type LocalClock struct {
	*sync.Mutex
	nextTs uint64
}

var _ Clock = (*LocalClock)(nil)

func (h LocalClock) Now() uint64 {
	defer func() {
		h.Lock()
		h.nextTs = h.nextTs + 1
		h.Unlock()
	}()
	return h.nextTs
}
