package mvcc

import "sync"

type HlcClock struct {
	*sync.Mutex
	nextTs uint64
}

var _ Clock = (*HlcClock)(nil)

func (h HlcClock) Now() uint64 {
	h.Lock()
	defer h.Unlock()

	beginTs := h.nextTs - 1
	return beginTs
}
