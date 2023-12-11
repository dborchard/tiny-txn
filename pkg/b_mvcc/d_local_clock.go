package mvcc

import "sync"

type LocalClock struct {
	*sync.Mutex
	nextTs uint64
}

var _ Clock = (*LocalClock)(nil)

func (clk LocalClock) Now() uint64 {
	defer func() {
		clk.Lock()
		clk.nextTs = clk.nextTs + 1
		clk.Unlock()
	}()
	return clk.nextTs
}
