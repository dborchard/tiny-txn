package mvcc

import "sync"

type LocalClock struct {
	*sync.Mutex
	Ts uint64
}

var _ Clock = (*LocalClock)(nil)

func (clk LocalClock) Now() uint64 {
	defer func() {
		clk.Lock()
		clk.Ts = clk.Ts + 1
		clk.Unlock()
	}()
	return clk.Ts
}
