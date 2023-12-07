package checkpoint

import (
	"time"
	cache "tiny_txn/pkg/f_cache"
	wal "tiny_txn/pkg/h_wal"
)

type CheckPointer interface {
	Start() error
	End(ts uint64) error
}

type DiskCheckPointer struct {
	Started bool // start check point
	Ts      time.Time

	Wal   wal.Wal
	Cache cache.Cache
}

func (d *DiskCheckPointer) Start() error {
	panic("implement me")
}

func (d *DiskCheckPointer) End(ts uint64) error {
	panic("implement me")
}
