package wal

type Wal interface {
	Append([]byte) error

	StartCKPT() error
	EndCKPT() error

	SyncCache() error

	Recover() (uint64, error)

	Close() error
}
