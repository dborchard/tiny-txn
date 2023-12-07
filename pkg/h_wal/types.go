package wal

type Wal interface {
	Append([]byte) error

	StartCKPT() error
	EndCKPT() error

	SyncCache() error

	Close() error
}
