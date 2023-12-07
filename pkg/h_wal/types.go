package wal

type Writer interface {
	Append([]byte) error

	StartCKPT() error
	EndCKPT() error

	Close() error
}
