package client

type Client interface {
	NewTransaction() Transaction
	GetLatestCommitTS() uint64
	Close() error
}

type Transaction interface {
	Set(key string, val []byte) error
	Get(key string) (val []byte, err error)
	Del(key string) error

	Commit() error
	Rollback() error
}
