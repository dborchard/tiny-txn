package mvcc

type MVCC interface {
	Put(key string, ts uint64, val []byte) error
	Get(key string, ts uint64) ([]byte, uint64, error)
	Del(key string, ts uint64) error

	Close()
}
