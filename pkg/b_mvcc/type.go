package mvcc

type Transaction interface {
	Set(key string, val []byte) error
	Get(key string) (val []byte, err error)
	Del(key string) error

	Commit() error
	Rollback() error
}

type Manager interface {
	Close()

	Begin() uint64
	MayCommit(txn *Txn) (uint64, error)
}

type requestType int

const (
	Commit requestType = iota // commit
	Done                      // done
	Start                     // start
)

type request struct {
	typ        requestType
	ts         uint64
	responseCh chan *response
	reads      map[string]uint64 // key -> ts
	writeMap   map[string][]byte // key -> value
}

type response struct {
	err error
	ts  uint64
}

type committedTxn struct {
	keys []string
	ts   uint64
}
