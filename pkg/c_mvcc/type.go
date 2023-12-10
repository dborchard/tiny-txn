package mvcc

type MVCC interface {
	NewTransaction() Transaction
	GetLatestCommitTS() uint64
	Close() error
}

type Transaction interface {
	Set(key string, val []byte) error
	Get(key string) (val []byte, err error)
	Del(key string) error

	ApplySnapshot(data []byte) error
	Snapshot() ([]byte, error)

	Commit() error
	Rollback() error
}

type Scheduler interface {
	Run() // starts the go routine
	Stop()

	Begin() uint64
	Commit(uint64, map[string]uint64, map[string][]byte) (uint64, error)
	Done(uint64) error
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
