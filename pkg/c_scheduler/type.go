package scheduler

type Scheduler interface {
	Run()
	Stop()

	Start() uint64

	Commit(uint64, map[string]uint64, map[string][]byte) (uint64, error)
	Done(uint64) error
}

type message struct {
	typ      messageType
	ts       uint64
	resCh    chan *result
	readMap  map[string]uint64 // key -> ts
	writeMap map[string][]byte // key -> value
}

type result struct {
	err error
	ts  uint64
}

type messageType int

const (
	Commit messageType = iota // commit
	Done                      // done
	Start                     // start
)
