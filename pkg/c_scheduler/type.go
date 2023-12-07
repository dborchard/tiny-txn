package scheduler

type Scheduler interface {
	Run()          // starts the go routine
	Start() uint64 // starts the gc cycle + message processing
	Stop()         // stops the go routine, gc cycle, and message processing

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

type versionedKey struct {
	Key string
	Ts  uint64
}
