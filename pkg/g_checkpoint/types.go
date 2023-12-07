package checkpoint

type Checkpoint interface {
	Start() error
	End(ts uint64) error
}
