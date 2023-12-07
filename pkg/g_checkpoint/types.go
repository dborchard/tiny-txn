package checkpoint

type CheckPointer interface {
	Start() error
	End(ts uint64) error
}
