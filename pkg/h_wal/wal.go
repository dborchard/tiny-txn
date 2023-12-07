package wal

var _ Wal = new(DistWal)

type DistWal struct {
}

func New() Wal {
	//TODO implement me
	panic("implement me")
}

func (d *DistWal) Append(bytes []byte) error {
	//TODO implement me
	panic("implement me")
}

func (d *DistWal) StartCKPT() error {
	//TODO implement me
	panic("implement me")
}

func (d *DistWal) EndCKPT() error {
	//TODO implement me
	panic("implement me")
}

func (d *DistWal) SyncCache() error {
	//TODO implement me
	panic("implement me")
}

func (d *DistWal) Recover() (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (d *DistWal) Close() error {
	//TODO implement me
	panic("implement me")
}
