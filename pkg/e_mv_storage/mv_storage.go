package mvstorage

var _ MvStorage = new(BTreeMvStorage)

type BTreeMvStorage struct {
}

func New() MvStorage {
	//TODO implement me
	panic("implement me")
}

func (b *BTreeMvStorage) Get(key string, ts uint64) ([]byte, uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BTreeMvStorage) Set(key string, ts uint64, val []byte) error {
	//TODO implement me
	panic("implement me")
}

func (b *BTreeMvStorage) Del(key string, ts uint64) error {
	//TODO implement me
	panic("implement me")
}

func (b *BTreeMvStorage) Close() {
	//TODO implement me
	panic("implement me")
}
