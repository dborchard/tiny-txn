package mvcc

var _ MVCC = new(BTreeMvcc)

type BTreeMvcc struct {
}

func New() MVCC {
	//TODO implement me
	panic("implement me")
}

func (b *BTreeMvcc) Get(key string, ts uint64) ([]byte, uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BTreeMvcc) Set(key string, ts uint64, val []byte) error {
	//TODO implement me
	panic("implement me")
}

func (b *BTreeMvcc) Del(key string, ts uint64) error {
	//TODO implement me
	panic("implement me")
}

func (b *BTreeMvcc) Close() {
	//TODO implement me
	panic("implement me")
}
