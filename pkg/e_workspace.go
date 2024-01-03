package pkg

type MV struct {
}

func (mvcc *MV) Put(key string, ts uint64, value []byte) {
}

func (mvcc *MV) Get(key string, ts uint64) ([]byte, uint64, error) {
	return nil, 0, nil
}

type Workspace struct {
	memtable *MV
}

func (w *Workspace) put(batch map[string][]byte, ts uint64) {
	for k, v := range batch {
		w.memtable.Put(k, ts, v)
	}
}

func (w *Workspace) get(key string, ts uint64) ([]byte, uint64, error) {
	return w.memtable.Get(key, ts)
}
