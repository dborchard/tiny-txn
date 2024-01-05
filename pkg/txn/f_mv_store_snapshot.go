package txn

type Snapshot struct {
	ts      uint64
	mvStore *MvStore
}

func (snapshot *Snapshot) Get(key []byte) (Value, bool) {
	versionedKey := NewVersionedKey(key, snapshot.ts)
	return snapshot.mvStore.Get(versionedKey)
}

func (mvStore *MvStore) Snapshot(ts uint64) *Snapshot {
	mvStore.lock.RLock()
	defer mvStore.lock.RUnlock()

	return &Snapshot{
		ts: ts,
		mvStore: &MvStore{
			btree: mvStore.btree.Copy(),
		},
	}
}
