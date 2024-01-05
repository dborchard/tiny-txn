package pkg

import (
	"github.com/tidwall/btree"
	"sync"
)

type MvStore struct {
	lock  sync.RWMutex
	btree *btree.BTreeG[Pair[VersionedKey, Value]]
}

func NewMVStore() *MvStore {
	return &MvStore{
		btree: btree.NewBTreeG(func(a Pair[VersionedKey, Value], b Pair[VersionedKey, Value]) bool {
			return a.Key.Compare(b.Key) < 0
		}),
	}
}

func (mvStore *MvStore) PutOrUpdate(key VersionedKey, value Value) {
	mvStore.lock.Lock()
	defer mvStore.lock.Unlock()

	kv := Pair[VersionedKey, Value]{
		Key: key,
		Val: value,
	}
	mvStore.btree.Set(kv)
}

func (mvStore *MvStore) Get(key VersionedKey) (Value, bool) {
	mvStore.lock.RLock()
	defer mvStore.lock.RUnlock()

	kv := Pair[VersionedKey, Value]{
		Key: key,
		Val: Value{},
	}
	res, present := mvStore.btree.Get(kv)
	return res.Val, present
}
