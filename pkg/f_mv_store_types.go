package pkg

import "bytes"

type Pair[K any, V any] struct {
	Key K
	Val V
}

type VersionedKey struct {
	Key     []byte
	Version uint64
}

func NewVersionedKey(key []byte, version uint64) VersionedKey {
	return VersionedKey{Key: key, Version: version}
}

func (vk VersionedKey) Compare(other VersionedKey) int {
	if cmp := bytes.Compare(vk.Key, other.Key); cmp != 0 {
		return cmp
	}
	switch {
	case vk.Version < other.Version:
		return -1
	case vk.Version > other.Version:
		return 1
	default:
		return 0
	}
}

type Value struct {
	value []byte
}

func NewValue(value []byte) Value {
	return Value{
		value: value,
	}
}

func (value Value) Slice() []byte {
	return value.value
}
