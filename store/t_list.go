package store

import "math"

const (
	ListTypeLeft  uint8 = 0
	ListTypeRight uint8 = 1
	minIndex            = math.MinInt64 + 1
	maxIndex            = math.MaxInt64 - 1
)

func (s *Store) LLen(key []byte) (int64, error) {
	meta, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !meta.IsList() {
		return 0, ErrMetaType
	}
	return meta.Size, nil
}

func (s *Store) LPop(key []byte) ([]byte, error) {
	return s.lPopData(ListTypeLeft, key)
}

func (s *Store) RPop(key []byte) ([]byte, error) {
	return s.lPopData(ListTypeRight, key)
}

func (s Store) lPopData(t uint8, key []byte) ([]byte, error) {
	b := s.batch.Begin()
	defer b.Release()
	meta, err := s.getMeta(key)
	if err != nil {
		return nil, err
	}
	if !meta.IsList() {
		return nil, ErrMetaType
	}
	if meta.Empty() {
		return nil, nil
	}
	var dk []byte
	if t == ListTypeLeft {
		dk = listStore.DataKey(key, meta.LIndex)
		meta.LIndex++
	} else {
		meta.RIndex--
		dk = listStore.DataKey(key, meta.RIndex)
	}
	v, err := s.db.Get(dk)
	if err != nil {
		return nil, err
	}
	b.Delete(dk)
	// update meta
	meta.Size = meta.RIndex - meta.LIndex
	s.setMeta(b, key, meta)
	err = b.Commit()
	return v, err
}

func (s *Store) LPush(key []byte, args ...[]byte) (int64, error) {
	return s.lPushData(ListTypeLeft, key, args...)
}

func (s *Store) RPush(key []byte, args ...[]byte) (int64, error) {
	return s.lPushData(ListTypeRight, key, args...)
}

func (s *Store) lPushData(t uint8, key []byte, args ...[]byte) (int64, error) {
	b := s.batch.Begin()
	defer b.Release()
	meta, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !meta.IsList() {
		return 0, ErrMetaType
	}
	for _, v := range args {
		var dk []byte
		if t == ListTypeLeft {
			meta.LIndex--
			dk = listStore.DataKey(key, meta.LIndex)
		} else {
			dk = listStore.DataKey(key, meta.RIndex)
			meta.RIndex++
		}
		b.Put(dk, v)
	}
	// update meta
	meta.Size = meta.RIndex - meta.LIndex
	s.setMeta(b, key, meta)
	err = b.Commit()
	return meta.Size, err
}

func (s *Store) lClear(b *Batch, key []byte) error {
	start := listStore.DataKey(key, minIndex)
	end := listStore.DataKey(key, maxIndex)
	it := s.db.NewRangeIterator(start, end, 0, -1)
	defer it.Close()
	for ; it.Valid(); it.Next() {
		b.Delete(it.Key())
	}
	err := b.Commit()
	return err
}
