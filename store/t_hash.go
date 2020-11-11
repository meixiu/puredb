package store

import (
	"fmt"

	"github.com/spf13/cast"
)

type FVPair struct {
	Field []byte
	Value []byte
}

func (p FVPair) String() string {
	return fmt.Sprintf("%s=>%s", p.Field, p.Value)
}

func (s *Store) HLen(key []byte) (int64, error) {
	meta, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !meta.IsHash() {
		return 0, ErrMetaType
	}
	return meta.Size, nil
}

func (s *Store) HSet(key []byte, field []byte, value []byte) (int64, error) {
	b := s.batch.Begin()
	defer b.Release()
	meta, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !meta.IsHash() {
		return 0, ErrMetaType
	}
	dk := hashStore.DataKey(key, field)
	var n int64
	if has, _ := s.db.Has(dk); !has {
		n = 1
	}
	if n > 0 {
		meta.Size += n
		s.setMeta(b, key, meta)
	}
	b.Put(dk, value)
	err = b.Commit()
	return n, err
}

func (s *Store) HMSet(key []byte, args ...FVPair) error {
	b := s.batch.Begin()
	defer b.Release()
	meta, err := s.getMeta(key)
	if err != nil {
		return err
	}
	if !meta.IsHash() {
		return ErrMetaType
	}
	var n int64
	for _, v := range args {
		dk := hashStore.DataKey(key, v.Field)
		if has, _ := s.db.Has(dk); !has {
			n++
		}
		b.Put(dk, v.Value)
	}
	if n > 0 {
		meta.Size += n
		s.setMeta(b, key, meta)
	}
	err = b.Commit()
	return err
}

func (s *Store) HIncrBy(key []byte, field []byte, delta int64) (int64, error) {
	b := s.batch.Begin()
	defer b.Release()
	meta, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !meta.IsHash() {
		return 0, ErrMetaType
	}
	var n int64
	dk := hashStore.DataKey(key, field)
	v, err := s.db.Get(dk)
	if err != nil {
		return 0, err
	}
	// not exists
	if v == nil {
		n = 1
	}
	if n > 0 {
		meta.Size += n
		s.setMeta(b, key, meta)
	}
	total := cast.ToInt64(string(v)) + delta
	b.Put(dk, []byte(cast.ToString(total)))
	err = b.Commit()
	return total, err
}

func (s *Store) HGet(key []byte, field []byte) ([]byte, error) {
	dk := hashStore.DataKey(key, field)
	v, err := s.db.Get(dk)
	return v, err
}

func (s *Store) HMGet(key []byte, args ...[]byte) ([]FVPair, error) {
	list := make([]FVPair, 0)
	for _, field := range args {
		dk := hashStore.DataKey(key, field)
		v, err := s.db.Get(dk)
		if err != nil {
			return nil, err
		}
		list = append(list, FVPair{
			Field: field,
			Value: v,
		})
	}
	return list, nil
}

func (s *Store) HGetAll(key []byte) ([]FVPair, error) {
	dk := hashStore.DataKey(key, nil)
	it := s.db.NewPrefixIterator(dk, 0, -1)
	defer it.Close()
	list := make([]FVPair, 0)
	err := it.ForEach(func(key, value []byte) error {
		_, field, err := hashStore.ParseDataKey(key)
		if err != nil {
			return err
		}
		list = append(list, FVPair{
			Field: field,
			Value: value,
		})
		return nil
	})
	return list, err
}

func (s *Store) HDel(key []byte, args ...[]byte) (int64, error) {
	b := s.batch.Begin()
	defer b.Release()
	meta, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !meta.IsHash() {
		return 0, ErrMetaType
	}
	var n int64 = 0
	for _, field := range args {
		dk := hashStore.DataKey(key, field)
		if has, _ := s.db.Has(dk); !has {
			continue
		}
		n++
		b.Delete(dk)
	}
	if n > 0 {
		meta.Size -= n
		s.setMeta(b, key, meta)
	}
	err = b.Commit()
	return n, err
}

func (s *Store) hClear(b *Batch, key []byte) error {
	dk := hashStore.DataKey(key, nil)
	it := s.db.NewPrefixIterator(dk, 0, -1)
	defer it.Close()
	for ; it.Valid(); it.Next() {
		b.Delete(it.Key())
	}
	err := b.Commit()
	return err
}
