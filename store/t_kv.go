package store

import (
	"fmt"

	"github.com/spf13/cast"
)

type KVPair struct {
	Key   []byte
	Value []byte
}

func (p KVPair) String() string {
	return fmt.Sprintf("%s=>%s", p.Key, p.Value)
}

// StrLen returns the length of the data.
func (s *Store) StrLen(key []byte) (int64, error) {
	v, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, err
	}
	return int64(len(v)), nil
}

func (s *Store) Get(key []byte) ([]byte, error) {
	return s.db.Get(kvStore.DataKey(key))
}

func (s *Store) MGet(keys ...[]byte) ([]KVPair, error) {
	list := make([]KVPair, 0)
	for _, key := range keys {
		value, err := s.db.Get(kvStore.DataKey(key))
		if err != nil {
			return nil, err
		}
		list = append(list, KVPair{
			Key:   key,
			Value: value,
		})
	}
	return list, nil
}

// Incr increases the data.
func (s *Store) Incr(key []byte) (int64, error) {
	return s.IncrBy(key, 1)
}

func (s *Store) IncrBy(key []byte, delta int64) (int64, error) {
	return s.incr(key, delta)
}

func (s *Store) incr(key []byte, delta int64) (int64, error) {
	b := s.batch.Begin()
	defer b.Release()
	meta, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !meta.IsString() {
		return 0, ErrMetaType
	}
	dk := kvStore.DataKey(key)
	v, err := s.db.Get(dk)
	if err != nil {
		return 0, err
	}
	n := cast.ToInt64(string(v)) + delta
	b.Put(dk, []byte(cast.ToString(n)))
	// add meta
	s.setMeta(b, key, meta)
	err = b.Commit()
	return n, err
}

func (s *Store) Set(key []byte, value []byte) error {
	b := s.batch.Begin()
	defer b.Release()
	meta, err := s.getMeta(key)
	if err != nil {
		return err
	}
	if !meta.IsString() {
		return ErrMetaType
	}
	dk := kvStore.DataKey(key)
	b.Put(dk, value)
	// add meta
	s.setMeta(b, key, meta)
	err = b.Commit()
	return err
}

func (s *Store) MSet(args ...KVPair) error {
	b := s.batch.Begin()
	defer b.Release()
	for _, v := range args {
		meta, err := s.getMeta(v.Key)
		if err != nil {
			return err
		}
		if !meta.IsString() {
			return ErrMetaType
		}
		b.Put(kvStore.DataKey(v.Key), v.Value)
		s.setMeta(b, v.Key, meta)
	}
	err := b.Commit()
	return err
}

func (s *Store) kvClear(b *Batch, key []byte) error {
	dk := kvStore.DataKey(key)
	b.Delete(dk)
	return nil
}
