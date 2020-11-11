package store

import (
	"github.com/meixiu/puredb/driver"
)

const (
	DirForward  uint8 = 0
	DirBackward uint8 = 1
)

type RangeIterator struct {
	iter   driver.Iterator
	offset int
	limit  int
	count  int
	dir    uint8
}

func (it *RangeIterator) Close() {
	it.iter.Close()
}

func (it *RangeIterator) Valid() bool {
	if it.offset < 0 {
		return false
	}
	if !it.iter.Valid() {
		return false
	}
	if it.limit >= 0 && it.count >= it.limit {
		return false
	}
	return true
}

func (it *RangeIterator) Next() {
	it.count++
	if it.dir == DirForward {
		it.iter.Next()
	} else {
		it.iter.Prev()
	}
}

func (it *RangeIterator) Key() []byte {
	return it.iter.Key()
}

func (it *RangeIterator) Value() []byte {
	return it.iter.Value()
}

func (it *RangeIterator) ForEach(fn func(key, value []byte) error) error {
	for ; it.Valid(); it.Next() {
		err := fn(it.Key(), it.Value())
		if err != nil {
			return err
		}
	}
	return nil
}

func newRangeIterator(iter driver.Iterator, offset int, limit int, dir uint8) *RangeIterator {
	it := &RangeIterator{
		iter:   iter,
		offset: offset,
		limit:  limit,
		dir:    dir,
	}
	if dir == DirForward {
		it.iter.SeekToFirst()
	} else {
		it.iter.SeekToLast()
	}
	for i := 0; i < it.offset; i++ {
		if it.iter.Valid() {
			if it.dir == DirForward {
				it.iter.Next()
			} else {
				it.iter.Prev()
			}
		}
	}
	return it
}

func prefixRange(prefix []byte) ([]byte, []byte) {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return prefix, limit
}
