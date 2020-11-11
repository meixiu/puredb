package driver

import (
	"bytes"
)

const (
	DirForward  uint8 = 0
	DirBackward uint8 = 1
)

const (
	RangeClose uint8 = 0x00
	RangeLOpen uint8 = 0x01
	RangeROpen uint8 = 0x10
	RangeOpen  uint8 = 0x11
)

// Range min must less or equal than max
//
// range type:
//
//  close: [min, max]
//  open: (min, max)
//  lopen: (min, max]
//  ropen: [min, max)
type Range struct {
	Start []byte
	End   []byte
	Type  uint8
}

type Limit struct {
	Offset int
	Num    int
}

type RangeIterator struct {
	r    *Range
	l    *Limit
	iter Iterator
	step int
	dir  uint8
}

func (it *RangeIterator) Close() {
	it.iter.Close()
}

func (it *RangeIterator) Valid() bool {
	if it.l.Offset < 0 {
		return false
	}
	if !it.iter.Valid() {
		return false
	}
	if it.l.Num >= 0 && it.step >= it.l.Num {
		return false
	}
	switch it.dir {
	case DirForward:
		if it.r.End != nil {
			r := bytes.Compare(it.iter.Key(), it.r.End)
			if it.r.Type&RangeROpen > 0 {
				return !(r >= 0)
			} else {
				return !(r > 0)
			}
		}
	case DirBackward:
		if it.r.Start != nil {
			r := bytes.Compare(it.iter.Key(), it.r.Start)
			if it.r.Type&RangeLOpen > 0 {
				return !(r <= 0)
			} else {
				return !(r < 0)
			}
		}
	}
	return true
}

func (it *RangeIterator) Next() {
	it.step++
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

func (it *RangeIterator) ForEach(fn func(key, value []byte)) {
	for ; it.Valid(); it.Next() {
		fn(it.Key(), it.Value())
	}
}

func NewRangeIterator(iter Iterator, r *Range, l *Limit) *RangeIterator {
	return newRangeIterator(iter, r, l, DirForward)
}

func NewRevRangeIterator(iter Iterator, r *Range, l *Limit) *RangeIterator {
	return newRangeIterator(iter, r, l, DirBackward)
}

func newRangeIterator(iter Iterator, r *Range, l *Limit, dir uint8) *RangeIterator {
	if r == nil {
		r = &Range{}
	}
	if l == nil {
		l = &Limit{0, -1}
	}
	it := &RangeIterator{
		r:    r,
		l:    l,
		iter: iter,
		dir:  dir,
	}
	switch dir {
	case DirForward:
		if r.Start == nil {
			it.iter.SeekToFirst()
		} else {
			it.iter.SeekTo(r.Start)
			if r.Type&RangeLOpen > 0 {
				if it.iter.Valid() && bytes.Equal(it.iter.Key(), r.Start) {
					it.iter.Next()
				}
			}
		}
	case DirBackward:
		if r.End == nil {
			it.iter.SeekToLast()
		} else {
			it.iter.SeekTo(r.End)
			if !it.iter.Valid() {
				it.iter.SeekToLast()
			} else {
				if !bytes.Equal(it.iter.Key(), r.End) {
					it.iter.Prev()
				}
			}
			if r.Type&RangeROpen > 0 {
				if it.iter.Valid() && bytes.Equal(it.iter.Key(), r.End) {
					it.iter.Prev()
				}
			}
		}
	}
	for i := 0; i < l.Offset; i++ {
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
