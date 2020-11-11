package store

import (
	"errors"
	"fmt"
	"math"
)

const (
	MinScore = math.MinInt64 + 1
	MaxScore = math.MaxInt64 - 1
)

var (
	ErrMemberNotExists = errors.New("zset: member not exists")
)

type ScorePair struct {
	Score  int64
	Member []byte
}

func (p ScorePair) String() string {
	return fmt.Sprintf("%d=>%s", p.Score, p.Member)
}

func (p *ScorePair) MemberString() string {
	return string(p.Member)
}

type ZSetRange struct {
	Min    int64
	Max    int64
	Offset int
	Limit  int
}

func (r *ZSetRange) MinKey(key []byte) []byte {
	mk := zsetStore.IndexKey(key, r.Min, nil)
	return mk
}

func (r *ZSetRange) MaxKey(key []byte) []byte {
	mk := zsetStore.IndexKey(key, r.Max+1, nil)
	return mk
}

func (s *Store) ZRank(key []byte, member []byte) (int64, error) {
	return s.zMemberRank(key, member, false)
}

func (s *Store) ZRevRank(key []byte, member []byte) (int64, error) {
	return s.zMemberRank(key, member, true)
}

func (s *Store) zMemberRank(key []byte, member []byte, rev bool) (int64, error) {
	score, err := s.zMemberScore(key, member)
	if err != nil {
		return -1, err
	}
	r := &ZSetRange{
		Min: MinScore,
		Max: MaxScore,
	}
	ik := zsetStore.IndexKey(key, score, member)
	var it *RangeIterator
	if !rev {
		it = s.db.NewRangeIterator(r.MinKey(key), ik, 0, -1)
	} else {
		// 排除自身
		_, next := prefixRange(ik)
		it = s.db.NewRevRangeIterator(next, r.MaxKey(key), 0, -1)
	}
	defer it.Close()
	var n int64
	for ; it.Valid(); it.Next() {
		n++
	}
	return n, nil
}

func (s *Store) ZRange(key []byte, start int64, stop int64) ([]ScorePair, error) {
	offset, limit := s.zParseLimit(key, start, stop)
	r := &ZSetRange{
		Min:    MinScore,
		Max:    MaxScore,
		Offset: offset,
		Limit:  limit,
	}
	return s.zRangeList(key, r, false)
}

func (s *Store) ZRevRange(key []byte, start int64, stop int64) ([]ScorePair, error) {
	offset, limit := s.zParseLimit(key, start, stop)
	r := &ZSetRange{
		Min:    MinScore,
		Max:    MaxScore,
		Offset: offset,
		Limit:  limit,
	}
	return s.zRangeList(key, r, true)
}

func (s *Store) ZRangeByScore(key []byte, min int64, max int64, offset int, limit int) ([]ScorePair, error) {
	r := &ZSetRange{
		Min:    min,
		Max:    max,
		Offset: offset,
		Limit:  limit,
	}
	return s.zRangeList(key, r, false)
}

func (s *Store) ZRevRangeByScore(key []byte, min int64, max int64, offset int, limit int) ([]ScorePair, error) {
	r := &ZSetRange{
		Min:    min,
		Max:    max,
		Offset: offset,
		Limit:  limit,
	}
	return s.zRangeList(key, r, true)
}

func (s *Store) zParseLimit(key []byte, start int64, stop int64) (int, int) {
	size, _ := s.ZCard(key)
	if start < 0 {
		start = size + start
	}
	if stop < 0 {
		stop = size + stop
	}
	return int(start), int(stop - start + 1)
}

func (s *Store) zRangeList(key []byte, r *ZSetRange, rev bool) ([]ScorePair, error) {
	var it *RangeIterator
	if rev {
		it = s.db.NewRevRangeIterator(r.MinKey(key), r.MaxKey(key), r.Offset, r.Limit)
	} else {
		it = s.db.NewRangeIterator(r.MinKey(key), r.MaxKey(key), r.Offset, r.Limit)
	}
	defer it.Close()
	list := make([]ScorePair, 0)
	err := it.ForEach(func(key, value []byte) error {
		score, member, err := zsetStore.ParseIndexKey(key)
		if err != nil {
			return err
		}
		list = append(list, ScorePair{
			Score:  score,
			Member: member,
		})
		return nil
	})
	return list, err
}

func (s *Store) ZScore(key []byte, member []byte) (int64, error) {
	return s.zMemberScore(key, member)
}

func (s *Store) zMemberScore(key []byte, member []byte) (int64, error) {
	dk := zsetStore.DataKey(key, member)
	p, err := s.db.Get(dk)
	if err != nil {
		return 0, err
	}
	if p == nil {
		return 0, ErrMemberNotExists
	}
	score, err := NewBuffer(p).ReadInt64()
	return score, err
}

func (s *Store) ZCount(key []byte, min int64, max int64) (int64, error) {
	r := &ZSetRange{
		Min: min,
		Max: max,
	}
	it := s.db.NewRangeIterator(r.MinKey(key), r.MaxKey(key), 0, -1)
	defer it.Close()
	var n int64
	for ; it.Valid(); it.Next() {
		n++
	}
	return n, nil
}

func (s *Store) ZCard(key []byte) (int64, error) {
	meta, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !meta.IsZset() {
		return 0, ErrMetaType
	}
	return meta.Size, nil
}

func (s *Store) ZAdd(key []byte, args ...ScorePair) (int64, error) {
	b := s.batch.Begin()
	defer b.Release()
	meta, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !meta.IsZset() {
		return 0, ErrMetaType
	}
	var n int64
	var l = make(map[string]bool)
	for _, v := range args {
		if _, has := l[v.MemberString()]; has {
			continue
		}
		l[v.MemberString()] = true
		exists, err := s.zSetItem(b, key, v.Score, v.Member)
		if err != nil {
			return 0, err
		}
		if !exists {
			n++
		}
	}
	if n > 0 {
		meta.Size += n
		s.setMeta(b, key, meta)
	}
	err = b.Commit()
	return n, err
}

func (s *Store) zSetItem(b *Batch, key []byte, score int64, member []byte) (bool, error) {
	dk := zsetStore.DataKey(key, member)
	ik := zsetStore.IndexKey(key, score, member)
	var exists bool
	if v, err := s.db.Get(dk); err != nil {
		return false, err
	} else if v != nil {
		exists = true
		oldScore, _ := NewBuffer(v).ReadInt64()
		oldIk := zsetStore.IndexKey(key, oldScore, member)
		b.Delete(oldIk)
	}
	buf := NewBuffer(nil)
	buf.WriteInt64(score)
	b.Put(dk, buf.Bytes())
	b.Put(ik, []byte{})
	return exists, nil
}

func (s *Store) ZRem(key []byte, members ...[]byte) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	b := s.batch.Begin()
	defer b.Release()
	meta, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !meta.IsZset() {
		return 0, ErrMetaType
	}
	var n int64
	for _, member := range members {
		score, err := s.zMemberScore(key, member)
		if err != nil {
			continue
		}
		dk := zsetStore.DataKey(key, member)
		ik := zsetStore.IndexKey(key, score, member)
		b.Delete(dk)
		b.Delete(ik)
		n++
	}
	if n > 0 {
		meta.Size -= n
		s.setMeta(b, key, meta)
	}
	err = b.Commit()
	return n, err
}

func (s *Store) zClear(b *Batch, key []byte) error {
	// delete data
	dk := zsetStore.DataKey(key, nil)
	dataIter := s.db.NewPrefixIterator(dk, 0, -1)
	defer dataIter.Close()
	for ; dataIter.Valid(); dataIter.Next() {
		b.Delete(dataIter.Key())
	}
	// delete index
	start := zsetStore.IndexKey(key, MinScore, nil)
	end := zsetStore.IndexKey(key, MaxScore, nil)
	indexIter := s.db.NewRangeIterator(start, end, 0, -1)
	defer indexIter.Close()
	for ; indexIter.Valid(); indexIter.Next() {
		b.Delete(indexIter.Key())
	}
	err := b.Commit()
	return err
}
