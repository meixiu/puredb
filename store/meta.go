package store

import (
	"fmt"

	"github.com/meixiu/puredb/purext/logger"
)

type Meta struct {
	Code   byte
	Expire int64
	Size   int64
	LIndex int64
	RIndex int64
}

func (m *Meta) IsString() bool {
	if !m.Exists() {
		m.Code = StringCode
	}
	return m.Code == StringCode
}

func (m *Meta) IsHash() bool {
	if !m.Exists() {
		m.Code = HashCode
	}
	return m.Code == HashCode
}

func (m *Meta) IsList() bool {
	if !m.Exists() {
		m.Code = ListCode
	}
	return m.Code == ListCode
}

func (m *Meta) IsZset() bool {
	if !m.Exists() {
		m.Code = ZSetCode
	}
	return m.Code == ZSetCode
}

func (m *Meta) Exists() bool {
	return m.Code != byte(0)
}

func (m *Meta) Empty() bool {
	return m.Size == 0
}

func (s *Store) metaKey(key []byte) []byte {
	w := NewBuffer(nil)
	w.WriteByte(MetaCode)
	w.WriteBytes(key)
	logger.Debug("metaKey:", w.String())
	return w.Bytes()
}

func (s *Store) getMeta(key []byte) (*Meta, error) {
	meta := &Meta{}
	mk := s.metaKey(key)
	p, err := s.db.Get(mk)
	if err != nil || p == nil {
		return meta, err
	}
	buf := NewBuffer(p)
	meta.Code, _ = buf.ReadByte()
	meta.Expire, _ = buf.ReadInt64()
	meta.Size, _ = buf.ReadInt64()
	meta.LIndex, _ = buf.ReadInt64()
	meta.RIndex, _ = buf.ReadInt64()
	return meta, nil
}

func (s *Store) setMeta(b *Batch, key []byte, meta *Meta) error {
	w := NewBuffer(nil)
	w.WriteByte(meta.Code)
	w.WriteInt64(meta.Expire)
	w.WriteInt64(meta.Size)
	w.WriteInt64(meta.LIndex)
	w.WriteInt64(meta.RIndex)
	fmt.Println(s.metaKey(key), w.String())
	b.Put(s.metaKey(key), w.Bytes())
	return nil
}

func (s *Store) delMeta(b *Batch, key []byte) error {
	mk := s.metaKey(key)
	b.Delete(mk)
	return nil
}
