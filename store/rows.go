package store

import (
	"errors"
	"fmt"

	"github.com/meixiu/puredb/purext/logger"
)

const (
	MetaCode   = byte('#')
	DataCode   = byte('&')
	TimeCode   = byte('@')
	IndexCode  = byte('+')
	StringCode = byte('K')
	HashCode   = byte('H')
	ListCode   = byte('L')
	ZSetCode   = byte('Z')
)

var (
	kvStore   = &kvRows{StringCode}
	hashStore = &hashRows{HashCode}
	listStore = &listRows{ListCode}
	zsetStore = &zsetRows{ZSetCode}
)

var (
	ErrMetaType = errors.New("error meta type")
	ErrHashKey  = errors.New("error hash key")
	ErrZsetKey  = errors.New("error zset key")
)

type kvRows struct {
	code byte
}

func (r *kvRows) MetaKey(key []byte) []byte {
	return nil
}

// DataKey $K{key}
func (r *kvRows) DataKey(key []byte) []byte {
	w := NewBuffer(nil)
	w.WriteByte(DataCode)
	w.WriteByte(r.code)
	w.WriteBytes(key)
	logger.Debug("String DataKey:", w.String())
	return w.Bytes()
}

type hashRows struct {
	code byte
}

// MetaKey #{key}  => {size}
func (r *hashRows) MetaKey(key []byte) []byte {
	w := NewBuffer(nil)
	w.WriteByte(MetaCode)
	w.WriteByte(r.code)
	w.WriteBytes(key)
	logger.Debug("Hash MetaKey:", w.String())
	return w.Bytes()
}

// DataKey $H{key}:{field} => {value}
func (r *hashRows) DataKey(key []byte, field []byte) []byte {
	w := NewBuffer(nil)
	w.WriteByte(DataCode)
	w.WriteByte(r.code)
	w.WriteVarBytes(key)
	w.WriteByte(':')
	if field != nil {
		w.WriteVarBytes(field)
	}
	logger.Debug("Hash DataKey:", w.String())
	return w.Bytes()
}

func (r *hashRows) ParseDataKey(key []byte) ([]byte, []byte, error) {
	buf := NewBuffer(key)
	buf.ReadBytes(2)
	k, _ := buf.ReadVarBytes()
	if k == nil {
		return nil, nil, ErrHashKey
	}
	buf.ReadByte()
	f, _ := buf.ReadVarBytes()
	if f == nil {
		return nil, nil, ErrHashKey
	}
	return k, f, nil
}

type listRows struct {
	code byte
}

func (r *listRows) MetaKey(key []byte) []byte {
	w := NewBuffer(nil)
	w.WriteByte(MetaCode)
	w.WriteByte(r.code)
	w.WriteBytes(key)
	logger.Debug("List MetaKey:", w.String())
	return w.Bytes()
}

// DataKey $L{key}:{index} => {value}
func (r *listRows) DataKey(key []byte, index int64) []byte {
	w := NewBuffer(nil)
	w.WriteByte(DataCode)
	w.WriteByte(r.code)
	w.WriteVarBytes(key)
	w.WriteByte(':')
	w.WriteInt64(index)
	logger.Debug("List DataKey:", w.String())
	return w.Bytes()
}

type zsetRows struct {
	code byte
}

func (r *zsetRows) MetaKey(key []byte) []byte {
	w := NewBuffer(nil)
	w.WriteByte(MetaCode)
	w.WriteByte(r.code)
	w.WriteBytes(key)
	logger.Debug("Zset MetaKey:", w.String())
	return w.Bytes()
}

// DataKey $Z{key}:{member} => {score}
func (r *zsetRows) DataKey(key []byte, member []byte) []byte {
	w := NewBuffer(nil)
	w.WriteByte(DataCode)
	w.WriteByte(r.code)
	w.WriteVarBytes(key)
	w.WriteByte(':')
	if member != nil {
		w.WriteVarBytes(member)
	}
	logger.Debug("Zset DataKey:", w.String())
	return w.Bytes()
}

// IndexKey +Z{key}={score}:{member} => nil
func (r *zsetRows) IndexKey(key []byte, score int64, member []byte) []byte {
	fmt.Println(key, score, member)
	w := NewBuffer(nil)
	w.WriteByte(IndexCode)
	w.WriteByte(r.code)
	w.WriteVarBytes(key)
	if score < 0 {
		w.WriteByte('<')
	} else {
		w.WriteByte('=')
	}
	w.WriteInt64(score)
	w.WriteByte(':')
	if member != nil {
		w.WriteVarBytes(member)
	}
	logger.Debug("Zset IndexKey:", w.String())
	return w.Bytes()
}

func (r *zsetRows) ParseIndexKey(key []byte) (int64, []byte, error) {
	buf := NewBuffer(key)
	buf.ReadBytes(2)
	k, _ := buf.ReadVarBytes()
	if k == nil {
		return 0, nil, ErrZsetKey
	}
	buf.ReadByte()
	score, _ := buf.ReadInt64()
	buf.ReadByte()
	memeber, _ := buf.ReadVarBytes()
	if memeber == nil {
		return 0, nil, ErrZsetKey
	}
	return score, memeber, nil
}
