package store

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/meixiu/puredb/purext/bytesize"
)

var (
	ErrVarBytesLen = errors.New("invalid var bytes length")
	MaxVarBytesLen = bytesize.MB * 512
)

type Buffer struct {
	buf *bytes.Buffer
}

func NewBuffer(p []byte) *Buffer {
	buf := &Buffer{buf: bytes.NewBuffer(p)}
	return buf
}

func (buf *Buffer) readFull(p []byte) (int, error) {
	n := 0
	for len(p) != 0 {
		i, err := buf.buf.Read(p)
		n, p = n+i, p[i:]
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (buf *Buffer) writeFull(p []byte) (int, error) {
	n := 0
	for len(p) != 0 {
		i, err := buf.buf.Write(p)
		n, p = n+i, p[i:]
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (buf *Buffer) ReadByte() (byte, error) {
	b, err := buf.buf.ReadByte()
	return b, err
}

func (buf *Buffer) ReadBytes(n int) ([]byte, error) {
	p := make([]byte, n)
	_, err := buf.readFull(p)
	return p, err
}

func (buf *Buffer) ReadVarInt() (int64, error) {
	v, err := binary.ReadVarint(buf.buf)
	return v, err
}

func (buf *Buffer) ReadUVarInt() (uint64, error) {
	u, err := binary.ReadUvarint(buf.buf)
	return u, err
}

func (buf *Buffer) ReadVarBytes() ([]byte, error) {
	n, err := buf.ReadUVarInt()
	if err != nil {
		return nil, err
	}
	if n < 0 || n > uint64(MaxVarBytesLen) {
		return nil, err
	} else if n == 0 {
		return []byte{}, nil
	}
	return buf.ReadBytes(int(n))
}

func (buf *Buffer) ReadInt64() (int64, error) {
	i, err := buf.ReadUint64()
	return int64(i), err
}

func (buf *Buffer) ReadUint64() (uint64, error) {
	p, err := buf.ReadBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(p), nil
}

func (buf *Buffer) WriteByte(b byte) error {
	return buf.buf.WriteByte(b)
}

func (buf *Buffer) WriteBytes(p []byte) error {
	_, err := buf.writeFull(p)
	return err
}

func (buf *Buffer) WriteVarInt(v int64) error {
	p := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(p, v)
	_, err := buf.writeFull(p[:n])
	return err
}

func (buf *Buffer) WriteUVarInt(v uint64) error {
	p := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(p, v)
	_, err := buf.writeFull(p[:n])
	return err
}

func (buf *Buffer) WriteVarBytes(p []byte) error {
	if n := uint64(len(p)); n > uint64(MaxVarBytesLen) {
		return ErrVarBytesLen
	} else if err := buf.WriteUVarInt(n); err != nil {
		return err
	}
	_, err := buf.writeFull(p)
	return err
}

func (buf *Buffer) WriteInt64(s int64) error {
	return buf.WriteUint64(uint64(s))
}

func (buf *Buffer) WriteUint64(s uint64) error {
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, s)
	_, err := buf.writeFull(p)
	return err
}

func (buf *Buffer) Reset() {
	buf.buf.Reset()
}

func (buf *Buffer) Len() int {
	return buf.buf.Len()
}

func (buf *Buffer) Bytes() []byte {
	return buf.buf.Bytes()
}

func (buf *Buffer) String() string {
	return buf.buf.String()
}
