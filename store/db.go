package store

import (
	"github.com/meixiu/puredb/driver"
)

type DB struct {
	rdb driver.DB
}

func newDB(rdb driver.DB) *DB {
	return &DB{rdb: rdb}
}

func (db *DB) Close() {
	db.rdb.Close()
}

func (db *DB) Clear() error {
	return db.rdb.Clear()
}

func (db *DB) NewBatch() driver.Batch {
	return db.rdb.NewBatch()
}

func (db *DB) NewIterator() driver.Iterator {
	return db.rdb.NewIterator()
}

func (db *DB) NewSnapshot() driver.Snapshot {
	return db.rdb.NewSnapshot()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return db.rdb.Get(key)
}

func (db *DB) Put(key, value []byte) error {
	return db.rdb.Put(key, value)
}

func (db *DB) Has(key []byte) (bool, error) {
	return db.rdb.Has(key)
}

func (db *DB) Delete(key []byte) error {
	return db.rdb.Delete(key)
}

func (db *DB) Compact() error {
	return db.rdb.Compact()
}

func (db *DB) Stats() string {
	return db.rdb.Stats()
}

func (db *DB) NewRangeIterator(start, end []byte, offset, limit int) *RangeIterator {
	iter := db.rdb.NewRangeIterator(start, end)
	it := newRangeIterator(iter, offset, limit, DirForward)
	return it
}

func (db *DB) NewRevRangeIterator(start, end []byte, offset, limit int) *RangeIterator {
	iter := db.rdb.NewRangeIterator(start, end)
	it := newRangeIterator(iter, offset, limit, DirBackward)
	return it
}

func (db *DB) NewPrefixIterator(prefix []byte, offset, limit int) *RangeIterator {
	start, end := prefixRange(prefix)
	return db.NewRangeIterator(start, end, offset, limit)
}

func (db *DB) NewRevPrefixIterator(prefix []byte, offset, limit int) *RangeIterator {
	start, end := prefixRange(prefix)
	return db.NewRevRangeIterator(start, end, offset, limit)
}
