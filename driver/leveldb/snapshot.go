package leveldb

import (
	"errors"

	"github.com/meixiu/puredb/driver"
	"github.com/syndtr/goleveldb/leveldb"
)

type Snapshot struct {
	snap *leveldb.Snapshot
	db   *DB
}

func (sp *Snapshot) Close() {
	sp.snap.Release()
}

func (sp *Snapshot) NewIterator() driver.Iterator {
	return &Iterator{
		iter: sp.snap.NewIterator(nil, sp.db.readOpts),
	}
}

func (sp *Snapshot) Get(key []byte) ([]byte, error) {
	value, err := sp.snap.Get(key, sp.db.readOpts)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, nil
	}
	return value, err
}
