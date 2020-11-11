package leveldb

import (
	"bytes"
	"fmt"
	"os"

	"github.com/meixiu/puredb/config"

	"github.com/syndtr/goleveldb/leveldb/util"

	"errors"

	"github.com/meixiu/puredb/driver"

	"github.com/syndtr/goleveldb/leveldb/filter"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type DB struct {
	path      string
	rdb       *leveldb.DB
	opts      *opt.Options
	readOpts  *opt.ReadOptions
	writeOpts *opt.WriteOptions
}

func (db *DB) init(path string, cfg config.LevelDBConfig, repair bool) error {
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}
	opts := &opt.Options{}
	opts.ErrorIfMissing = false
	opts.ErrorIfExist = false

	opts.Filter = filter.NewBloomFilter(cfg.BloomFilterSize)

	opts.Compression = opt.SnappyCompression

	opts.BlockSize = cfg.BlockSize
	opts.WriteBuffer = cfg.WriteBufferSize
	opts.OpenFilesCacheCapacity = cfg.MaxOpenFiles

	opts.CompactionTableSize = 32 * 1024 * 1024
	opts.WriteL0SlowdownTrigger = 16
	opts.WriteL0PauseTrigger = 64

	db.path = path
	db.opts = opts
	db.readOpts = &opt.ReadOptions{
		DontFillCache: true,
	}
	db.writeOpts = &opt.WriteOptions{
		Sync: true,
	}

	if repair {
		if rdb, err := leveldb.RecoverFile(db.path, db.opts); err != nil {
			return err
		} else {
			db.rdb = rdb
			return nil
		}
	}
	if rdb, err := leveldb.OpenFile(path, db.opts); err != nil {
		return err
	} else {
		db.rdb = rdb
	}
	return nil
}

func (db *DB) Close() {
	if db.rdb != nil {
		db.rdb.Close()
		db.rdb = nil
	}
}

func (db *DB) Clear() error {
	if db.rdb != nil {
		db.rdb.Close()
		db.rdb = nil
		db.opts.ErrorIfMissing = false
		db.opts.ErrorIfExist = true
		if err := os.RemoveAll(db.path); err != nil {
			return err
		} else if db.rdb, err = leveldb.OpenFile(db.path, db.opts); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) NewBatch() driver.Batch {
	wb := &Batch{
		db:    db,
		batch: &leveldb.Batch{},
	}
	return wb
}

func (db *DB) NewIterator() driver.Iterator {
	return &Iterator{
		iter: db.rdb.NewIterator(nil, db.readOpts),
	}
}

func (db *DB) NewRangeIterator(start, end []byte) driver.Iterator {
	return &Iterator{
		iter: db.rdb.NewIterator(&util.Range{
			Start: start,
			Limit: end,
		}, db.readOpts),
	}
}

func (db *DB) NewSnapshot() driver.Snapshot {
	snap, _ := db.rdb.GetSnapshot()
	return &Snapshot{
		snap: snap,
		db:   db,
	}
}

func (db *DB) Get(key []byte) ([]byte, error) {
	value, err := db.rdb.Get(key, db.readOpts)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, nil
	}
	return value, err
}

func (db *DB) Put(key, value []byte) error {
	return db.rdb.Put(key, value, nil)
}

func (db *DB) Has(key []byte) (bool, error) {
	return db.rdb.Has(key, db.readOpts)
}

func (db *DB) Delete(key []byte) error {
	return db.rdb.Delete(key, nil)
}

func (db *DB) Compact() error {
	return db.rdb.CompactRange(util.Range{
		Start: nil,
		Limit: nil,
	})
}

func (db *DB) Stats() string {
	var b bytes.Buffer
	for _, s := range []string{"leveldb.stats", "leveldb.sstables"} {
		v, _ := db.rdb.GetProperty(s)
		fmt.Fprintf(&b, "[%s]\n%s\n", s, v)
	}
	return b.String()
}
