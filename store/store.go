package store

import (
	"os"

	"github.com/meixiu/puredb/config"
	"github.com/meixiu/puredb/driver"
)

type Store struct {
	db *DB

	batch *Batch
}

func Open(cfg *config.Config) (*Store, error) {
	d, err := driver.GetDriver(cfg)
	if err != nil {
		return nil, err
	}
	path := cfg.DBPath
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	rdb, err := d.Open(path, cfg)
	if err != nil {
		return nil, err
	}
	db := newDB(rdb)
	store := &Store{
		db:    db,
		batch: newBatch(db.NewBatch()),
	}
	return store, nil
}

func Repair(cfg *config.Config) error {
	d, err := driver.GetDriver(cfg)
	if err != nil {
		return err
	}
	path := cfg.DBPath
	if _, err := d.Repair(path, cfg); err != nil {
		return err
	}
	return nil
}
