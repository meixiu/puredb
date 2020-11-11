package leveldb

import (
	"github.com/meixiu/puredb/config"

	"github.com/meixiu/puredb/driver"
)

type Driver struct{}

func (s *Driver) Name() string {
	return "leveldb"
}

func (s *Driver) Open(path string, cfg *config.Config) (driver.DB, error) {
	return s.openOrRepair(path, cfg, false)
}

func (s *Driver) Repair(path string, cfg *config.Config) (driver.DB, error) {
	return s.openOrRepair(path, cfg, true)
}

func (s *Driver) openOrRepair(path string, cfg *config.Config, repair bool) (driver.DB, error) {
	db := &DB{}
	if err := db.init(path, cfg.GoLevelDB, repair); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func init() {
	driver.Register(&Driver{})
}
