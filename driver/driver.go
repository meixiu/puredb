package driver

import (
	"fmt"

	"github.com/meixiu/puredb/config"
)

type Driver interface {
	Name() string
	Open(path string, cfg *config.Config) (DB, error)
	Repair(path string, cfg *config.Config) (DB, error)
}

var (
	drivers = make(map[string]Driver)
)

// Register
func Register(driver Driver) {
	if _, ok := drivers[driver.Name()]; ok {
		panic(fmt.Sprintf("%s has been already registered", driver.Name()))
	}
	drivers[driver.Name()] = driver
}

func GetDriver(cfg *config.Config) (Driver, error) {
	d, ok := drivers[cfg.Driver]
	if !ok {
		return nil, fmt.Errorf("%s is not registered", cfg.Driver)
	}
	return d, nil
}
