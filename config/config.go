package config

import "github.com/meixiu/puredb/purext/bytesize"

type Config struct {
	Addr      string
	Password  string
	Driver    string
	DBPath    string
	GoLevelDB LevelDBConfig
}

func NewDefaultConfig() *Config {
	c := &Config{
		Addr:     ":6380",
		Password: "",
		Driver:   "leveldb",
		DBPath:   "db_data",
		GoLevelDB: LevelDBConfig{
			BlockSize:       bytesize.KB.Int() * 4,
			CacheSize:       bytesize.MB.Int() * 4,
			WriteBufferSize: bytesize.MB.Int() * 4,
			BloomFilterSize: 10,
			MaxOpenFiles:    500,
		},
	}
	return c
}
