package main

import (
	"time"

	"github.com/meixiu/puredb/config"

	_ "github.com/meixiu/puredb/driver/leveldb"
	"github.com/meixiu/puredb/server"
)

func main() {
	conf := config.NewDefaultConfig()
	conf.Password = "xxx"
	server.Start(conf)
	time.Sleep(time.Hour)
}
