package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/meixiu/puredb/purext/logger"

	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/meixiu/puredb/store"

	"github.com/meixiu/puredb/config"

	_ "github.com/meixiu/puredb/driver/leveldb"
)

type test struct {
	Name  string
	Index int
}

func main() {
	rang := util.BytesPrefix([]byte("kl"))
	fmt.Println(string(rang.Start), string(rang.Limit))
	cfg := config.NewDefaultConfig()
	db, err := store.Open(cfg)
	if err != nil {
		log.Println("Err:", err)
	}
	err1 := db.Set([]byte("t3:2"), []byte("v3:2"))
	if err1 != nil {
		log.Println("err:", err1)
	}
	v1, err := db.Get([]byte("t3:1"))
	fmt.Println("v1:", string(v1), err)

	db.MSet(store.KVPair{
		Key:   []byte("t3:1"),
		Value: []byte("t3:11111"),
	}, store.KVPair{
		Key:   []byte("t3:2"),
		Value: []byte("t3:22222"),
	}, store.KVPair{
		Key:   []byte("t3:3"),
		Value: []byte("t3:33333"),
	})
	logger.SetLevel(logger.INFO)
	//n1, err := db.Del([]byte("t3:1"), []byte("t3:2"), []byte("t3:3"))
	//fmt.Println(n1, err)

	V2, err := db.MGet([]byte("t3:1"), []byte("t3:2"), []byte("t3:3"))
	fmt.Println(V2, err)

	v3, err := db.Get([]byte("t3:1"))
	fmt.Println(string(v3), err)

	vn, err := db.StrLen([]byte("t3:1"))
	fmt.Println(vn, err)

	v4, err := db.Exists([]byte("t3:1:not_exists"))
	fmt.Println(v4, err)

	n2, err := db.HSet([]byte("h:1"), []byte("a"), []byte("hv:1"))
	fmt.Println(n2, err)
	db.HSet([]byte("h:1"), []byte("b"), []byte("hv:1a"))
	db.HSet([]byte("h:1"), []byte("c"), []byte("hv:1b"))
	db.HSet([]byte("h:1"), []byte("d"), []byte("hv:1c"))

	v5, err := db.HGet([]byte("h:1"), []byte("a"))
	fmt.Println(string(v5), err)

	//vn2, err := db.HDel([]byte("h:1"), []byte("b"), []byte("d"))
	//fmt.Println(vn2, err)

	n7, err := db.IncrBy([]byte("incr:1"), 10)
	fmt.Println(n7, err)

	db.HIncrBy([]byte("h:4"), []byte("i"), 111)
	db.HIncrBy([]byte("h:4"), []byte("i"), -10)

	db.HMSet([]byte("h:4"), store.FVPair{
		Field: []byte("f1"),
		Value: []byte("v1"),
	}, store.FVPair{
		Field: []byte("f2222"),
		Value: []byte("2222"),
	})

	v7, err := db.HGetAll([]byte("h:4"))
	fmt.Println(v7, err)

	v8, err := db.HMGet([]byte("h:4"), []byte("f1"), []byte("f2"))
	fmt.Println(v8, err)

	n10, err := db.HLen([]byte("h:4"))
	fmt.Println(n10, err)
	fmt.Println(strings.Repeat("=", 50))
	for i := 0; i < 10; i++ {
		db.RPush([]byte("list:1"), []byte(fmt.Sprintf("value:%d", i)))
	}
	fmt.Println(strings.Repeat("=", 50))

	for i := 0; i < 15; i++ {
		fmt.Println(db.LLen([]byte("list:1")))
		n, err := db.LPop([]byte("list:1"))
		fmt.Println(string(n), err)
	}

	for i := 0; i < 10; i++ {
		db.ZAdd([]byte("z:1"), store.ScorePair{
			Score:  int64(i),
			Member: []byte(fmt.Sprintf("mem:%d", i)),
		})
	}
	fmt.Println(db.ZCount([]byte("z:1"), 0, 200))
	fmt.Println(db.ZRange([]byte("z:1"), 0, 10))
	fmt.Println(db.ZRevRange([]byte("z:1"), -3, -1))
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println(db.ZRem([]byte("z:1"), []byte("mem:6"), []byte("mem:5")))
	//fmt.Println(db.ZClear([]byte("z:1")))
	fmt.Println(db.ZRangeByScore([]byte("z:1"), 4, 8, 0, 2))
	fmt.Println(db.ZRevRangeByScore([]byte("z:1"), 0, 100, 0, -1))
	fmt.Println(db.ZRank([]byte("z:1"), []byte("mem:0")))
	fmt.Println(db.ZRevRank([]byte("z:1"), []byte("mem:0")))

}
