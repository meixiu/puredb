package server

import (
	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
	"github.com/meixiu/puredb/store"
)

func init() {
	handle["hlen"] = hlenCmd
	handle["hget"] = hgetCmd
	handle["hmget"] = hmgetCmd
	handle["hgetall"] = hgetallCmd
	handle["hset"] = hsetCmd
	handle["hmset"] = hmsetCmd
	handle["hincrby"] = hincrbyCmd
	handle["hdel"] = hdelCmd
}

func hlenCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	n, err := db.HLen(c.Arg(0))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}

func hgetCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	p, err := db.HGet(c.Arg(0), c.Arg(1))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendBulk(p)
}

func hmgetCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() < 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	key := c.Arg(0)
	fields := argsToArray(c.Args[1:])
	kvs, err := db.HMGet(key, fields...)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.Append(fvPariToArray(kvs))
}

func hgetallCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	kvs, err := db.HGetAll(c.Arg(0))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.Append(fvPariToMap(kvs))
}

func hsetCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 3 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	n, err := db.HSet(c.Arg(0), c.Arg(1), c.Arg(2))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.Append(n)
}

func hmsetCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() < 3 || c.ArgN()%2 != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	key := c.Arg(0)
	kvs := make([]store.FVPair, 0)
	args := c.Args[1:]
	for i := 0; i < len(args)/2; i++ {
		kvs = append(kvs, store.FVPair{
			Field: args[2*i],
			Value: args[2*i+1],
		})
	}
	err := db.HMSet(key, kvs...)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendOK()
}

func hincrbyCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 3 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	delta, _ := c.Arg(2).Int()
	n, err := db.HIncrBy(c.Arg(0), c.Arg(1), delta)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}

func hdelCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() < 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	key := c.Arg(0)
	fields := argsToArray(c.Args[1:])
	n, err := db.HDel(key, fields...)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}
