package server

import (
	"github.com/meixiu/puredb/store"

	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
)

func init() {
	handle["get"] = getCmd
	handle["set"] = setCmd
	handle["mget"] = mgetCmd
	handle["mset"] = msetCmd
	handle["strlen"] = strlenCmd
	handle["incr"] = incrCmd
	handle["incrby"] = incrbyCmd
}

func getCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	b, err := db.Get(c.Arg(0))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	if b == nil {
		w.AppendNil()
		return
	}
	w.AppendBulk(b)
}

func setCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	err := db.Set(c.Arg(0), c.Arg(1))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendOK()
}

func mgetCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() == 0 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	keys := make([][]byte, 0)
	for _, v := range c.Args {
		keys = append(keys, v.Bytes())
	}
	kvs, err := db.MGet(keys...)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	arr := kvPariToArray(kvs)
	w.Append(arr)
}

func msetCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() == 0 || c.ArgN()%2 != 0 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	kvs := make([]store.KVPair, c.ArgN()/2)
	for i := 0; i < c.ArgN()/2; i++ {
		kvs[i] = store.KVPair{
			Key:   c.Arg(2 * i),
			Value: c.Arg(2*i + 1),
		}
	}
	err := db.MSet(kvs...)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendOK()
}

func strlenCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	n, err := db.StrLen(c.Arg(0))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}

func incrCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	n, err := db.Incr(c.Arg(0))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}

func incrbyCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	delta, err := c.Arg(1).Int()
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	n, err := db.IncrBy(c.Arg(0), delta)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}
