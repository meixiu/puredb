package server

import (
	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
)

func init() {
	handle["llen"] = llenCmd
	handle["lpop"] = lpopCmd
	handle["rpop"] = rpopCmd
	handle["lpush"] = lpushCmd
	handle["rpush"] = rpushCmd
}

func llenCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	n, err := db.LLen(c.Arg(0))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}

func lpopCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	p, err := db.LPop(c.Arg(0))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	if p == nil {
		w.AppendNil()
	} else {
		w.AppendBulk(p)
	}
}

func rpopCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	p, err := db.RPop(c.Arg(0))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	if p == nil {
		w.AppendNil()
	} else {
		w.AppendBulk(p)
	}
}

func lpushCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() < 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	key := c.Arg(0)
	args := argsToArray(c.Args[1:])
	n, err := db.LPush(key, args...)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}

func rpushCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() < 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	key := c.Arg(0)
	args := argsToArray(c.Args[1:])
	n, err := db.RPush(key, args...)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}
