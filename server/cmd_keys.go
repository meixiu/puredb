package server

import (
	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
)

func init() {
	handle["exists"] = existsCmd
	handle["del"] = delCmd
}

func existsCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	has, err := db.Exists(c.Arg(0))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	if has {
		w.AppendInt(1)
	} else {
		w.AppendInt(0)
	}
}

func delCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() == 0 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	keys := make([][]byte, 0)
	for _, v := range c.Args {
		keys = append(keys, v.Bytes())
	}
	n, err := db.Del(keys...)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}
