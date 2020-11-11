package server

import (
	"context"

	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
)

type ctxAuthOK struct{}

func authCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	if conf.Password == "" {
		w.AppendError("ERR Client sent AUTH, but no password is set")
		return
	}
	if conf.Password != c.Arg(0).String() {
		w.AppendError("ERR invalid password")
		return
	}
	client := redeo.GetClient(c.Context())
	client.SetContext(context.WithValue(client.Context(), ctxAuthOK{}, true))
	w.AppendOK()
}
