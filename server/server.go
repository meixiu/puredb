package server

import (
	"net"

	"github.com/bsm/redeo/resp"

	"github.com/meixiu/puredb/config"
	"github.com/meixiu/puredb/purext/logger"

	"github.com/bsm/redeo"
	"github.com/meixiu/puredb/store"
)

var (
	server *redeo.Server
	db     *store.Store
	handle = make(map[string]redeo.HandlerFunc)
	conf   *config.Config
)

func Start(cfg *config.Config) {
	conf = cfg
	s, err := store.Open(conf)
	if err != nil {
		logger.Fatal(err)
	}
	db = s
	server := redeo.NewServer(nil)
	server.HandleFunc("auth", authCmd)
	for k, fn := range handle {
		server.HandleFunc(k, warpCheckAuth(fn))
	}
	go runServer(server)
}

func warpCheckAuth(fn redeo.HandlerFunc) redeo.HandlerFunc {
	return func(w resp.ResponseWriter, c *resp.Command) {
		if conf.Password != "" {
			client := redeo.GetClient(c.Context())
			if v, ok := client.Context().Value(ctxAuthOK{}).(bool); !ok || !v {
				w.AppendError("NOAUTH Authentication required.")
				return
			}
		}
		fn(w, c)
	}
}

func runServer(server *redeo.Server) {
	listener, err := net.Listen("tcp", conf.Addr)
	if err != nil {
		logger.Fatal(err)
	}
	defer listener.Close()
	logger.Infof("waiting for connections on %s", listener.Addr().String())
	server.Serve(listener)
}
