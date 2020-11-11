package server

import (
	"fmt"
	"strings"

	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
	"github.com/meixiu/puredb/store"
)

func init() {
	handle["zcount"] = zcountCmd
	handle["zscore"] = zscoreCmd
	handle["zcard"] = zcardCmd
	handle["zadd"] = zaddCmd
	handle["zrem"] = zremCmd
	handle["zrank"] = zrankCmd
	handle["zrevrank"] = zrevrankCmd
	handle["zrange"] = zrangeCmd
	handle["zrevrange"] = zrevrangeCmd
	handle["zrangebyscore"] = zrangebyscoreCmd
	handle["zrevrangebyscore"] = zrevrangebyscoreCmd
}

func zcountCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 3 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	min, max := scoreToInt(c.Arg(1), c.Arg(2))
	n, err := db.ZCount(c.Arg(0), min, max)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}

func zscoreCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	n, err := db.ZScore(c.Arg(0), c.Arg(1))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}

func zcardCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	n, err := db.ZCard(c.Arg(0))
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}

func zaddCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() < 3 || c.ArgN()%2 != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	key := c.Arg(0)
	kvs := make([]store.ScorePair, 0)
	args := c.Args[1:]
	for i := 0; i < len(args)/2; i++ {
		score, _ := args[2*i+1].Int()
		kvs = append(kvs, store.ScorePair{
			Member: args[2*i],
			Score:  score,
		})
	}
	n, err := db.ZAdd(key, kvs...)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}

func zremCmd(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() < 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	key := c.Arg(0)
	member := argsToArray(c.Args[1:])
	n, err := db.ZRem(key, member...)
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}

func zrankCmd(w resp.ResponseWriter, c *resp.Command) {
	zrankGeneric(w, c, false)
}

func zrevrankCmd(w resp.ResponseWriter, c *resp.Command) {
	zrankGeneric(w, c, true)
}

func zrankGeneric(w resp.ResponseWriter, c *resp.Command, rev bool) {
	if c.ArgN() != 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	var n int64
	var err error
	if rev {
		n, err = db.ZRevRank(c.Arg(0), c.Arg(1))
	} else {
		n, err = db.ZRank(c.Arg(0), c.Arg(1))
	}
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.AppendInt(n)
}

func zrangeCmd(w resp.ResponseWriter, c *resp.Command) {
	zrangeGeneric(w, c, false)
}

func zrevrangeCmd(w resp.ResponseWriter, c *resp.Command) {
	zrangeGeneric(w, c, true)
}

func zrangeGeneric(w resp.ResponseWriter, c *resp.Command, rev bool) {
	if c.ArgN() != 3 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	start, err := c.Arg(1).Int()
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	stop, err := c.Arg(2).Int()
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	var kvs []store.ScorePair
	if rev {
		kvs, err = db.ZRevRange(c.Arg(0), start, stop)
	} else {
		kvs, err = db.ZRange(c.Arg(0), start, stop)
	}
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	w.Append(sPariToArray(kvs))
}

func zrangebyscoreCmd(w resp.ResponseWriter, c *resp.Command) {
	zrangebyscoreGeneric(w, c, false)
}

func zrevrangebyscoreCmd(w resp.ResponseWriter, c *resp.Command) {
	zrangebyscoreGeneric(w, c, true)
}

func scoreToInt(minScore, maxScore []byte) (int64, int64) {
	var min, max int64
	fmt.Println(string(minScore), string(maxScore))
	if string(minScore) == "-inf" {
		min = store.MinScore
	} else {
		min = argsToInt64(minScore)
	}
	if string(maxScore) == "+inf" {
		max = store.MaxScore
	} else {
		max = argsToInt64(maxScore)
	}
	return min, max
}

func zrangebyscoreGeneric(w resp.ResponseWriter, c *resp.Command, rev bool) {
	if c.ArgN() < 3 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}
	key := c.Arg(0)
	var minScore, maxScore []byte
	if rev {
		minScore, maxScore = c.Arg(2), c.Arg(1)
	} else {
		minScore, maxScore = c.Arg(1), c.Arg(2)
	}
	min, max := scoreToInt(minScore, maxScore)
	var withScores bool
	args := c.Args[3:]
	if len(args) > 0 {
		if strings.ToLower(args[0].String()) == "withscores" {
			withScores = true
			args = args[1:]
		}
	}
	var offset int = 0
	var count int = -1
	if len(args) > 0 {
		if len(args) != 3 {
			w.AppendError("ERR syntax error")
			return
		}
		if strings.ToLower(args[0].String()) != "limit" {
			w.AppendError("ERR syntax error")
			return
		}
		offset = int(argsToInt64(args[1]))
		count = int(argsToInt64(args[2]))
	}
	var kvs []store.ScorePair
	var err error
	if rev {
		kvs, err = db.ZRevRangeByScore(key, min, max, offset, count)
	} else {
		kvs, err = db.ZRangeByScore(key, min, max, offset, count)
	}
	if err != nil {
		w.AppendError(err.Error())
		return
	}
	if withScores {
		w.Append(sPariToMap(kvs))
	} else {
		w.Append(sPariToArray(kvs))
	}
}
