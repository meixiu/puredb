package server

import (
	"fmt"

	"github.com/bsm/redeo/resp"
	"github.com/meixiu/puredb/store"
)

func argsToInt64(args resp.CommandArgument) int64 {
	n, err := args.Int()
	fmt.Println("argsToInt64", args, n, err)
	return n
}

func argsToArray(args []resp.CommandArgument) [][]byte {
	p := make([][]byte, 0)
	for _, v := range args {
		p = append(p, v)
	}
	return p
}

func kvPariToArray(args []store.KVPair) []interface{} {
	p := make([]interface{}, 0)
	for _, v := range args {
		if v.Value == nil {
			p = append(p, nil)
		} else {
			p = append(p, v.Value)
		}
	}
	return p
}

func sPariToArray(args []store.ScorePair) []interface{} {
	p := make([]interface{}, 0)
	for _, v := range args {
		p = append(p, v.Member)
	}
	return p
}

func sPariToMap(args []store.ScorePair) []interface{} {
	fmt.Println(args)
	p := make([]interface{}, 0)
	for _, v := range args {
		p = append(p, v.Member, v.Score)
	}
	return p
}

func fvPariToArray(args []store.FVPair) []interface{} {
	p := make([]interface{}, 0)
	for _, v := range args {
		if v.Value == nil {
			p = append(p, nil)
		} else {
			p = append(p, v.Value)
		}
	}
	return p
}

func fvPariToMap(args []store.FVPair) []interface{} {
	fmt.Println(args)
	p := make([]interface{}, 0)
	for _, v := range args {
		p = append(p, v.Field)
		if v.Value == nil {
			p = append(p, nil)
		} else {
			p = append(p, v.Value)
		}
	}
	return p
}
