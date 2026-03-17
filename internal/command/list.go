package command

import (
	"errors"

	"github.com/TZJ-BYTE/RediGo/internal/database"
	"github.com/TZJ-BYTE/RediGo/internal/datastruct"
	"github.com/TZJ-BYTE/RediGo/internal/protocol"
)

// LPushCommand LPUSH 命令
type LPushCommand struct{}

func (c *LPushCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'lpush' command"))
	}

	key := argString(args, 0)

	value, exists := db.Get(key)
	var list *datastruct.List

	if !exists {
		list = &datastruct.List{
			Data: make([]string, 0),
		}
	} else {
		l, ok := value.Value.(*datastruct.List)
		if !ok {
			return protocol.MakeError(errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"))
		}
		list = l
	}

	for i := 1; i < len(args); i++ {
		list.PushLeft(argString(args, i))
	}

	if err := db.Set(key, &datastruct.DataValue{
		Value:      list,
		ExpireTime: 0,
	}); err != nil {
		return protocol.MakeError(err)
	}

	return protocol.MakeInteger(int64(len(list.Data)))
}

// RPushCommand RPUSH 命令
type RPushCommand struct{}

func (c *RPushCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) < 2 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'rpush' command"))
	}

	key := argString(args, 0)

	value, exists := db.Get(key)
	var list *datastruct.List

	if !exists {
		list = &datastruct.List{
			Data: make([]string, 0),
		}
	} else {
		l, ok := value.Value.(*datastruct.List)
		if !ok {
			return protocol.MakeError(errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"))
		}
		list = l
	}

	for i := 1; i < len(args); i++ {
		list.PushRight(argString(args, i))
	}

	if err := db.Set(key, &datastruct.DataValue{
		Value:      list,
		ExpireTime: 0,
	}); err != nil {
		return protocol.MakeError(err)
	}

	return protocol.MakeInteger(int64(len(list.Data)))
}

// LPopCommand LPOP 命令
type LPopCommand struct{}

func (c *LPopCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'lpop' command"))
	}

	key := argString(args, 0)

	value, exists := db.Get(key)
	if !exists {
		return protocol.MakeNull()
	}

	list, ok := value.Value.(*datastruct.List)
	if !ok {
		return protocol.MakeError(errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"))
	}

	val, success := list.PopLeft()
	if !success {
		return protocol.MakeNull()
	}

	return protocol.MakeBulkString(val)
}

// RPopCommand RPOP 命令
type RPopCommand struct{}

func (c *RPopCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'rpop' command"))
	}

	key := argString(args, 0)

	value, exists := db.Get(key)
	if !exists {
		return protocol.MakeNull()
	}

	list, ok := value.Value.(*datastruct.List)
	if !ok {
		return protocol.MakeError(errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"))
	}

	val, success := list.PopRight()
	if !success {
		return protocol.MakeNull()
	}

	return protocol.MakeBulkString(val)
}

// LLenCommand LLEN 命令
type LLenCommand struct{}

func (c *LLenCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 1 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'llen' command"))
	}

	key := argString(args, 0)

	value, exists := db.Get(key)
	if !exists {
		return protocol.MakeInteger(0)
	}

	list, ok := value.Value.(*datastruct.List)
	if !ok {
		return protocol.MakeError(errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"))
	}

	return protocol.MakeInteger(int64(len(list.Data)))
}

// LRangeCommand LRANGE 命令
type LRangeCommand struct{}

func (c *LRangeCommand) Execute(db *database.Database, args [][]byte) *protocol.Response {
	if len(args) != 3 {
		return protocol.MakeError(errors.New("ERR wrong number of arguments for 'lrange' command"))
	}

	key := argString(args, 0)
	start := parseIntBytes(args[1])
	stop := parseIntBytes(args[2])

	value, exists := db.Get(key)
	if !exists {
		return protocol.MakeArray([]string{})
	}

	list, ok := value.Value.(*datastruct.List)
	if !ok {
		return protocol.MakeError(errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"))
	}

	result := list.Range(start, stop)
	return protocol.MakeArray(result)
}

func parseIntBytes(b []byte) int {
	n, _ := protocol.ParseInt(b)
	return n
}
