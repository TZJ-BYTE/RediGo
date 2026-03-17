package server

import (
	"github.com/TZJ-BYTE/RediGo/internal/database"
	"github.com/TZJ-BYTE/RediGo/internal/datastruct"
	"github.com/TZJ-BYTE/RediGo/internal/protocol"
)

func fastPathExecute(dst []byte, db *database.Database, cmd string, args [][]byte) ([]byte, bool) {
	switch cmd {
	case "GET":
		if len(args) != 1 {
			return protocol.AppendErrorString(dst, "ERR wrong number of arguments for 'get' command"), true
		}
		value, exists := db.GetBytes(args[0])
		if !exists {
			return protocol.AppendNull(dst), true
		}
		switch v := value.Value.(type) {
		case *datastruct.String:
			return protocol.AppendBulkString(dst, v.Data), true
		case *datastruct.BytesString:
			return protocol.AppendBulkBytes(dst, v.Data), true
		default:
			return protocol.AppendErrorString(dst, "WRONGTYPE Operation against a key holding the wrong kind of value"), true
		}

	case "SET":
		if len(args) < 2 {
			return protocol.AppendErrorString(dst, "ERR wrong number of arguments for 'set' command"), true
		}
		if err := db.SetStringBytes(args[0], args[1]); err != nil {
			return protocol.AppendErrorString(dst, err.Error()), true
		}
		return protocol.AppendSimpleString(dst, "OK"), true

	case "INCR":
		if len(args) != 1 {
			return protocol.AppendErrorString(dst, "ERR wrong number of arguments for 'incr' command"), true
		}
		newValue, err := db.IncrStringBytes(args[0])
		if err != nil {
			return protocol.AppendErrorString(dst, err.Error()), true
		}
		return protocol.AppendInteger(dst, newValue), true
	}

	return dst, false
}
