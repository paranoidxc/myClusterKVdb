package database

import (
	"myredis/interface/resp"
	"myredis/lib/wildcard"
	"myredis/resp/reply"
)

func init() {
	// 至少2个参数
	RegisterCommand("DEL", execDel, -2)
	RegisterCommand("Exists", execExists, -2)
	RegisterCommand("FlushDB", execFlushDB, 1)
	RegisterCommand("Type", execType, 2)
	RegisterCommand("Rename", execRename, 3)
	RegisterCommand("Renamenx", execRenamenx, 3)
	RegisterCommand("keys", execKeys, 2) // keys *
}

// DEL
// k1 k2 k3
func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...)

	return reply.MakeIntReply(int64(deleted))
}

// EXISTS
// EXISTS k1 k2 k3
func execExists(db *DB, args [][]byte) resp.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result += 1
		}
	}

	return reply.MakeIntReply(result)
}

// FLUSHDB
func execFlushDB(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	return reply.MakeOkReply()
}

// TYPE
// type k1
func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none")
	}

	// todo 暂时只有string类型的实现
	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string")
	}

	return reply.MakeStatusReply("unknown")
}

// RENAME
// rename k1 k2
func execRename(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dest := string(args[1])

	entity, exists := db.GetEntity(src)
	if !exists {
		return reply.MakeErrReply("no such key")
	}

	db.PutEntity(dest, entity)
	db.Remove(src)

	return reply.MakeOkReply()
}

// RENAMENX
// RENAMENX k1 k2
// 要先判断 k2 是否存在
func execRenamenx(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dest := string(args[1])

	_, ok := db.GetEntity(dest)
	if ok {
		//存在 不操作
		return reply.MakeIntReply(0)
	}

	entity, exists := db.GetEntity(src)
	if !exists {
		return reply.MakeErrReply("no such key")
	}

	db.PutEntity(dest, entity)
	db.Remove(src)

	return reply.MakeIntReply(1)
}

// KEYS *
func execKeys(db *DB, args [][]byte) resp.Reply {
	pattern := wildcard.CompilePattern(string(args[0]))
	result := make([][]byte, 0)

	db.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})

	return reply.MakeMultiBulkReply(result)
}
