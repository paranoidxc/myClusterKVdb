package database

import (
	"myredis/interface/database"
	"myredis/interface/resp"
	"myredis/resp/reply"
)

// GET k1
func execGet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}

	bytes := entity.Data.([]byte)
	return reply.MakeBulkReply(bytes)
}

// SET k1 v
func execSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	db.PutEntity(key, entity)
	return reply.MakeOkReply()
}

// SETNX k1 v1
func execSetnx(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	result := db.PutIfAbsent(key, entity)
	return reply.MakeIntReply(int64(result))
}

// GETSET k1 v1
// 获取原来的值 设置新值
func execGetSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity, exists := db.GetEntity(key)
	db.PutEntity(key, &database.DataEntity{
		Data: value,
	})
	if !exists {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.([]byte)
	return reply.MakeBulkReply(bytes)
}

// STRLEN
func execStrlen(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.([]byte)
	return reply.MakeIntReply(int64(len(bytes)))
}

func init() {
	RegisterCommand("GET", execGet, 2) //get k1
	RegisterCommand("SET", execSet, 3) //set k1 v1
	RegisterCommand("setnx", execSetnx, 3)
	RegisterCommand("getset", execGetSet, 3)
	RegisterCommand("strlen", execStrlen, 2)
}
