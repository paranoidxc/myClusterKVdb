package database

import (
	"myredis/datastruct/dict"
	"myredis/interface/database"
	"myredis/interface/resp"
	"myredis/resp/reply"
	"strings"
)

type DB struct {
	index  int
	data   dict.Dict
	addAof func(CmdLine)
}

// 所有 redis 指令(get,set ...)给一个实现
type ExecFunc func(db *DB, args [][]byte) resp.Reply

// PING SET SETNX
// PING
// SET key val
type CmdLine = [][]byte

func makeDB() *DB {
	db := &DB{
		data: dict.MakeSyncDict(),
		//给个空的实现 在启动的时候用
		addAof: func(line CmdLine) {},
	}

	return db
}

func (db *DB) Exec(c resp.Connection, cmdLine CmdLine) resp.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("Err unknown command" + cmdName)
	}

	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}

	fun := cmd.executor
	// SET K V
	// 只要 K，V 就可以了 已经知道是 SET 指令了
	return fun(db, cmdLine[1:])
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	return true
}

// GetEntity returns DataEntity bind to given key
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {

	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity a DataEntity into DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush clean database
func (db *DB) Flush() {
	db.data.Clear()
}
