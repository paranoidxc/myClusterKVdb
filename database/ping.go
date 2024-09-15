package database

import (
	"myredis/interface/resp"
	"myredis/resp/reply"
)

func init() {
	RegisterCommand("ping", Ping, 1)
}

func Ping(db *DB, args [][]byte) resp.Reply {
	return reply.MakePongReply()
}
