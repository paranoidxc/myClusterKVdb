package cluster

import (
	"myredis/interface/resp"
	"myredis/lib/utils"
	"myredis/resp/reply"
)

// del k1 k2 k3 ...
func del(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	// 转发取决于key的hash
	cmds := cmdArgs[1:]

	for _, keyB := range cmds {
		key := string(keyB)
		peer := cluster.peerPicker.PickNode(key)
		cluster.relay(peer, c, utils.ToCmdLine("del", key))
	}

	return reply.MakeIntReply(int64(len(cmds)))

	/*
		replies := cluster.broadcast(c, cmdArgs)

		var errReply reply.ErrorReply
		var deleted int64 = 0

		for _, r := range replies {
			if reply.IsErrorReply(r) {
				errReply = r.(reply.ErrorReply)
				break
			}
			intReply, ok := r.(*reply.IntReply)
			if !ok {
				errReply = reply.MakeErrReply("error ")
			} else {
				deleted += intReply.Code
			}
		}

		if errReply == nil {
			return reply.MakeIntReply(deleted)
		}

		return reply.MakeErrReply("error:" + errReply.Error())
	*/

}
