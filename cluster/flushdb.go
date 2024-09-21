package cluster

import (
	"myredis/interface/resp"
	"myredis/lib/logger"
	"myredis/resp/reply"
)

func flushdb(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	// 真实命令
	cluster.relay(cluster.self, c, cmdArgs)
	if string(cmdArgs[0]) == "flushdb" {
		// 修改后的命名 防止二次转发
		cmdArgs[0] = []byte("flushdbabc")
		logger.Info("change flushdb cmd to: ", string(cmdArgs[0]))
		if cluster.self == cluster.lead {
			for _, node := range cluster.nodes {
				if node != cluster.self {
					_ = cluster.relay(node, c, cmdArgs)
				}
			}
		}
	}

	return reply.MakeOkReply()

	/*
		replies := cluster.broadcast(c, cmdArgs)

		var errReply reply.ErrorReply

		for _, r := range replies {
			if reply.IsErrorReply(r) {
				errReply = r.(reply.ErrorReply)
				break
			}
		}

		if errReply == nil {
			return reply.MakeOkReply()
		}

		return reply.MakeErrReply("error:" + errReply.Error())

	*/
}
