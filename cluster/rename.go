package cluster

import (
	"myredis/interface/resp"
	"myredis/resp/reply"
)

// rename k1 k2
// key 有可能跑到其他节点去
func rename(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	if len(cmdArgs) != 3 {
		return reply.MakeArgNumErrReply("ERR wrong number args")
	}

	src := string(cmdArgs[1])
	dest := string(cmdArgs[2])

	//192.168....:6379
	srcPeer := cluster.peerPicker.PickNode(src)
	descPeer := cluster.peerPicker.PickNode(dest)
	if srcPeer != descPeer {
		return reply.MakeErrReply("ERR rename must within on peer")
	}

	return cluster.relay(srcPeer, c, cmdArgs)
}
