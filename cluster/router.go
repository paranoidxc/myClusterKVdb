package cluster

import (
	"myredis/interface/resp"
)

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["exists"] = defaultFunc //exists k1
	routerMap["type"] = defaultFunc
	routerMap["set"] = defaultFunc
	routerMap["setnx"] = defaultFunc
	routerMap["get"] = defaultFunc
	routerMap["getset"] = defaultFunc
	routerMap["ping"] = ping
	routerMap["rename"] = rename
	routerMap["renamenx"] = rename
	routerMap["flushdb"] = flushdb
	routerMap["del"] = del
	routerMap["select"] = execSelect

	return routerMap
}

// get key
// set key val
func defaultFunc(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	// 转发取决于key的hash
	key := string(cmdArgs[1])
	peer := cluster.peerPicker.PickNode(key)
	//logger.Info("PickNode", key, peer)

	return cluster.relay(peer, c, cmdArgs)
}
