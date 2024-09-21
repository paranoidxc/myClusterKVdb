package cluster

import "myredis/interface/resp"

// del k1 k2 k3
func ping(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	//return cluster.ClusterExec(c, cmdArgs)
	return cluster.db.Exec(c, cmdArgs)
}
