package cluster

import (
	"context"
	pool "github.com/jolestar/go-commons-pool/v2"
	"myredis/config"
	database2 "myredis/database"
	"myredis/interface/database"
	"myredis/interface/resp"
	"myredis/lib/consistenthash"
	"myredis/lib/logger"
	"myredis/resp/reply"
	"strings"
)

type ClusterDatabase struct {
	self           string
	nodes          []string
	peerPicker     *consistenthash.NodeMap
	peerConnection map[string]*pool.ObjectPool
	db             database.Database
}

func MakeClusterDatabase() *ClusterDatabase {
	c := &ClusterDatabase{
		self:           config.Properties.Self,
		db:             database2.NewStandaloneDatabase(),
		peerPicker:     consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}
	nodes := make([]string, 0, 1+len(config.Properties.Peers))
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)
	c.peerPicker.AddNode(nodes...)

	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		c.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}

	//logger.Info("c.peerConnection", c.peerConnection)
	c.nodes = nodes

	return c
}

type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply

var router = makeRouter()

func (cluster *ClusterDatabase) Exec(c resp.Connection, args [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			result = reply.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(args[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		result = reply.MakeErrReply("not supported cmd")
	}

	result = cmdFunc(cluster, c, args)

	return
}

func (cluster *ClusterDatabase) AfterClientClose(c resp.Connection) {
	cluster.db.AfterClientClose(c)
}

func (cluster *ClusterDatabase) Close() {
	cluster.db.Close()
}
