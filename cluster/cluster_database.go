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
	"myredis/lib/utils"
	"myredis/resp/reply"
	"strconv"
	"strings"
)

type ClusterDatabase struct {
	self              string
	lead              string
	nodes             []string
	peerPicker        *consistenthash.NodeMap
	peerConnection    map[string]*pool.ObjectPool
	db                database.Database
	clusterNodes      []string
	clusterPicker     *consistenthash.NodeMap
	clusterConnection map[string]*pool.ObjectPool
}

func MakeClusterDatabase() *ClusterDatabase {
	c := &ClusterDatabase{
		self:              config.Properties.Self,
		lead:              config.Properties.Lead,
		db:                database2.NewStandaloneDatabase(),
		peerPicker:        consistenthash.NewNodeMap(nil),
		peerConnection:    make(map[string]*pool.ObjectPool),
		clusterPicker:     consistenthash.NewNodeMap(nil),
		clusterConnection: make(map[string]*pool.ObjectPool),
	}
	// 分布式
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

	// 集群
	clusterNodes := make([]string, 0, 1+len(config.Properties.Clusters))
	for _, cluster := range config.Properties.Clusters {
		clusterNodes = append(clusterNodes, cluster)
	}
	clusterNodes = append(clusterNodes, config.Properties.Self)
	c.clusterPicker.AddNode(clusterNodes...)
	for _, cluster := range config.Properties.Clusters {
		c.clusterConnection[cluster] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: cluster,
		})
	}
	c.clusterNodes = clusterNodes

	return c
}

type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply

var router = makeRouter()

func (cluster *ClusterDatabase) ClusterExec(c resp.Connection, cmdArgs [][]byte) (result resp.Reply) {
	// 执行
	logger.Info("ClusterExec", "self", cluster.self, "lead", cluster.lead, "cmdArgs", cmdArgs[0])
	result = cluster.db.Exec(c, cmdArgs)
	if cluster.self == cluster.lead {
		// 是 lead 然后群发
		// TODO 有些命令并不需要转发 如 get
		for _, peer := range cluster.clusterNodes {
			if peer == cluster.self {
				continue
			}
			go func(peer string) {
				logger.Info("peer", peer)
				peerClient, err := cluster.getClusteClient(peer)
				if err != nil {
					return
					//return reply.MakeErrReply(err.Error())
				}
				defer func() {
					_ = cluster.returnClusterClient(peer, peerClient)
				}()
				peerClient.Send(utils.ToCmdLine("select", strconv.Itoa(c.GetDBIndex())))
				peerClient.Send(cmdArgs)
			}(peer)
		}
	}
	return
}

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
