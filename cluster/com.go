package cluster

import (
	"context"
	"errors"
	"myredis/interface/resp"
	"myredis/lib/logger"
	"myredis/lib/utils"
	"myredis/resp/client"
	"myredis/resp/reply"
	"strconv"
)

func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	factory, ok := cluster.peerConnection[peer]
	if !ok {
		logger.Info("cluster peerConnection", cluster.peerConnection)
		logger.Info("peer", peer)
		return nil, errors.New("getPeerClient connection not found")
	}

	object, err := factory.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}

	c, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("wrong type")
	}

	return c, err
}

func (cluster *ClusterDatabase) returnPeerClient(peer string, c *client.Client) error {
	factory, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection not found")
	}

	return factory.ReturnObject(context.Background(), c)
}

func (cluster *ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	if peer == cluster.self {
		return cluster.db.Exec(c, args)
	}

	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient)
	}()
	peerClient.Send(utils.ToCmdLine("select", strconv.Itoa(c.GetDBIndex())))
	return peerClient.Send(args)
}

func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
	results := make(map[string]resp.Reply)

	for _, node := range cluster.nodes {
		result := cluster.relay(node, c, args)
		results[node] = result
	}

	return results
}
