package aof

import (
	"io"
	"myredis/config"
	"myredis/interface/database"
	"myredis/lib/logger"
	"myredis/lib/utils"
	"myredis/resp/connection"
	"myredis/resp/parser"
	"myredis/resp/reply"
	"os"
	"strconv"
)

type CmdLine = [][]byte

const aofBufferSize = 1 << 16

type payLoad struct {
	cmdLine CmdLine
	dbIndex int
}

type AofHandler struct {
	database    database.Database
	aofChan     chan *payLoad
	aofFile     *os.File
	aofFilename string
	currentDB   int
}

// new aofHandler
func NewAofHandler(database database.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.database = database
	handler.LoadAof()
	logger.Info("loadAof File done")

	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return nil, err
	}

	handler.aofFile = aofFile
	handler.aofChan = make(chan *payLoad, aofBufferSize)
	go func() {
		handler.handleAof()
	}()

	return handler, nil
}

// add payload -> aofChan
func (handler *AofHandler) AddAof(dbIndex int, cmd CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payLoad{
			cmdLine: cmd,
			dbIndex: dbIndex,
		}
	}
}

// handleAof payload <- aofChan
// 落盘
func (handler *AofHandler) handleAof() {
	handler.currentDB = 0
	for p := range handler.aofChan {
		if p.dbIndex != handler.currentDB {
			//select 3
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Error(err)
				continue
			}
			handler.currentDB = p.dbIndex
		}
		// cmdLine
		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Error(err)
			continue
		}
	}
}

// LoadAof
// 从磁盘文件读到内存中
func (handler *AofHandler) LoadAof() {
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Error(err)
		return
	}
	defer file.Close()

	ch := parser.ParseStream(file)
	fakeConn := &connection.Connection{}
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error(err)
			continue
		}

		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}

		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("need multi bulk")
			continue
		}

		rep := handler.database.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(rep) {
			logger.Error(rep)
		}
	}
}
