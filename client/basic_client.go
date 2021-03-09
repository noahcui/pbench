package client

import (
	"errors"
	"github.com/acharapko/pbench/cfg"
	"github.com/acharapko/pbench/db"
	"github.com/acharapko/pbench/idservice"
	"github.com/acharapko/pbench/log"
	"github.com/acharapko/pbench/net"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type BasicClient struct {
	ClientId		idservice.ID
	PreferredNodeId idservice.ID
	Communication   net.Communication
	cmdId           int32

	messageChan chan interface{}
	handles     map[string]reflect.Value

	pendingResponses map[int]chan interface{}  // table of pending responses channel. key is cmd id still waiting for
	pendingMutex sync.RWMutex

}

func NewClient(preferredNodeId idservice.ID, clientId idservice.ID) *BasicClient {
	log.Debugf("Starting new client with preferred node id: %v", preferredNodeId)
	clientCommunicator := net.NewClientCommunicator(cfg.GetConfig().Addrs)
	client := BasicClient{
		ClientId:		  clientId,
		PreferredNodeId:  preferredNodeId,
		Communication:    clientCommunicator,
		cmdId:            0,
		messageChan:      make(chan interface{}, cfg.GetConfig().ChanBufferSize),
		handles:          make(map[string]reflect.Value),
		pendingResponses: make(map[int]chan interface{}),
	}

	client.Register(net.Reply{}, client.handleReplies)

	go client.handle()
	return &client
}

func (c *BasicClient) Get(k db.Key) (db.Value, error) {
	cmdId := c.NextCmdID()
	cmd := db.Command{
		Key:       k,
		Value:     nil,
		Type:	   db.CmdRead,
		ClientID:  c.ClientId,
		CommandID: int(cmdId),
	}

	replyMsg := c.SendCommand(cmd)

	v := replyMsg.Value
	var err error = nil
	if replyMsg.HasErr {
		err = errors.New(replyMsg.ErrStr)
	}
	return v, err
}

func (c *BasicClient) Put(k db.Key, v db.Value) error {
	cmdId := c.NextCmdID()
	cmd := db.Command{
		Key:       k,
		Value:     v,
		Type:	   db.CmdWrite,
		ClientID:  c.ClientId,
		CommandID: int(cmdId),
	}

	replyMsg := c.SendCommand(cmd)

	var err error = nil
	if replyMsg.HasErr {
		err = errors.New(replyMsg.ErrStr)
	}
	return err
}

func (c *BasicClient) SendCommand(cmd db.Command) net.Reply {
	var req net.Request
	var respChan chan interface{}
	respChan = make(chan interface{}, 1)
	to := c.PreferredNodeId

	if c.PreferredNodeId == 0 {
		ids := c.Communication.GetKnownIDs()
		to = ids[rand.Intn(len(ids))]
	}

	log.Debugf("Sending GET{%v} to node %v", cmd, to)

	req = net.Request{
		Command:    cmd,
		Properties: nil,
		Timestamp:  time.Now().UnixNano(),
		NodeID:     0,
	}

	c.AddPendingChanel(respChan, req.Command.CommandID)

	c.Communication.Send(to, req)

	respMsg := <-respChan

	replyMsg := respMsg.(net.Reply)

	return replyMsg
}

// Register a handle function for each message type
func (c *BasicClient) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 1 || fn.Type().In(0) != t {
		panic("register handle function error")
	}
	c.handles[t.String()] = fn
}

func (c *BasicClient) NextCmdID() int {
	return int(atomic.AddInt32(&c.cmdId, 1))
}

func (c *BasicClient) AddPendingChanel(pending chan interface{}, cmdId int) {
	c.pendingMutex.Lock()
	defer c.pendingMutex.Unlock()
	c.pendingResponses[cmdId] = pending
}


// handle receives messages from message channel and calls handle function using refection
func (c *BasicClient) handle() {
	for {
		msg := c.Communication.Recv()
		go c.HandleMsg(msg)
	}
}

func (c *BasicClient) HandleMsg(msg interface{}) {
	log.Debugf("Handling msg: %v", msg)
	if msg != nil {
		v := reflect.ValueOf(msg)
		name := v.Type().String()
		f, exists := c.handles[name]
		if !exists {
			log.Fatalf("no registered handle function for message type %v", name)
		}
		f.Call([]reflect.Value{v})
	}
}

func (c *BasicClient)handleReplies(m net.Reply) {
	c.pendingMutex.RLock()
	defer c.pendingMutex.RUnlock()
	if respChan, exists := c.pendingResponses[m.Command.CommandID]; exists {
		respChan <- m
	}
}



