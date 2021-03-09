package node

import (
	"github.com/acharapko/pbench/cfg"
	"github.com/acharapko/pbench/db"
	"github.com/acharapko/pbench/idservice"
	"github.com/acharapko/pbench/log"
	"github.com/acharapko/pbench/net"
	"github.com/acharapko/pbench/retrolog"
	"net/http"
	"reflect"
	"sync"
)

// Node is the primary access point for every replica
// it includes networking, state machine and RESTful API server
type Node interface {
	net.Communication
	db.Database
	ID() idservice.ID
	Run()
	Retry(r net.Request)
	Forward(id idservice.ID, r net.Request)
	Register(m interface{}, f interface{})
	HandleMsg(m interface{})
}

// node implements Node interface
type node struct {
	id idservice.ID

	net.Communication
	db.Database
	MessageChan chan interface{}
	handles     map[string]reflect.Value
	server      *http.Server

	recvCount	int

	sync.RWMutex
	forwards map[string]*net.Request
}

// NewNode creates a new Node object from configuration
func NewNode(id idservice.ID) Node {
	if cfg.GetConfig().UseRetroLog {
		retrolog.Retrolog = retrolog.NewRetroLog("pbench", int(id), "logs/", 100, false)
		retrolog.Retrolog.CreateTimerSet("sentM", 5)
		retrolog.Retrolog.CreateTimerSet("recvM", 5)
	}
	return &node{
		id:            id,
		Communication: net.NewCommunicator(id, cfg.GetConfig().Addrs),
		Database:      db.NewDatabase(),
		MessageChan:   make(chan interface{}, cfg.GetConfig().ChanBufferSize),
		handles:       make(map[string]reflect.Value),
		forwards:      make(map[string]*net.Request),
		recvCount:     0,
	}
}

func (n *node) ID() idservice.ID {
	return n.id
}

func (n *node) Retry(r net.Request) {
	log.Debugf("node %v retry request %v", n.id, r)
	n.MessageChan <- r
}

// Register a handle function for each message type
func (n *node) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 1 || fn.Type().In(0) != t {
		panic("register handle function error")
	}
	n.handles[t.String()] = fn
}

// Run start and run the node
func (n *node) Run() {
	log.Infof("node %v start running with %d handles", n.id, len(n.handles))
	if len(n.handles) > 0 {
		go n.handle()
		n.recv()
	}
	//n.http()
}

// recv receives messages from socket and pass to message channel
func (n *node) recv() {
	for {
		m := n.Recv()
		n.MessageChan <- m
	}
}

// handle receives messages from message channel and calls handle function using refection
func (n *node) handle() {
	for {
		msg := <-n.MessageChan
		n.HandleMsg(msg)
	}
}

func (n *node) HandleMsg(msg interface{}) {
	if msg != nil {
		v := reflect.ValueOf(msg)
		name := v.Type().String()
		f, exists := n.handles[name]
		if !exists {
			log.Fatalf("no registered handle function for message type %v. Registered handles: %v", name, n.handles)
		}
		f.Call([]reflect.Value{v})
	}
}

/*
func (n *node) Forward(idservice NodeId, m Request) {
	key := m.Commands.Key
	url := config.HTTPAddrs[idservice] + "/" + strconv.Itoa(int(key))

	log.Debugf("Node %v forwarding %v to %s", n.NodeId(), m, idservice)

	method := http.MethodGet
	var body io.Reader
	if !m.Commands.IsRead() {
		method = http.MethodPut
		body = bytes.NewBuffer(m.Commands.Value)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Error(err)
		return
	}
	req.Header.Set(HTTPClientID, string(n.idservice))
	req.Header.Set(HTTPCommandID, strconv.Itoa(m.Commands.CommandID))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error(err)
		m.Reply(Reply{
			Commands: m.Commands,
			Err:     err,
		})
		return
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		b, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Error(err)
		}
		m.Reply(Reply{
			Commands: m.Commands,
			Value:   Value(b),
		})
	} else {
		m.Reply(Reply{
			Commands: m.Commands,
			Err:     errors.New(res.Status),
		})
	}
}
*/

func (n *node) Forward(id idservice.ID, m net.Request) {
	log.Debugf("Node %v forwarding %v to %s", n.ID(), m, id)
	m.NodeID = n.id
	n.Lock()
	n.forwards[m.Command.String()] = &m
	n.Unlock()
	n.Send(id, m)
}
