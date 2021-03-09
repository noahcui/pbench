package net

import (
	"errors"
	"fmt"
	"github.com/acharapko/pbench/cfg"
	"github.com/acharapko/pbench/hlc"
	"github.com/acharapko/pbench/idservice"
	"github.com/acharapko/pbench/retrolog"
	"github.com/acharapko/pbench/util"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/acharapko/pbench/log"
	"sync"
)

// Communication integrates all networking interface and fault injections
type Communication interface {

	AddAddress(id idservice.ID, addr string)

	GetAddresses() map[idservice.ID]string

	GetKnownIDs() []idservice.ID

	// Send put message to outbound queue
	Send(to idservice.ID, m interface{})

	// MulticastZone send msg to all nodes in the same site
	MulticastZone(zone int, m interface{})

	// MulticastQuorum sends msg to random number of nodes
	MulticastQuorum(quorum int, m interface{})

	// Broadcast send to all peers
	Broadcast(m interface{})

	// BroadcastOneDifferent sends m1 to one random peer, and m2 to the rest
	BroadcastOneDifferent(m1 interface{}, m2 interface{})

	// Recv receives a message
	Recv() interface{}

	Close()

	// Fault injection
	Drop(id idservice.ID, t int)             // drops every message send to NodeId last for t seconds
	Slow(id idservice.ID, d int, t int)      // delays every message send to NodeId for d ms and last for t seconds
	Flaky(id idservice.ID, p float64, t int) // drop message by chance p for t seconds
	Crash(t int)                             // node crash for t seconds
}

type TransportLinkManager interface {
	// Adds transportLink to an existing pool of all transports
	AddTransportLink(t TransportLink, to idservice.ID)
}

type communicator struct {
	id        		idservice.ID		// this node's NodeId
	isClient  		bool // or if this communicator is on the client side and has no server
	addresses 		map[idservice.ID]string
	nodes     		map[idservice.ID]TransportLink
	clientListener  TransportLink
	ids       		[]idservice.ID

	crash bool
	drop  map[idservice.ID]bool
	slow  map[idservice.ID]int
	flaky map[idservice.ID]float64

	sendLock map[idservice.ID]chan bool

	msgid		int64

	recv  		chan interface{}  // receive channel from all transports

	sync.RWMutex

	sentCount	int
}

// NewCommunicator return Communication interface instance given self NodeId, node list, transportLink and codec name
func NewCommunicator(nodeId idservice.ID, addrs map[idservice.ID]string) Communication {
	initMsgId := int64(nodeId) << 32
	communicator := &communicator{
		id:        nodeId,
		isClient:  false,
		addresses: addrs,
		nodes:     make(map[idservice.ID]TransportLink),
		ids:       make([]idservice.ID, 0),
		crash:     false,
		drop:      make(map[idservice.ID]bool),
		slow:      make(map[idservice.ID]int),
		flaky:     make(map[idservice.ID]float64),
		sendLock:  make(map[idservice.ID]chan bool),
		recv:	   make(chan interface{}, cfg.GetConfig().ChanBufferSize),
		msgid:	   initMsgId,
		sentCount: 0,
	}

	communicator.ids = communicator.refreshIds()
	communicator.nodes[nodeId] = NewTransportLink(addrs[nodeId], nodeId, false)
	communicator.nodes[nodeId].Listen(communicator)
	go communicator.receiveFromLink(communicator.nodes[nodeId])

	clientListenAddr, err := getClientAddressFromServer(addrs[nodeId])
	if err == nil {
		communicator.clientListener = NewTransportLink(clientListenAddr, nodeId, true)
		communicator.clientListener.Listen(communicator)
		go communicator.receiveFromLink(communicator.clientListener)
	} else {
		log.Errorf("Error getting client address from server address: %v", err)
	}

	return communicator
}

// NewCommunicator return Communication interface instance given self NodeId, node list, transportLink and codec name
func NewClientCommunicator(addrs map[idservice.ID]string) Communication {
	clientAddrs := make(map[idservice.ID]string, len(addrs))
	for id, addr := range addrs {
		clientListenAddr, err := getClientAddressFromServer(addr)
		if err == nil {
			clientAddrs[id] = clientListenAddr
		} else {
			log.Errorf("Error getting client address from server address: %v", err)
		}
	}

	communicator := &communicator{
		isClient:  true,
		addresses: clientAddrs,
		nodes:     make(map[idservice.ID]TransportLink),
		crash:     false,
		drop:      make(map[idservice.ID]bool),
		slow:      make(map[idservice.ID]int),
		flaky:     make(map[idservice.ID]float64),
		sendLock:  make(map[idservice.ID]chan bool),
		recv:	   make(chan interface{}, cfg.GetConfig().ChanBufferSize),
		sentCount: 0,
	}

	communicator.ids = communicator.refreshIds()

	return communicator
}

// IDs returns all node ids
func (c *communicator) refreshIds() []idservice.ID {
	ids := make([]idservice.ID, 0)
	for id := range c.addresses {
		ids = append(ids, id)
		c.sendLock[id] = make(chan bool, 1)
		c.sendLock[id] <- true
	}
	return ids
}

func (c *communicator) AddAddress(id idservice.ID, addr string) {
	c.Lock()
	defer c.Unlock()
	c.addresses[id] = addr
	if existingAddr, exists := c.addresses[id]; exists {
		if existingAddr != addr {
			// address has change, close old Link and open new one
			c.nodes[id].Close()
			c.nodes[id] = nil
		}
	}
	c.ids = c.refreshIds()
}

func (c *communicator) GetAddresses() map[idservice.ID]string {
	c.RLock()
	defer c.RUnlock()
	return c.addresses
}

func (c *communicator) GetKnownIDs() []idservice.ID {
	c.RLock()
	defer c.RUnlock()
	return c.ids
}

// TransportLinkManager implementation
func (c *communicator) AddTransportLink(t TransportLink, to idservice.ID) {
	c.Lock()
	if c.nodes[to] != nil {
		c.nodes[to].Close()
	}
	c.nodes[to] = t

	if c.isClient {
		// the link is bi-directional for clients, so start receiving from it
		go c.receiveFromLink(t)
	}

	log.Debugf("Added %v to nodes", to)
	c.Unlock()
}

func (c *communicator) receiveFromLink(tr TransportLink) {
	for {
		c.recv <- tr.Recv()
	}
}

// Communication Implementation
func (c *communicator) Recv() interface{} {
	return <-c.recv
}

func (c *communicator) Send(to idservice.ID, m interface{}) {
	pm := c.wrapInProtocolMsg(m)
	c.send(to, pm)
	log.Debugf("sent %v to %v", m, to)
}

func (c *communicator) MulticastZone(zone int, m interface{}) {
	//log.Debugf("node %c broadcasting message %+v in zone %d", c.idservice, m, zone)
	pm := c.wrapInProtocolMsg(m)
	for id := range c.addresses {
		if id == c.id {
			continue
		}
		if id.Zone() == zone {
			c.send(id, pm)
		}
	}
}

func (c *communicator) MulticastQuorum(quorum int, m interface{}) {
	//log.Debugf("node %c multicasting message %+v for %d nodes", c.idservice, m, quorum)
	pm := c.wrapInProtocolMsg(m)
	i := 0
	for id := range c.addresses {
		if id == c.id {
			continue
		}
		c.send(id, pm)
		i++
		if i == quorum {
			break
		}
	}
}

func (c *communicator) Broadcast(m interface{}) {
	//log.Debugf("node %c broadcasting message %+v", c.idservice, m)
	pm := c.wrapInProtocolMsg(m)
	for id := range c.addresses {
		if id == c.id {
			continue
		}
		c.send(id, pm)
	}
}

func (c *communicator) BroadcastOneDifferent(m1 interface{}, m2 interface{}) {
	//log.Debugf("node %c broadcasting message %+v", c.idservice, m)
	oneSent := false
	pm := c.wrapInProtocolMsg(m1)
	for id := range c.addresses {
		if id == c.id {
			continue
		}
		c.send(id, pm)
		if !oneSent {
			oneSent = true
			tempHlc := pm.HlcTime
			pm = c.wrapInProtocolMsg(m2)
			pm.HlcTime = tempHlc
		}
	}
}

func (c *communicator) Close() {
	for _, t := range c.nodes {
		t.Close()
	}
	close(c.recv)
}

func (c *communicator) Drop(id idservice.ID, t int) {
	c.drop[id] = true
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		c.drop[id] = false
	}()
}

func (c *communicator) Slow(id idservice.ID, delay int, t int) {
	c.slow[id] = delay
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		c.slow[id] = 0
	}()
}

func (c *communicator) Flaky(id idservice.ID, p float64, t int) {
	c.flaky[id] = p
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		c.flaky[id] = 0
	}()
}

func (c *communicator) Crash(t int) {
	log.Infof("Crashing node %v for %d seconds", c.id, t)
	c.crash = true
	if t > 0 {
		timer := time.NewTimer(time.Duration(t) * time.Second)
		go func() {
			<-timer.C
			c.crash = false
			log.Infof("Restoring node %v after crash", c.id)
		}()
	}
}

// helpers
func getClientAddressFromServer(addr string) (string, error) {
	s := strings.Split(addr, ":")
	if len(s) != 3 {
		return addr, errors.New(fmt.Sprintf("Incorrect address specification for address %s", addr))
	}
	port, _ := strconv.Atoi(s[2])
	port += 1000
	return s[0] + ":" + s[1] + ":" + strconv.Itoa(port), nil
}

func (c *communicator) wrapInProtocolMsg(m interface{}) ProtocolMsg{
	ts := hlc.HLClock.Now()
	msgId := c.incrementMsgId()
	pm := ProtocolMsg{HlcTime: ts.ToInt64(), Msg: m, MsgId: msgId}
	return pm
}

func (c *communicator) recordInRetrolog(msgId int64, to idservice.ID) {
	if cfg.GetConfig().UseRetroLog {
		rqlstruct := retrolog.NewRqlStruct(nil).AddVarInt("mid", msgId).AddVarInt32("to", int(to))
		retrolog.Retrolog.StartTx().AppendSetStruct("sentM", rqlstruct)
		c.Lock()
		c.sentCount++
		retrolog.Retrolog.AppendVarInt32("sentCount", c.sentCount).Commit()
		c.Unlock()
	}
}

func (c *communicator) send(to idservice.ID, m interface{}) {
	if c.crash {
		return
	}

	if c.drop[to] {
		return
	}

	if p, ok := c.flaky[to]; ok && p > 0 {
		if rand.Float64() < p {
			return
		}
	}

	<-c.sendLock[to]
	t, exists := c.nodes[to]
	if !exists || t.Mode() == ModeClosed {
		address, ok := c.addresses[to]
		if !ok {
			log.Errorf("communicator does not have address of node %c", to)
			return
		}
		t = NewTransportLink(address, c.id, c.isClient)
		log.Debugf("Dialing %v", to)
		err := util.Retry(t.Dial, 100, time.Duration(50)*time.Millisecond)
		if err == nil {
			c.AddTransportLink(t, to)
		} else {
			panic(err)
		}
	}
	c.sendLock[to] <- true

	if delay, ok := c.slow[to]; ok && delay > 0 {
		timer := time.NewTimer(time.Duration(delay) * time.Millisecond)
		go func() {
			<-timer.C
			t.Send(m)
		}()
		return
	}

	t.Send(m)
}

func (c *communicator) incrementMsgId() int64 {
	c.Lock()
	defer c.Unlock()
	c.msgid++
	return c.msgid
}