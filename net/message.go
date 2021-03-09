package net

import "C"
import (
	"encoding/gob"
	"fmt"
	"github.com/acharapko/pbench/db"
	"github.com/acharapko/pbench/hlc"
	"github.com/acharapko/pbench/idservice"
	"github.com/acharapko/pbench/log"
)

func init() {
	gob.Register(HandshakeMsg{}) // initial handshake
	gob.Register(Request{}) // from client to server
	gob.Register(Reply{}) // from server to client
	gob.Register(Read{})
	gob.Register(ReadReply{})
	gob.Register(ProtocolMsg{}) // wrapper for messages when need HLC in the system
}

/***************************
 * Protocol Related Messages
 ***************************/

// Initial Handshake
type HandshakeMsg struct {
	IsClient	bool // whether this is a client connecting, if not we should have a NodeId
	NodeId		idservice.ID
}

func (h HandshakeMsg) String() string {
	return fmt.Sprintf("HandshakeMsg {isClient=%t, NodeId=%v}",h.IsClient, h.NodeId)
}

// generic protocol msg with HLC
type ProtocolMsg struct {
	HlcTime 	int64
	MsgId		int64
	Msg         interface{}
}

func (p ProtocolMsg) String() string {
	return fmt.Sprintf("ProtocolMsg {msgid=%d, hlc=%d msg=%v}",p.MsgId, p.HlcTime, p.Msg)
}

/***************************
 * Client-Replica Messages *
 ***************************/

// generic client protocol msg with HLC
type ClientMsgWrapper struct {
	Msg         interface{}
	Timestamp   hlc.Timestamp
	C           chan interface{} // reply channel created by request receiver
}

// Reply replies to current client session
func (c *ClientMsgWrapper) Reply(reply interface{}) {
	c.C <- reply
}

func (c *ClientMsgWrapper) SetReplier(encoder *gob.Encoder) {
	c.C = make(chan interface{}, 1)
	go func(r *ClientMsgWrapper) {
		ts := hlc.HLClock.Now()
		var pm interface{}
		pm = ProtocolMsg{HlcTime: ts.ToInt64(), Msg: <-c.C}
		err := encoder.Encode(&pm)
		if err != nil {
			log.Errorf("Error replying to client: %v", err)
		}
	}(c)
}

func (c ClientMsgWrapper) String() string {
	return fmt.Sprintf("ClientMsgWrapper {msg=%v @ hlc=%v}", c.Msg, c.Timestamp)
}

// Request is client request with http response channel
type Request struct {
	Command    db.Command			// Commands for the request
	Properties map[string]string	// any additional metadata
	Timestamp  int64
	NodeID     idservice.ID 	// forward by node. This means the request is not directly from client and is forwarded
}

func (r Request) String() string {
	return fmt.Sprintf("Request {cmd=%v nid=%v}", r.Command, r.NodeID)
}

type Reply struct {
	Command    db.Command
	Value      db.Value
	NodeID	   idservice.ID
	Properties map[string]string
	Timestamp  int64
	HasErr     bool
	ErrStr     string
}

func (r Reply) String() string {
	return fmt.Sprintf("Reply {cmd=%v value=%x prop=%v, err=%v}", r.Command, r.Value, r.Properties, r.ErrStr)
}

// Read can be used as a special request that directly read the value of key without go through replication protocol in Replica
type Read struct {
	CommandID int
	Key       db.Key
}

func (r Read) String() string {
	return fmt.Sprintf("Read {cid=%d, key=%d}", r.CommandID, r.Key)
}

// ReadReply cid and value of reading key
type ReadReply struct {
	CommandID int
	Value     db.Value
}

func (r ReadReply) String() string {
	return fmt.Sprintf("ReadReply {cid=%d, val=%x}", r.CommandID, r.Value)
}
