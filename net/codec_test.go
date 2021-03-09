package net

import (
	"bytes"
	"encoding/gob"
	"testing"
)

type A struct {
	I int
	S string
	B bool
}

type B struct {
	S string
}

func TestCodecGob(t *testing.T) {
	gob.Register(A{})
	gob.Register(B{})
	var send interface{}
	var recv interface{}

	buf := new(bytes.Buffer)
	c := NewCodec("gob", buf)

	send = A{1, "a", true}

	c.Encode(&send)
	c.Decode(&recv)
	if send.(A) != recv.(A) {
		t.Errorf("expect send %v and recv %v to be euqal", send, recv)
	}

	send = B{"test"}

	c.Encode(&send)
	c.Decode(&recv)
	if send.(B) != recv.(B) {
		t.Errorf("expect send %v and recv %v to be euqal", send, recv)
	}
}

func TestCodecGobHandshakeMsg(t *testing.T) {
	gob.Register(HandshakeMsg{})
	var send interface{}
	var recv interface{}

	buf := new(bytes.Buffer)
	c := NewCodec("gob", buf)

	send = HandshakeMsg{IsClient:false, NodeId:id1}

	c.Encode(&send)
	c.Decode(&recv)
	if send.(HandshakeMsg) != recv.(HandshakeMsg) {
		t.Errorf("expect send %v and recv %v to be euqal", send, recv)
	}


}

