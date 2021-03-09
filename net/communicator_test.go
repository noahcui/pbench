package net

import (
	"encoding/gob"
	"github.com/acharapko/pbench/idservice"
	"github.com/acharapko/pbench/log"
	"testing"
)

var id1 = idservice.NewIDFromString("1.1")
var id2 = idservice.NewIDFromString("1.2")

var Address = map[idservice.ID]string{
	id1: "127.0.0.1:1736",
	id2: "127.0.0.1:1737",
}

type MSG struct {
	I int
	S string
}

func testServerCommunication(transport string, t *testing.T) {
	gob.Register(MSG{})
	log.Debugf("test")
	address := make(map[idservice.ID]string)
	if transport != "" {
		address[id1] = transport + "://" + Address[id1]
		address[id2] = transport + "://" + Address[id2]
	} else {
		address[id1] = Address[id1]
		address[id2] = Address[id2]
	}

	sock2 := NewCommunicator(id2, address)
	defer sock2.Close()

	sock1 := NewCommunicator(id1, address)
	defer sock1.Close()

	var send interface{}
	send = MSG{42, "hello"}
	go sock1.Broadcast(send)

	var recv interface{}
	recv = sock2.Recv()
	log.Debugf("test recv in sock2: %v", recv)
	if send.(MSG) != recv.(MSG) {
		t.Error("expect recv equal to send message")
	}

	send = MSG{43, "hello2"}
	go sock2.Send(id1, send)

	var recv2 interface{}
	recv2 = sock1.Recv()
	log.Debugf("test recv in sock1: %v", recv2)
	if send.(MSG) != recv2.(MSG) {
		t.Error("expect recv equal to send message")
	}
}

func testClientCommunication(transport string, t *testing.T) {
	gob.Register(MSG{})
	log.Debugf("test")
	address := make(map[idservice.ID]string)
	if transport != "" {
		address[id1] = transport + "://" + Address[id1]
		address[id2] = transport + "://" + Address[id2]
	} else {
		address[id1] = Address[id1]
		address[id2] = Address[id2]
	}

	server := NewCommunicator(id1, address)
	defer server.Close()

	client := NewClientCommunicator(address)

	var send interface{}
	send = MSG{I:42, S:"hello"}
	go client.Send(id1, send)

	var recv interface{}
	recv = server.Recv()
	log.Debugf("test recv in server: %v", recv)

	cmw := recv.(ClientMsgWrapper)
	if send.(MSG) != cmw.Msg {
		t.Error("expect recv equal to send message")
	}

	send = MSG{43, "hello2"}
	cmw.Reply(send)

	recv = client.Recv()
	log.Debugf("test recv in client: %v", recv)
}

func testClientCommunicationMultipleClients(transport string, t *testing.T) {
	gob.Register(MSG{})
	log.Debugf("test")
	address := make(map[idservice.ID]string)
	if transport != "" {
		address[id1] = transport + "://" + Address[id1]
		address[id2] = transport + "://" + Address[id2]
	} else {
		address[id1] = Address[id1]
		address[id2] = Address[id2]
	}

	server := NewCommunicator(id1, address)
	defer server.Close()

	client1 := NewClientCommunicator(address)
	client2 := NewClientCommunicator(address)

	var send1 interface{}
	send1 = MSG{I:42, S:"hello_client1"}
	go client1.Send(id1, send1)

	var send2 interface{}
	send2 = MSG{I:42, S:"hello_client2"}
	go client2.Send(id1, send2)

	var recv interface{}
	recv = server.Recv()
	log.Debugf("test recv in server: %v", recv)

	recv = server.Recv()
	log.Debugf("test recv in server: %v", recv)

	/*cmw := recv.(ClientMsgWrapper)
	if send1.(MSG) != cmw.Msg {
		t.Error("expect recv equal to send message")
	}

	send = MSG{43, "hello2"}
	cmw.Reply(send)

	recv = client1.Recv()
	log.Debugf("test recv in client: %v", recv)*/
}

func TestCommunicatorServer(t *testing.T) {
	//testServerCommunication("chan", t)
	testServerCommunication("tcp", t)
	//testServerCommunication("udp", t)
}

func TestCommunicatorClient(t *testing.T) {
	//testClientCommunication("tcp", t)
	testClientCommunicationMultipleClients("tcp", t)
}
