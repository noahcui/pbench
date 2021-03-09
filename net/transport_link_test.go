package net

import (
	"encoding/gob"
	"github.com/acharapko/pbench/idservice"
	"testing"
)

func TestTransport(t *testing.T) {
	// use message type A and B from codec_test.go
	gob.Register(A{})
	gob.Register(B{})

	id1 := idservice.NewIDFromString("1.1")
	id2 := idservice.NewIDFromString("1.2")

	server := NewTransportLink("tcp://127.0.0.1:1735", id1, false)
	server.Listen(nil)

	client := NewTransportLink("tcp://127.0.0.1:1735", id2, false)
	client.Dial()

	client.Send(A{
		I: 42,
		S: "hello world",
		B: true,
	})

	client.Send(B{
		S: "hello gob",
	})

	m := server.Recv()
	switch m.(type) {
	case A:
		t.Logf("recv message %+v", m)
	default:
		t.Error()
	}

	m = server.Recv()
	switch m.(type) {
	case B:
		t.Logf("recv message %+v", m)
	default:
		t.Error()
	}

	client.Send(A{
		I: 105,
		S: "new world",
	})

	m = server.Recv()
	switch m.(type) {
	case A:
		t.Logf("recv message %+v", m)
	default:
		t.Error()
	}
}
