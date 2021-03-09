package net

import (
	"bytes"
	"encoding/gob"
	"flag"
	"github.com/acharapko/pbench/cfg"
	"github.com/acharapko/pbench/hlc"
	"github.com/acharapko/pbench/idservice"
	"github.com/acharapko/pbench/retrolog"
	"net"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/acharapko/pbench/log"
)

type TransportMode int

const (
	ModeNone TransportMode = iota
	ModeListener // for server that listens for connections
	ModeDialer   // for client that dials the server
	ModeClosed
)

func (m TransportMode) String() string {
	return [...]string{"None", "Listener", "Dialer"}[m]
}

var scheme = flag.String("transportLink", "tcp", "transportLink scheme (tcp, udp, chan), default tcp")

// TransportLink = client & server
type TransportLink interface {
	// Scheme returns transportLink scheme
	Scheme() string

	// Mode returns whether this transportLink is a listener of a dialer
	Mode() TransportMode

	// Send sends message into t.send chan
	Send(interface{})

	// Recv waits for message from t.recv chan
	Recv() interface{}

	// Dial connects to remote server non-blocking once connected
	Dial() error

	// StartOutgoing starts sending any messages in outbound channel on an existing connection
	StartOutgoing(conn net.Conn)

	// Starts handling incoming messages from the remote endpoint
	StartIncoming(conn net.Conn, tm TransportLinkManager)

	// Listen waits for connections, non-blocking once listener starts
	Listen(tm TransportLinkManager)

	// Close closes send channel and stops listener
	Close()
}


func NewTransportLinkByNodeId(endpointNodeId idservice.ID, thisNodeId idservice.ID, isClientTransport bool) TransportLink {
	return NewTransportLink(cfg.GetConfig().Addrs[endpointNodeId], thisNodeId, isClientTransport)
}

// NewTransportLink creates new transportLink object with end point url, this node's NodeId and client flag
// for transports that dial to remote server, endpoint is address of the remote server
// for transports that listen for incoming connection, endpointAddr does not matter
// nodeId is this node
// isClientTransport should be set to true if this transportLink is on the client side and there is no nodeId
func NewTransportLink(endpointAddr string, nodeId idservice.ID, isClientTransport bool) TransportLink {
	if !strings.Contains(endpointAddr, "://") {
		endpointAddr = *scheme + "://" + endpointAddr
	}
	uri, err := url.Parse(endpointAddr)
	if err != nil {
		log.Fatalf("error parsing address %s : %s\n", endpointAddr, err)
	}

	transport := &transportLink{
		id:			nodeId,
		isClient: 	isClientTransport,
		mode:  		ModeNone,
		uri:   		uri,
		send:  		make(chan interface{}, cfg.GetConfig().ChanBufferSize),
		recv:  		make(chan interface{}, cfg.GetConfig().ChanBufferSize),
		close: 		make(chan struct{}),
	}

	switch uri.Scheme {
	case "tcp":
		t := new(tcp)
		t.transportLink = transport
		return t
	case "udp":
		t := new(udp)
		t.transportLink = transport
		return t
	default:
		log.Fatalf("unknown scheme %s", uri.Scheme)
	}
	return nil
}

type transportLink struct {
	id 				idservice.ID
	isClient		bool
	mode  			TransportMode
	uri   			*url.URL
	send  			chan interface{}
	recv  			chan interface{}
	close 			chan struct{}

	modeMux			sync.RWMutex
}

func (t *transportLink) Send(m interface{}) {
	t.send <- m
}

func (t *transportLink) Recv() interface{} {
	return <-t.recv
}

func (t *transportLink) Mode() TransportMode {
	return t.mode
}

func (t *transportLink) Close() {
	debug.PrintStack()
	if t.mode == ModeListener {
		log.Debugf("Closing channels in transportLink listening on %v at node %v", t.uri, t.id)
	} else {
		log.Debugf("Closing channels in transportLink to %v at node %v", t.uri, t.id)
	}
	close(t.send)
	close(t.recv)
	close(t.close)
	t.mode = ModeClosed
}

func (t *transportLink) Scheme() string {
	return t.uri.Scheme
}

/******************************
/*     TCP communication      *
/******************************/
type tcp struct {
	*transportLink
}

func (t *tcp) Dial() error {
	log.Debugf("Dialing %v from node %v", t.uri.Host, t.id)
	t.modeMux.Lock()
	defer t.modeMux.Unlock()
	if t.mode == ModeNone {
		conn, err := net.Dial(t.Scheme(), t.uri.Host)
		if err != nil {
			return err
		}
		t.mode = ModeDialer

		go t.StartOutgoing(conn)
		if t.isClient {
			// client dialer also listens to replies
			// unlike server communication which uses one directional links,
			// with clients we maintain two-way link
			go t.StartIncoming(conn, nil)
		}
	} else {
		log.Errorf("Trying to dial on transportLink in mode: %v", t.mode)
	}

	return nil
}

func (t *tcp) Listen(tm TransportLinkManager) {
	log.Debugf("start listening id: %v, port: %v", t.id, t.uri.Port())
	t.modeMux.Lock()
	defer t.modeMux.Unlock()
	if t.mode == ModeNone {
		listener, err := net.Listen("tcp", ":"+t.uri.Port())
		if err != nil {
			log.Fatal("TCP Listener error: ", err)
		}
		t.mode = ModeListener

		go func(listener net.Listener) {
			defer listener.Close()
			for {
				conn, err := listener.Accept()
				if err != nil {
					log.Error("TCP Accept error: ", err)
					continue
				}

				go t.StartIncoming(conn, tm)
			}
		}(listener)
	} else {
		log.Errorf("Trying to listen on transportLink in mode: %v", t.mode)
	}
}

/*func (t *tcp) StartHandshake(conn net.Conn) error {
	go t.StartIncoming(conn, nil)

	handshakeMsg := HandshakeMsg{
		IsClient: t.isClient,
		NodeId:   t.id,
	}

	var send interface{}
	send = handshakeMsg
	encoder := gob.NewEncoder(conn)
	err := encoder.Encode(&send)
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}*/


func (t *tcp) StartOutgoing(conn net.Conn) {
	encoder := gob.NewEncoder(conn)
	defer conn.Close()

	for m := range t.send {
		err := encoder.Encode(&m)
		if err != nil {
			log.Error(err)
		}
	}
}

func (t *tcp) StartIncoming(conn net.Conn, tm TransportLinkManager) {
	log.Debugf("Waiting for msgs from %v on node %v", conn.RemoteAddr(), t.id)
	decoder := gob.NewDecoder(conn)
	var encoder *gob.Encoder
	defer conn.Close()
	numFail := 0
	for {
		select {
		case <-t.close:
			log.Debugf("Closing reading connection from %v", conn.RemoteAddr())
			return
		default:
			var m interface{}
			err := decoder.Decode(&m)
			if err != nil {
				log.Error(err)
				numFail++
				time.Sleep(10 * time.Millisecond)
				if numFail >= 20 {
					return
				}
				continue
			}
			//log.Debugf("transportLink recv msg: %v", m)

			protocolMsg := m.(ProtocolMsg)
			hlcTS := *hlc.NewTimestampI64(protocolMsg.HlcTime)
			hlc.HLClock.Update(hlcTS)
			if cfg.GetConfig().UseRetroLog {
				retrolog.Retrolog.StartTx().AppendSetInt("recvM", protocolMsg.MsgId)
			}

			if t.isClient && t.mode == ModeListener {
				if encoder == nil {
					encoder = gob.NewEncoder(conn)
				}
				cmw := &ClientMsgWrapper{
					Msg:       protocolMsg.Msg,
					Timestamp: hlcTS,
					C:         nil,
				}
				cmw.SetReplier(encoder)
				//log.Debugf("adding msg %v to recv", *cmw)
				t.recv <- *cmw
			} else {
				//log.Debugf("adding msg %v to recv", protocolMsg.Msg)
				t.recv <- protocolMsg.Msg
			}
		}
	}
}

/******************************
/*     UDP communication      *
/******************************/
type udp struct {
	*transportLink
}

func (u *udp) Dial() error {
	addr, err := net.ResolveUDPAddr("udp", u.uri.Host)
	if err != nil {
		log.Fatal("UDP resolve address error: ", err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}

	go func(conn *net.UDPConn) {
		// packet := make([]byte, 1500)
		// w := bytes.NewBuffer(packet)
		w := new(bytes.Buffer)
		for m := range u.send {
			gob.NewEncoder(w).Encode(&m)
			_, err := conn.Write(w.Bytes())
			if err != nil {
				log.Error(err)
			}
			w.Reset()
		}
	}(conn)

	return nil
}

func (u *udp) Listen(tm TransportLinkManager) {
	addr, err := net.ResolveUDPAddr("udp", ":"+u.uri.Port())
	if err != nil {
		log.Fatal("UDP resolve address error: ", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal("UDP Listener error: ", err)
	}
	go func(conn *net.UDPConn) {
		packet := make([]byte, 1500)
		defer conn.Close()
		for {
			select {
			case <-u.close:
				return
			default:
				_, err := conn.Read(packet)
				if err != nil {
					log.Error(err)
					continue
				}
				r := bytes.NewReader(packet)
				var m interface{}
				gob.NewDecoder(r).Decode(&m)
				u.recv <- m
			}
		}
	}(conn)
}

func (t *udp) StartIncoming(conn net.Conn, tm TransportLinkManager) {

}

func (t *udp) StartOutgoing(conn net.Conn) {

}