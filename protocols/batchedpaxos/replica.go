package batchedpaxos

import (
	"github.com/acharapko/pbench/db"
	"github.com/acharapko/pbench/idservice"
	"github.com/acharapko/pbench/log"
	"github.com/acharapko/pbench/net"
	"github.com/acharapko/pbench/node"
	"sync"
	"time"
)

//var ephemeralLeader = flag.Bool("ephemeral_leader", false, "unstable leader, if true paxos replica try to become leader instead of forward requests to current leader")
//var read = flag.String("read", "", "read from \"leader\", \"quorum\" or \"any\" replica")

var ephemeralLeader = false

const (
	PropertyHeaderSlot   = "Slot"
	PropertyHeaderBallot = "Ballot"
	PropertyExecute      = "Execute"
	PropertyInProgress   = "Inprogress"
)

type reply func(m interface{})

type pendingBatch struct {
	requests []*net.Request
	replyFuncs []reply
	Commands []db.Command

	sync.RWMutex
}

func NewPendingBatch() *pendingBatch {
	return &pendingBatch{
		requests:   make([]*net.Request, 0),
		replyFuncs: make([]reply, 0),
		Commands:   make([]db.Command, 0),
	}
}

func (b *pendingBatch) add(req *net.Request, replyFunc reply) {
	b.Lock()
	defer b.Unlock()
	b.requests = append(b.requests, req)
	b.Commands = append(b.Commands, req.Command)
	b.replyFuncs = append(b.replyFuncs, replyFunc)
}

// Replica for one Paxos instance
type Replica struct {
	node.Node
	cleanupMultiplier uint64
	pendingRequests *pendingBatch // phase 1 pending requests

	*Paxos
}

// NewReplica generates new Paxos replica
func NewReplica(id idservice.ID) *Replica {
	r := new(Replica)
	r.Node = node.NewNode(id)
	r.Paxos = NewPaxos(r)
	r.cleanupMultiplier = 3
	r.pendingRequests = NewPendingBatch()
	r.Register(net.ClientMsgWrapper{}, r.handleClientMsgWrapper)
	r.Register(P1a{}, r.HandleP1a)
	r.Register(P1b{}, r.HandleP1b)
	r.Register(P2a{}, r.HandleP2a)
	r.Register(P2b{}, r.HandleP2b)
	r.Register(P3{}, r.HandleP3)

	go r.startTicker()

	return r
}

//*********************************************************************************************************************
// Timer for all timed events, such as timeouts and log clean ups
//*********************************************************************************************************************
func (r *Replica) startTicker() {
	var ticks uint64 = 0
	for now := range time.Tick(10 * time.Millisecond) {
		// log cleanup
		ticks++
		if ticks % r.cleanupMultiplier == 0 {
			r.CleanupLog()
		}

		if r.IsLeader() {
			r.P2a(r.pendingRequests)
			r.pendingRequests = NewPendingBatch()
			r.P3Sync(now.UnixNano() / int64(time.Millisecond))
		} else if len(r.pendingRequests.Commands) > 0 {
			// we are not a leader, but have
		}
	}
}

func (r *Replica) handleClientMsgWrapper(cmw net.ClientMsgWrapper) {
	log.Debugf("Replica %s received %v\n", r.ID(), cmw)
	switch cmw.Msg.(type) {
	case net.Request:
		m := cmw.Msg.(net.Request)

		if ephemeralLeader || r.Paxos.IsLeader() || r.Paxos.Ballot() == 0 {
			r.HandleRequest(m, cmw.Reply)
		} else {
			go r.Forward(r.Paxos.Leader(), m)
		}
	}
}


// HandleQuorumRequest handles request and start phase 1 or phase 2
func (r *Replica) HandleRequest(req net.Request, reply reply) {
	// log.Debugf("Replica %s received %v\n", p.NodeId(), r)
	// build a batch
	r.pendingRequests.add(&req, reply)

	if !r.active {
		// current phase 1 pending
		if r.ballot.ID() != r.ID() {
			r.P1a()
		}
	} /*else {
		r.P2a(&req, reply)
	}*/
}
