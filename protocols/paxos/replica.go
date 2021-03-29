package paxos

import (
	"flag"
	"github.com/acharapko/pbench/cfg"
	"github.com/acharapko/pbench/db"
	"github.com/acharapko/pbench/idservice"
	"github.com/acharapko/pbench/log"
	"github.com/acharapko/pbench/net"
	"github.com/acharapko/pbench/node"
	"strconv"
	"time"
)

var read = flag.String("read", "", "read from \"leader\", \"quorum\" or \"any\" replica")

const (
	PropertyHeaderSlot   = "Slot"
	PropertyHeaderBallot = "Ballot"
	PropertyExecute      = "Execute"
	PropertyInProgress   = "Inprogress"
)

// Replica for one Paxos instance
type Replica struct {
	node.Node
	cleanupMultiplier uint64
	*Paxos
}

// NewReplica generates new Paxos replica
func NewReplica(id idservice.ID) *Replica {
	r := new(Replica)
	r.Node = node.NewNode(id)
	r.Paxos = NewPaxos(r)
	r.cleanupMultiplier = 3
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
			r.P3Sync(now.UnixNano() / int64(time.Millisecond))
		}
	}
}

func (r *Replica) handleClientMsgWrapper(cmw net.ClientMsgWrapper) {
	log.Debugf("Replica %s received %v\n", r.ID(), cmw)
	switch cmw.Msg.(type) {
	case net.Request:
		m := cmw.Msg.(net.Request)
		if m.Command.Type == db.CmdRead && *read != "" {
			v, inProgress := r.readInProgress(m)
			reply := net.Reply{
				Command:    m.Command,
				Value:      v,
				Properties: make(map[string]string),
				Timestamp:  time.Now().Unix(),
			}
			reply.Properties[PropertyHeaderSlot] = strconv.Itoa(r.Paxos.slot)
			reply.Properties[PropertyHeaderBallot] = r.Paxos.ballot.String()
			reply.Properties[PropertyExecute] = strconv.Itoa(r.Paxos.execute - 1)
			reply.Properties[PropertyInProgress] = strconv.FormatBool(inProgress)
			cmw.Reply(reply)
			return
		}

		if !cfg.GetConfig().EphemeralLeader || r.Paxos.IsLeader() || r.Paxos.Ballot() == 0 {
			r.Paxos.HandleRequest(m, cmw.Reply)
		} else {
			go r.Forward(r.Paxos.Leader(), m)
		}
	}
}

func (r *Replica) readInProgress(m net.Request) (db.Value, bool) {
	// TODO
	// (1) last slot is read?
	// (2) entry in log over writen
	// (3) value is not overwriten command

	// is in progress
	for i := r.Paxos.slot; i >= r.Paxos.execute; i-- {
		entry, exist := r.Paxos.log[i]
		if exist && entry.command.Key == m.Command.Key {
			return entry.command.Value, true
		}
	}

	// not in progress key
	return r.Node.Execute(m.Command), false
}
