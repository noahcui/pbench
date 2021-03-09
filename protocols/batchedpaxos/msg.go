package batchedpaxos

import (
	"encoding/gob"
	"fmt"
	"github.com/acharapko/pbench/db"
	"github.com/acharapko/pbench/idservice"
)

func init() {
	gob.Register(P1a{})
	gob.Register(P1b{})
	gob.Register(P2a{})
	gob.Register(P2b{})
	gob.Register(P3{})
}

// P1a prepare message
type P1a struct {
	Ballot idservice.Ballot
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%v}", m.Ballot)
}

// CommandBallot combines each command with its ballot number
type CommandBallot struct {
	Commands []db.Command
	Ballot   idservice.Ballot
}

func (cb CommandBallot) String() string {
	return fmt.Sprintf("cmd=%v b=%v", cb.Commands, cb.Ballot)
}

// P1b promise message
type P1b struct {
	Ballot idservice.Ballot
	ID     idservice.ID               // from node id
	Log    map[int]CommandBallot // uncommitted logs
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b {b=%v id=%s log=%v}", m.Ballot, m.ID, m.Log)
}

// P2a accept message
type P2a struct {
	Ballot  idservice.Ballot
	Slot    int
	Command []db.Command
	P3msg	P3
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v s=%d cmd=%v, P3={%v}}", m.Ballot, m.Slot, m.Command, m.P3msg)
}

// P2b accepted message
type P2b struct {
	Ballot idservice.Ballot
	ID     idservice.ID // from node id
	Slot   int
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v id=%s s=%d}", m.Ballot, m.ID, m.Slot)
}

// P3 commit message
type P3 struct {
	Ballot  idservice.Ballot
	Slot    []int
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v s=%d cmd=%v}", m.Ballot, m.Slot)
}


type P3RecoverRequest struct {
	Ballot  idservice.Ballot
	Slot    int
	nodeId  idservice.ID
}

func (m *P3RecoverRequest) String() string {
	return fmt.Sprintf("P3RecoverRequest {b=%v slots=%d, nodeToRecover=%v}",  m.Ballot, m.Slot, m.nodeId)
}

type P3RecoverReply struct {
	Ballot   idservice.Ballot
	Slot     int
	Commands []db.Command
}

func (m *P3RecoverReply) String() string {
	return fmt.Sprintf("P3Recoverreply {b=%v slots=%d, cmd=%v}",  m.Ballot, m.Slot, m.Commands)
}