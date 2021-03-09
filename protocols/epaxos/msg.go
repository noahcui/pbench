package epaxos

import (
	"encoding/gob"
	"fmt"
	"github.com/acharapko/pbench/db"
	"github.com/acharapko/pbench/idservice"
)

func init() {
	gob.Register(PreAccept{})
	gob.Register(PreAcceptReply{})
	gob.Register(Accept{})
	gob.Register(AcceptReply{})
	gob.Register(Commit{})
}

type PreAccept struct {
	Ballot  idservice.Ballot
	Replica idservice.ID
	Slot    int
	Command db.Command
	Seq     int
	Dep     map[idservice.ID]int
}

func (m PreAccept) String() string {
	return fmt.Sprintf("PreAccept {bal=%d id=%s s=%d cmd=%v seq=%d dep=%v}", m.Ballot, m.Replica, m.Slot, m.Command, m.Seq, m.Dep)
}

type PreAcceptReply struct {
	Ballot    idservice.Ballot
	Replica   idservice.ID
	Slot      int
	Seq       int
	Dep       map[idservice.ID]int
	Committed map[idservice.ID]int
}

func (m PreAcceptReply) String() string {
	return fmt.Sprintf("PreAcceptReply {bal=%d id=%s s=%d seq=%d dep=%v c=%v}", m.Ballot, m.Replica, m.Slot, m.Seq, m.Dep, m.Committed)
}

type Accept struct {
	Ballot  idservice.Ballot
	Replica idservice.ID
	Slot    int
	Seq     int
	Dep     map[idservice.ID]int
}

type AcceptReply struct {
	Ballot  idservice.Ballot
	Replica idservice.ID
	Slot    int
}

type Commit struct {
	Ballot  idservice.Ballot
	Replica idservice.ID
	Slot    int
	Command db.Command
	Seq     int
	Dep     map[idservice.ID]int
}