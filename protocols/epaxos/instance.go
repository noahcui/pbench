package epaxos

import (
	"github.com/acharapko/pbench/db"
	"github.com/acharapko/pbench/idservice"
	"github.com/acharapko/pbench/net"
	"github.com/acharapko/pbench/quorum"
)

type status int8

const (
	NONE status = iota
	PREACCEPTED
	ACCEPTED
	COMMITTED
	EXECUTED
)

type instance struct {
	cmd    db.Command
	ballot idservice.Ballot
	status status
	seq    int
	dep    map[idservice.ID]int

	// leader bookkeeping
	request *net.Request
	reply
	quorum  *quorum.Quorum
	changed bool // seq and dep changed
}

// merge the seq and dep for instance
func (i *instance) merge(seq int, dep map[idservice.ID]int) {
	if seq > i.seq {
		i.seq = seq
		i.changed = true
	}
	for id, d := range dep {
		if d > i.dep[id] {
			i.dep[id] = d
			i.changed = true
		}
	}
}

// copyDep clones dependency list of instance
func (i *instance) copyDep() (dep map[idservice.ID]int) {
	dep = make(map[idservice.ID]int)
	for id, d := range i.dep {
		dep[id] = d
	}
	return dep
}