package quorum

import (
	"fmt"
	"github.com/acharapko/pbench/cfg"
	"github.com/acharapko/pbench/idservice"
)

// Quorum records each acknowledgement and check for different types of quorum satisfied
type Quorum struct {
	size  int
	acks  map[idservice.ID]bool
	zones map[int]int
	nacks map[idservice.ID]bool
}

// NewQuorum returns a new Quorum
func NewQuorum() *Quorum {
	q := &Quorum{
		size:  0,
		acks:  make(map[idservice.ID]bool),
		zones: make(map[int]int),
	}
	return q
}

// ACK adds id to quorum ack records
func (q *Quorum) ACK(id idservice.ID) {
	if !q.acks[id] {
		q.acks[id] = true
		q.size++
		q.zones[id.Zone()]++
	}
}

// NACK adds id to quorum nack records
func (q *Quorum) NACK(id idservice.ID) {
	if !q.nacks[id] {
		q.nacks[id] = true
	}
}

// ADD increase ack size by one
func (q *Quorum) ADD() {
	q.size++
}

// Size returns current ack size
func (q *Quorum) Size() int {
	return q.size
}

// Reset resets the quorum to empty
func (q *Quorum) Reset() {
	q.size = 0
	q.acks = make(map[idservice.ID]bool)
	q.zones = make(map[int]int)
	q.nacks = make(map[idservice.ID]bool)
}

func (q *Quorum) All() bool {
	return q.size == cfg.GetConfig().N
}

// Majority quorum satisfied
func (q *Quorum) Majority() bool {
	return q.size > cfg.GetConfig().N/2
}

// FastQuorum from fast paxos
func (q *Quorum) FastQuorum() bool {
	return q.size >= cfg.GetConfig().N*3/4
}

// AllZones returns true if there is at one ack from each zone
func (q *Quorum) AllZones() bool {
	return len(q.zones) == cfg.GetConfig().Z
}

// ZoneMajority returns true if majority quorum satisfied in any zone
func (q *Quorum) ZoneMajority() bool {
	for z, n := range q.zones {
		if n > cfg.GetConfig().Npz[z]/2 {
			return true
		}
	}
	return false
}

// GridRow == AllZones
func (q *Quorum) GridRow() bool {
	return q.AllZones()
}

// GridColumn == all nodes in one zone
func (q *Quorum) GridColumn() bool {
	for z, n := range q.zones {
		if n == cfg.GetConfig().Npz[z] {
			return true
		}
	}
	return false
}

// FGridQ1 is flexible grid quorum for phase 1
func (q *Quorum) FGridQ1(Fz int) bool {
	zone := 0
	for z, n := range q.zones {
		if n > cfg.GetConfig().Npz[z]/2 {
			zone++
		}
	}
	return zone >= cfg.GetConfig().Z-Fz
}

// FGridQ2 is flexible grid quorum for phase 2
func (q *Quorum) FGridQ2(Fz int) bool {
	zone := 0
	for z, n := range q.zones {
		if n > cfg.GetConfig().Npz[z]/2 {
			zone++
		}
	}
	return zone >= Fz+1
}

func (q *Quorum) String() string {
	return fmt.Sprintf("Quourm {acked_szie=%v, acks=%v}", q.size, q.acks)
}