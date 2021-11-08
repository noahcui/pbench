package paxosRB

// Just for new commit
import (
	"strconv"
	"sync"
	"time"

	"github.com/acharapko/pbench/RingBuffer"
	"github.com/acharapko/pbench/cfg"
	"github.com/acharapko/pbench/db"
	"github.com/acharapko/pbench/hlc"
	"github.com/acharapko/pbench/idservice"
	"github.com/acharapko/pbench/log"
	"github.com/acharapko/pbench/net"
	"github.com/acharapko/pbench/node"
	"github.com/acharapko/pbench/quorum"
	"github.com/acharapko/pbench/util"
)

type reply func(m interface{})

// entry in log
type entry struct {
	ballot    idservice.Ballot
	command   db.Command
	commit    bool
	request   *net.Request
	quorum    *quorum.Quorum
	timestamp time.Time
	reply
}

type requestQueueItem struct {
	request *net.Request
	reply
}

// PaxosRB instance
type PaxosRB struct {
	node.Node

	config  []idservice.ID
	logRB   *RingBuffer.RB
	log     map[int]*entry   // log ordered by slot
	execute int              // next execute slot number
	active  bool             // active leader
	ballot  idservice.Ballot // highest ballot number
	slot    int              // highest slot number

	p3PendingBallot   idservice.Ballot
	p3pendingSlots    []int
	lastP3Time        int64
	lastCleanupMarker int
	globalExecute     int                  // executed by all nodes. Need for log cleanup
	executeByNode     map[idservice.ID]int // leader's knowledge of other nodes execute counter. Need for log cleanup

	quorum          *quorum.Quorum      // phase 1 quorum
	pendingRequests []*requestQueueItem // phase 1 pending requests

	Q1              func(*quorum.Quorum) bool
	Q2              func(*quorum.Quorum) bool
	ReplyWhenCommit bool

	// Locks
	logLck     sync.RWMutex
	p3Lock     sync.RWMutex
	markerLock sync.RWMutex
}

// NewPaxos creates new paxos instance
func NewPaxos(n node.Node, options ...func(*PaxosRB)) *PaxosRB {
	p := &PaxosRB{
		Node:            n,
		log:             make(map[int]*entry, cfg.GetConfig().BufferSize),
		logRB:           RingBuffer.NewRB(20000, 500),
		slot:            -1,
		quorum:          quorum.NewQuorum(),
		pendingRequests: make([]*requestQueueItem, 0),
		Q1:              func(q *quorum.Quorum) bool { return q.Majority() },
		Q2:              func(q *quorum.Quorum) bool { return q.Majority() },
		ReplyWhenCommit: false,
	}

	for _, opt := range options {
		opt(p)
	}

	return p
}

// IsLeader indicates if this node is current leader
func (p *PaxosRB) IsLeader() bool {
	return p.active || p.ballot.ID() == p.ID()
}

// Leader returns leader idservice of the current ballot
func (p *PaxosRB) Leader() idservice.ID {
	return p.ballot.ID()
}

// Ballot returns current ballot
func (p *PaxosRB) Ballot() idservice.Ballot {
	return p.ballot
}

// SetActive sets current paxos instance as active leader
func (p *PaxosRB) SetActive(active bool) {
	p.active = active
}

// SetBallot sets a new ballot number
func (p *PaxosRB) SetBallot(b idservice.Ballot) {
	p.ballot = b
}

// forceful sync of P3 messages if no progress was done in past 10 ms and no P3 msg was piggybacked
func (p *PaxosRB) P3Sync(tnow int64) {
	p.logLck.RLock()
	defer p.logLck.RUnlock()
	log.Debugf("Syncing P3 (tnow=%d)", tnow)
	if tnow-10 > p.lastP3Time && p.lastP3Time > 0 && p.p3PendingBallot > 0 {
		p.p3Lock.Lock()
		p.Broadcast(P3{
			Ballot: p.p3PendingBallot,
			Slot:   p.p3pendingSlots,
		})
		p.lastP3Time = tnow
		p.p3pendingSlots = make([]int, 0, 100)
		p.p3Lock.Unlock()
	}
}

func (p *PaxosRB) UpdateLastExecuteByNode(id idservice.ID, lastExecute int) {
	p.markerLock.Lock()
	defer p.markerLock.Unlock()
	p.executeByNode[id] = lastExecute
}

func (p *PaxosRB) GetSafeLogCleanupMarker() int {
	marker := p.execute
	for _, c := range p.executeByNode {
		if c < marker {
			marker = c
		}
	}
	return marker
}

func (p *PaxosRB) CleanupLog() {
	p.markerLock.Lock()
	marker := p.GetSafeLogCleanupMarker()
	//log.Debugf("Replica %v log cleanup. lastCleanupMarker: %d, safeCleanUpMarker: %d", p.NodeId(), p.lastCleanupMarker, marker)
	p.markerLock.Unlock()

	p.logLck.Lock()
	defer p.logLck.Unlock()
	for i := p.lastCleanupMarker; i < marker; i++ {
		delete(p.log, i)
	}
	p.lastCleanupMarker = marker
}

// HandleQuorumRequest handles request and start phase 1 or phase 2
func (p *PaxosRB) HandleRequest(r net.Request, reply reply) {
	// log.Debugf("Replica %s received %v\n", p.NodeId(), r)
	if !p.active {
		p.pendingRequests = append(p.pendingRequests, &requestQueueItem{
			request: &r,
			reply:   reply,
		})
		// current phase 1 pending
		if p.ballot.ID() != p.ID() {
			p.P1a()
		}
	} else {
		p.P2a(&r, reply)
	}
}

// P1a starts phase 1 prepare
func (p *PaxosRB) P1a() {
	if p.active {
		return
	}
	p.ballot.Next(p.ID())
	p.quorum.Reset()
	p.quorum.ACK(p.ID())
	p.Broadcast(P1a{Ballot: p.ballot})
}

// P2a starts phase 2 accept
func (p *PaxosRB) P2a(r *net.Request, reply reply) {

	// p.logLck.Lock()
	// defer p.logLck.Unlock()

	// Not thread safe, but system safe
	p.slot++

	/*
		p.log[p.slot] = &entry{
			ballot:    p.ballot,
			command:   r.Command,
			request:   r,
			quorum:    quorum.NewQuorum(),
			timestamp: time.Now(),
			reply:     reply,
		}
		p.log[p.slot].quorum.ACK(p.ID())
	*/
	p.logRB.Set(p.slot, &entry{
		ballot:    p.ballot,
		command:   r.Command,
		request:   r,
		quorum:    quorum.NewQuorum(),
		timestamp: time.Now(),
		reply:     reply,
	})
	m := P2a{
		Ballot:  p.ballot,
		Slot:    p.slot,
		Command: r.Command,
	}

	p.p3Lock.Lock()
	if p.p3PendingBallot > 0 {
		m.P3msg = P3{Ballot: p.p3PendingBallot, Slot: p.p3pendingSlots}
		p.p3pendingSlots = make([]int, 0, 100)
		p.lastP3Time = hlc.CurrentTimeInMS()
	}
	p.p3Lock.Unlock()

	if cfg.GetConfig().Thrifty {
		p.MulticastQuorum(cfg.GetConfig().N/2+1, m)
	} else {
		p.Broadcast(m)
	}
}

// HandleP1a handles P1a message
func (p *PaxosRB) HandleP1a(m P1a) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	// new leader
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
		// TODO use BackOff time or forward
		// forward pending requests to new leader
		p.forward()
		// if len(p.requests) > 0 {
		// 	defer p.P1a()
		// }
	}
	// p.logLck.RLock()
	// defer p.logLck.RUnlock()
	l := make(map[int]CommandBallot)
	for s := p.execute; s <= p.slot; s++ {
		e1, err := p.logRB.Get(s)
		e := e1.(*entry)
		if err != nil || e.commit {
			continue
		}
		l[s] = CommandBallot{e.command, e.ballot}
		// if p.log[s] == nil || e.commit {
		// 	continue
		// }
		// l[s] = CommandBallot{p.log[s].command, p.log[s].ballot}
	}

	p.Send(m.Ballot.ID(), P1b{
		Ballot: p.ballot,
		ID:     p.ID(),
		Log:    l,
	})
}

func (p *PaxosRB) update(scb map[int]CommandBallot) {
	// p.logLck.Lock()
	// defer p.logLck.Unlock()
	for s, cb := range scb {
		p.slot = util.Max(p.slot, s)
		e1, err := p.logRB.Get(s)
		e := e1.(*entry)

		if err != nil {
			p.logRB.Set(s, &entry{
				ballot:  cb.Ballot,
				command: cb.Command,
				commit:  false,
			})
		} else {
			if !e.commit && cb.Ballot > e.ballot {
				e.ballot = cb.Ballot
				e.command = cb.Command
			}

		}
		/*
			if e, exists := p.log[s]; exists {
				if !e.commit && cb.Ballot > e.ballot {
					e.ballot = cb.Ballot
					e.command = cb.Command
				}
			} else {
				p.log[s] = &entry{
					ballot:  cb.Ballot,
					command: cb.Command,
					commit:  false,
				}
			}
		*/
	}
}

// HandleP1b handles P1b message
func (p *PaxosRB) HandleP1b(m P1b) {
	// old message
	if m.Ballot < p.ballot || p.active {
		// log.Debugf("Replica %s ignores old message [%v]\n", p.NodeId(), m)
		return
	}

	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())

	p.update(m.Log)

	// reject message
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false // not necessary
		// forward pending requests to new leader
		p.forward()
		// p.P1a()
	}

	// ack message
	if m.Ballot.ID() == p.ID() && m.Ballot == p.ballot {
		p.quorum.ACK(m.ID)
		if p.Q1(p.quorum) {
			p.active = true
			p.p3PendingBallot = p.ballot
			// propose any uncommitted entries
			for i := p.execute; i <= p.slot; i++ {
				e, err := p.logRB.Get(i)
				logEntry, _ := e.(*entry)
				/*
						// TODO nil gap?
						p.logLck.RLock()
						logEntry := p.log[i]
						p.logLck.RUnlock()


					if logEntry == nil || logEntry.commit {
						continue
					}
				*/
				if err != nil || logEntry.commit {
					continue
				}
				logEntry.ballot = p.ballot
				logEntry.quorum = quorum.NewQuorum()
				logEntry.quorum.ACK(p.ID())
				p.Broadcast(P2a{
					Ballot:  p.ballot,
					Slot:    i,
					Command: logEntry.command,
				})

			}
			// propose new commands
			for _, req := range p.pendingRequests {
				p.P2a(req.request, req.reply)
			}
			p.pendingRequests = make([]*requestQueueItem, 0)
		}
	}
}

// HandleP2a handles P2a message
func (p *PaxosRB) HandleP2a(m P2a) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	if m.Ballot >= p.ballot {
		p.ballot = m.Ballot
		p.active = false
		// update slot number
		p.slot = util.Max(p.slot, m.Slot)
		// update entry
		p.logLck.Lock()
		// if e, exists := p.log[m.Slot]; exists {
		if e1, exists := p.logRB.Get(m.Slot); exists == nil {
			e, _ := e1.(*entry)
			if !e.commit && m.Ballot > e.ballot {
				// different command and request is not nil
				if !e.command.Equal(m.Command) && e.request != nil {
					p.Forward(m.Ballot.ID(), *e.request)
					// p.Retry(*e.request)
					log.Debugf("Received different command (%v!=%v) for slot %d, resetting the request", m.Command, e.command, m.Slot)
					e.request = nil
				}
				e.command = m.Command
				e.ballot = m.Ballot
			} else if e.commit && e.ballot == 0 {
				// we can have commit slot with no ballot when we received P3 before P2a
				e.command = m.Command
				e.ballot = m.Ballot
			}
		} else {
			p.logRB.Set(m.Slot, &entry{
				ballot:  m.Ballot,
				command: m.Command,
				commit:  false})
			/*
				p.log[m.Slot] = &entry{
					ballot:  m.Ballot,
					command: m.Command,
					commit:  false,
				}
			*/
		}
		p.logLck.Unlock()
	}

	p.Send(m.Ballot.ID(), P2b{
		Ballot: p.ballot,
		Slot:   m.Slot,
		ID:     p.ID(),
	})

	if len(m.P3msg.Slot) > 0 {
		p.HandleP3(m.P3msg)
	}
}

// HandleP2b handles P2b message
func (p *PaxosRB) HandleP2b(m P2b) {
	// old message
	/*
		p.logLck.RLock()
		entry, exist := p.log[m.Slot]
		p.logLck.RUnlock()
	*/
	e, _ := p.logRB.Get(m.Slot)
	entry, exist := e.(*entry)
	if !exist || m.Ballot < entry.ballot || entry.commit {
		return
	}
	// reject message
	// node update its ballot number and falls back to acceptor
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
		// send pending P3s we have for old ballot
		p.p3Lock.Lock()
		p.Broadcast(P3{
			Ballot: p.p3PendingBallot,
			Slot:   p.p3pendingSlots,
		})
		p.lastP3Time = hlc.CurrentTimeInMS()
		p.p3pendingSlots = make([]int, 0, 100)
		p.p3PendingBallot = 0
		p.p3Lock.Unlock()
	}

	// ack message
	// the current slot might still be committed with q2
	// if no q2 can be formed, this slot will be retried when received p2a or p3
	if m.Ballot.ID() == p.ID() && m.Ballot == entry.ballot {
		entry.quorum.ACK(m.ID)
		if p.Q2(entry.quorum) {
			entry.commit = true

			p.p3Lock.Lock()
			log.Debugf("Adding slot %d to P3Pending (%v)", m.Slot, p.p3pendingSlots)
			p.p3pendingSlots = append(p.p3pendingSlots, m.Slot)
			p.p3Lock.Unlock()

			if p.ReplyWhenCommit && entry.reply != nil {
				r := entry.request
				entry.reply(net.Reply{
					Command:   r.Command,
					Timestamp: r.Timestamp,
				})
			} else {
				p.exec()
			}
		}
	}
}

// HandleP3 handles phase 3 commit message
func (p *PaxosRB) HandleP3(m P3) {
	log.Debugf("Replica %s HandleP3 {%v} from %v", p.ID(), m, m.Ballot.ID())
	for _, slot := range m.Slot {
		p.slot = util.Max(p.slot, slot)
		// p.logLck.Lock()
		// e, exist := p.log[slot]
		e1, _ := p.logRB.Get(slot)
		e, exist := e1.(*entry)
		if exist {
			if e.ballot == m.Ballot {
				e.commit = true
			} else if e.request != nil {
				// p.Retry(*e.request)
				p.Forward(m.Ballot.ID(), *e.request)
				e.request = nil
				// ask to recover the slot
				log.Debugf("Replica %s needs to recover slot %d on ballot %v", p.ID(), slot, m.Ballot)
				p.sendRecoverRequest(m.Ballot, slot)
			}

		} else {
			// we mark slot as committed, but set ballot to 0 to designate that we have not received P2a for the slot and may need to recover later
			enew := &entry{commit: true, ballot: 0}
			// p.log[slot] = e
			p.logRB.Set(slot, enew)
		}
		// p.logLck.Unlock()

		if p.ReplyWhenCommit {
			if e.request != nil && e.reply != nil {
				e.reply(net.Reply{
					Command:   e.request.Command,
					Timestamp: e.request.Timestamp,
				})
			}
		}
	}
	p.exec()
	//log.Debugf("Leaving HandleP3")
}

func (p *PaxosRB) sendRecoverRequest(ballot idservice.Ballot, slot int) {
	p.Send(ballot.ID(), P3RecoverRequest{
		Ballot: ballot,
		Slot:   slot,
		nodeId: p.ID(),
	})
}

// HandleP3RecoverRequest handles slot recovery request at leader
func (p *PaxosRB) HandleP3RecoverRequest(m P3RecoverRequest) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())
	// p.logLck.Lock()
	// e, exist := p.log[m.Slot]
	e1, _ := p.logRB.Get(m.Slot)
	e, exist := e1.(*entry)
	if exist && e.commit {
		// ok to recover
		p.Send(m.nodeId, P3RecoverReply{
			Ballot:  e.ballot,
			Slot:    m.Slot,
			Command: e.command,
		})
	}
	// p.logLck.Unlock()

	p.exec()
	log.Debugf("Leaving HandleP3RecoverRequest")
}

// HandleP3RecoverReply handles slot recovery
func (p *PaxosRB) HandleP3RecoverReply(m P3RecoverReply) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())
	p.slot = util.Max(p.slot, m.Slot)
	// p.logLck.Lock()
	// e, exist := p.log[m.Slot]
	e1, _ := p.logRB.Get(m.Slot)
	e, exist := e1.(*entry)
	if exist {
		e.command = m.Command
		e.ballot = m.Ballot
		e.commit = true
	}
	// p.logLck.Unlock()

	p.exec()
	log.Debugf("Leaving HandleP3RecoverReply")
}

func (p *PaxosRB) exec() {
	p.logLck.Lock()
	defer p.logLck.Unlock()
	for {
		e1, err := p.logRB.NextToExe_ignore_gap()
		if err != nil {
			log.Debugf("%v", err)
			break
		}

		e, ok := e1.(*entry)
		if ok && p.execute+10 < p.slot && e.commit && e.ballot == 0 {
			// ask to recover the slot
			log.Debugf("Replica %s tries to recover slot %d on ballot %v", p.ID(), p.execute, p.Ballot())
			p.sendRecoverRequest(p.Ballot(), p.execute)
		}
		if !ok || !e.commit || (e.commit && e.ballot == 0) {
			break
		}
		log.Debugf("Replica %s execute [s=%d, cmd=%v]", p.ID(), p.execute, e.command)
		value := p.Execute(e.command)
		if e.request != nil && e.reply != nil {
			replyMsg := net.Reply{
				Command:    e.command,
				Value:      value,
				Properties: make(map[string]string),
			}
			replyMsg.Properties[PropertyHeaderSlot] = strconv.Itoa(p.execute)
			replyMsg.Properties[PropertyHeaderBallot] = e.ballot.String()
			replyMsg.Properties[PropertyExecute] = strconv.Itoa(p.execute)
			e.reply(replyMsg)
			e.request = nil
		}
		/*
			e, ok := p.log[p.execute]
			if ok && p.execute+10 < p.slot && e.commit && e.ballot == 0 {
				// ask to recover the slot
				log.Debugf("Replica %s tries to recover slot %d on ballot %v", p.ID(), p.execute, p.Ballot())
				p.sendRecoverRequest(p.Ballot(), p.execute)
			}

			if !ok || !e.commit || (e.commit && e.ballot == 0) {
				break
			}
			log.Debugf("Replica %s execute [s=%d, cmd=%v]", p.ID(), p.execute, e.command)
			value := p.Execute(e.command)
			if e.request != nil && e.reply != nil {
				replyMsg := net.Reply{
					Command:    e.command,
					Value:      value,
					Properties: make(map[string]string),
				}
				replyMsg.Properties[PropertyHeaderSlot] = strconv.Itoa(p.execute)
				replyMsg.Properties[PropertyHeaderBallot] = e.ballot.String()
				replyMsg.Properties[PropertyExecute] = strconv.Itoa(p.execute)
				e.reply(replyMsg)
				e.request = nil
			}
			// TODO clean up the log periodically
			// delete(p.log, p.execute)
			p.execute++
		*/
	}
	log.Debugf("Leaving exec")
}

func (p *PaxosRB) forward() {
	for _, m := range p.pendingRequests {
		p.Forward(p.ballot.ID(), *m.request)
	}
	p.pendingRequests = make([]*requestQueueItem, 0)
}
