/*
determin by execute
*/

package RingBuffer

import (
	"errors"
	"fmt"
	"sync"
)

type RB struct {
	len       int // sizeof buff
	next      int // next to execut
	maxcommit int
	wr        int // total rounds to wait before say I am dead
	buf       []interface{}
	// toexe  []int // times try to execute, init as 1 for each new entry
	commit []bool
	idx    []int
	mu     []sync.Mutex
	exe    []bool
	muc    sync.Mutex
	mue    sync.Mutex
	// mu     sync.Mutex
}

func (rb *RB) Arrayindex(index int) int {
	return index % rb.len
}

func (rb *RB) Set(index int, val interface{}) error {
	if index < 0 {
		msg := fmt.Sprintf("index:%v < 0", index)
		return errors.New(msg)
	}
	idx := rb.Arrayindex(index)
	rb.mu[idx].Lock()
	defer rb.mu[idx].Unlock()
	// Nothing is waiting to be executed
	if rb.exe[idx] {
		// correct index || init setup
		if index == rb.idx[idx]+rb.len || rb.idx[idx] < 0 {
			rb.buf[idx] = val
			rb.idx[idx] = index
			rb.commit[idx] = false
			rb.exe[idx] = false
			return nil
		}
		// Index messed up
		return errors.New("Index messed up")
	}
	return errors.New("there's something hasn't been executed yet")
}

func (rb *RB) CommitIndex(index int) error {
	if index < 0 {
		msg := fmt.Sprintf("index:%v < 0", index)
		return errors.New(msg)
	}
	idx := rb.Arrayindex(index)
	rb.mu[idx].Lock()
	defer rb.mu[idx].Unlock()
	// Index messed up
	if rb.idx[idx] != index {
		msg := fmt.Sprintf("index:%v != stored index %v", index, rb.idx[idx])
		return errors.New(msg)
	}
	rb.commit[idx] = true
	rb.muc.Lock()
	if index > rb.maxcommit {
		rb.maxcommit = index
	}
	rb.muc.Unlock()
	return nil
}

func (rb *RB) Get(index int) (interface{}, error) {
	if index < 0 {
		msg := fmt.Sprintf("index:%v < 0", index)
		return nil, errors.New(msg)
	}
	idx := rb.Arrayindex(index)
	rb.mu[idx].Lock()
	defer rb.mu[idx].Unlock()
	// Index messed up
	if rb.idx[idx] != index {
		msg := fmt.Sprintf("index:%v != stored index %v", index, rb.idx[idx])
		return nil, errors.New(msg)
	}
	return rb.buf[idx], nil
}

// First thing returned is nil unless there's something ready to be executed
func (rb *RB) NextToExe() (interface{}, error) {

	rb.mue.Lock()
	next := rb.next
	rb.mue.Unlock()
	rb.muc.Lock()
	max := rb.maxcommit
	rb.muc.Unlock()
	idx := rb.Arrayindex(next)
	// There's something ready to be executed!
	rb.mu[idx].Lock()
	defer rb.mu[idx].Unlock()
	if max < next {
		return nil, nil
	}
	if rb.commit[idx] && !rb.exe[idx] {
		rb.exe[idx] = true
		to_ret := rb.buf[idx]
		rb.mue.Lock()
		rb.next++
		rb.mue.Unlock()
		return to_ret, nil
	}
	if max-next > rb.wr {
		msg := fmt.Sprintf("big gap between to execute(%v) and maxcommit(%v)", next, max)
		return nil, errors.New(msg)
	}
	// Nothing is ready to be executed now. return nil
	return nil, nil
}

// don't care about gap
func (rb *RB) CommitIndex_ignore_gap(index int) error {
	if index < 0 {
		msg := fmt.Sprintf("index:%v < 0", index)
		return errors.New(msg)
	}
	idx := rb.Arrayindex(index)
	rb.mu[idx].Lock()
	defer rb.mu[idx].Unlock()
	// Index messed up
	if rb.idx[idx] != index {
		msg := fmt.Sprintf("commit index:%v != stored index %v", index, rb.idx[idx])
		return errors.New(msg)
	}
	rb.commit[idx] = true
	return nil
}

// don't care about gap
func (rb *RB) NextToExe_ignore_gap() (interface{}, error) {
	rb.mue.Lock()
	next := rb.next
	rb.mue.Unlock()
	idx := rb.Arrayindex(next)
	// There's something ready to be executed!
	rb.mu[idx].Lock()
	defer rb.mu[idx].Unlock()
	if rb.commit[idx] && !rb.exe[idx] {
		rb.exe[idx] = true
		to_ret := rb.buf[idx]
		rb.mue.Lock()
		rb.next++
		rb.mue.Unlock()
		return to_ret, nil
	}
	// Nothing is ready to be executed now. return nil
	return nil, nil
}

func (rb *RB) return_c_e() (int, int) {
	rb.mue.Lock()
	rb.muc.Lock()
	defer rb.mue.Unlock()
	defer rb.muc.Unlock()
	return rb.maxcommit, rb.next
}
func NewRB(len int, wr int) *RB {
	rb := RB{len: len}
	rb.len = len
	rb.next = 0
	rb.buf = make([]interface{}, len)
	rb.maxcommit = 0
	rb.commit = make([]bool, len)
	rb.idx = make([]int, len)
	rb.mu = make([]sync.Mutex, len)
	rb.exe = make([]bool, len)
	rb.wr = wr
	for i := 0; i < len; i++ {
		rb.buf[i] = nil
		rb.commit[i] = false
		rb.idx[i] = -1
		rb.exe[i] = true
	}
	return &rb
}

// Just for new commit
