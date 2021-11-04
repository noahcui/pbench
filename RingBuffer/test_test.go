package RingBuffer

import (
	// "crypto/rand"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var len int = 99999
var counter = 0
var wr = 1000

type LogEntry struct {
	Term    int
	Command interface{}
}

func set(t *testing.T, rb *RB, index int) error {
	err := rb.Set(index, LogEntry{index, index})
	return err
}

func commit(t *testing.T, rb *RB, index int) error {
	// return rb.CommitIndex(index)
	return rb.CommitIndex_ignore_gap(index)
}

func exe(t *testing.T, rb *RB, index int) (interface{}, error) {
	// return rb.NextToExe()
	return rb.NextToExe_ignore_gap()
}

func setcommitexe(t *testing.T, rb *RB, index int) {
	err := set(t, rb, index)
	if err != nil {
		t.Fatalf("set faild at index %v, errmsg: %v\n", index, err)
	}
	// fmt.Println(index)
	err = commit(t, rb, index)
	if err != nil {
		t.Fatalf("commit faild at index %v, errmsg: %v\n", index, err)
	}
	// fmt.Println(index)
	for val, err := exe(t, rb, index); val != nil; {
		val, err = exe(t, rb, index)
		if err != nil {
			t.Fatalf("exe faild, errmsg: %v\n", err)
		}
	}
	// fmt.Println(index)
	// fmt.Printf("\n")
}

// Just for new commit
func Test_Correct(t *testing.T) {
	fmt.Println("\ncorrect test started...")
	defer fmt.Println("finished...")
	rb := NewRB(len, wr)
	counter = 0
	for counter < len*10 {
		setcommitexe(t, rb, counter)
		// fmt.Println(counter)
		counter++
	}
	fmt.Println(" 10 times lenth put done")
	for i := len * 9; i < len*10; i++ {
		entry, err := rb.Get(i)
		if err != nil {
			t.Fatalf("get faild at index %v\n %v\n", i, err)
		}
		en := entry.(LogEntry)
		if en.Term%len != i%len {
			t.Fatalf("misorder at index %v\n", i)
		}
	}
}

func Test_lagged1(t *testing.T) {
	fmt.Println("\nlagged1 test started...")
	defer fmt.Println("finished...")
	counter = 0
	rb := NewRB(len, wr)
	for counter < len*10 {
		setcommitexe(t, rb, counter)
		counter++
	}
	err := rb.Set(0, LogEntry{0, 0})
	if err == nil {
		t.Fatalf("Should faild at index %v\n", 0)
	}
}

func Test_lagged2(t *testing.T) {
	fmt.Println("\nlagged2 test started...")
	defer fmt.Println("finished...")
	for i := 0; i < 100; i++ {
		counter = 0
		rb := NewRB(len, wr)
		for counter < len {
			setcommitexe(t, rb, counter)
			counter++
		}
		s := rand.NewSource(time.Now().UnixNano())
		r := rand.New(s)
		rl := r.Intn(len) + len
		for counter < len*2 {
			if counter == rl {
				counter++
				continue
			}
			err := rb.Set(counter, LogEntry{counter, counter})
			if err != nil {
				t.Fatalf("set faild at index %v\n", counter)
			}
			counter++
		}
		err := rb.Set(rl+len, LogEntry{rl, rl})
		if err == nil {
			t.Fatalf("Should faild at index %v\n", rl)
		}
		err = rb.Set(rl+len-1, LogEntry{rl - 1, rl - 1})
		if err == nil {
			t.Fatalf("Should faild at index %v\n", rl)
		}

	}
}

func Test_go_back(t *testing.T) {
	fmt.Println("\ngo_back test started...")
	defer fmt.Println("finished...")
	counter = 0
	rb := NewRB(len, wr)
	for counter < len*10 {
		setcommitexe(t, rb, counter)
		counter++
	}
	counter = 0
	for counter < len*9 {
		err := rb.Set(counter, LogEntry{counter, counter})
		if err == nil {
			t.Fatalf("Should faild at index %v\n", counter)
		}
		counter++
	}
}
func Test_Empty_get(t *testing.T) {
	fmt.Println("\nempty test started...")
	defer fmt.Println("finished...")
	counter = 0
	rb := NewRB(len, wr)
	for i := 0; i < len; i++ {
		_, err := rb.Get(i)
		// en := entry.(LogEntry)
		if err == nil {
			t.Fatalf("Should faild at index %v\n", i)
		}
	}
}

func Test_go_race(t *testing.T) {
	fmt.Println("\nrace test started...")
	defer fmt.Println("finished...")
	rb := NewRB(len, wr)
	go func(t *testing.T) {
		for i := 0; i < len/4; i++ {
			setcommitexe(t, rb, i*4)
		}
	}(t)
	go func(t *testing.T) {
		for i := 0; i < len/4; i++ {
			setcommitexe(t, rb, i*4+1)
		}
	}(t)
	go func(t *testing.T) {
		for i := 0; i < len/4; i++ {
			setcommitexe(t, rb, i*4+2)
		}
	}(t)
	go func(t *testing.T) {
		for i := 0; i < len/4; i++ {
			setcommitexe(t, rb, i*4+3)
		}
	}(t)
}
