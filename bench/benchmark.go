package bench

import (
	"github.com/acharapko/pbench/cfg"
	"github.com/acharapko/pbench/client"
	"github.com/acharapko/pbench/db"
	"github.com/acharapko/pbench/idservice"
	"github.com/acharapko/pbench/util"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/acharapko/pbench/log"
)

// db implements DB interface for benchmarking
type RWClient struct {
	client.Client
}

func (d *RWClient) Read(k int) ([]byte, error) {
	key := db.Key(k)
	v, err := d.Get(key)
	if len(v) == 0 {
		return nil, nil
	}
	return v, err
}

func (d *RWClient) Write(k int, v []byte) error {
	key := db.Key(k)
	err := d.Put(key, v)
	return err
}

// DB is general interface implemented by client to call client library
type DB interface {
	Read(key int) ([]byte, error)
	Write(key int, value []byte) error
}

// Benchmark is benchmarking tool that generates workload and collects operation history and latency
type Benchmark struct {
	clients []DB // read/write operation interface
	cfg.Bconfig
	*History

	rate      *Limiter
	latency   []time.Duration // latency per operation
	startTime time.Time
	zipf      *rand.Zipf
	counter   int

	wait sync.WaitGroup // waiting for all generated keys to complete
}

// NewBenchmark returns new Benchmark object given implementation of DB interface
func NewBenchmark(preferredId idservice.ID, algorithm string) *Benchmark {
	log.Debugf("Starting new Benchmark with preferred node %v and alg %s", preferredId, algorithm)
	b := new(Benchmark)
	b.Bconfig = cfg.GetConfig().Benchmark
	b.clients = make([]DB, b.Concurrency)
	for i := 0; i < b.Concurrency; i++ {
		db := new(RWClient)
		switch algorithm {
		case "paxos", "batchedpaxos", "epaxos":
			db.Client = client.NewClient(preferredId, idservice.NewIDFromString("0." + strconv.Itoa(i+1)))
		}
		b.clients[i] = db
	}

	b.History = NewHistory()
	if b.Throttle > 0 {
		b.rate = NewLimiter(b.Throttle)
	}
	rand.Seed(time.Now().UTC().UnixNano())
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	b.zipf = rand.NewZipf(r, b.ZipfianS, b.ZipfianV, uint64(b.K))
	return b
}

// Load will create all K keys to DB
func (b *Benchmark) Load() {
	b.W = 1.0
	b.Throttle = 0

	keys := make(chan int, b.Concurrency)
	latencies := make(chan time.Duration, 1000)
	defer close(latencies)
	go b.collect(latencies)

	b.startTime = time.Now()
	for i := 0; i < b.Concurrency; i++ {
		go b.closedLoopWorker(keys, latencies, b.clients[i])
	}
	for i := b.Min; i < b.Min+b.K; i++ {
		b.wait.Add(1)
		keys <- i
	}
	t := time.Now().Sub(b.startTime)

	close(keys)
	b.wait.Wait()
	stat := Statistic(b.latency)

	log.Infof("Benchmark took %v\n", t)
	log.Infof("Throughput %f\n", float64(len(b.latency))/t.Seconds())
	log.Info(stat)
}

// Run starts the main logic of benchmarking
func (b *Benchmark) Run() {
	var stop chan bool
	if b.Move {
		move := func() { b.Mu = float64(int(b.Mu+1) % b.K) }
		stop = util.Schedule(move, time.Duration(b.Speed)*time.Millisecond)
		defer close(stop)
	}

	b.latency = make([]time.Duration, 0)
	keys := make(chan int, b.Concurrency)
	latencies := make(chan time.Duration, 1000)
	defer close(latencies)
	go b.collect(latencies)

	for i := 0; i < b.Concurrency; i++ {
		if b.OpenLoopWorker {
			go b.openLoopWorker(keys, latencies, b.clients[i], b.MaxOutstanding, b.OpenLoopThrottle)
		} else {
			go b.closedLoopWorker(keys, latencies, b.clients[i])
		}
	}

	b.startTime = time.Now()
	if b.T > 0 {
		timer := time.NewTimer(time.Second * time.Duration(b.T))
	loop:
		for {
			select {
			case <-timer.C:
				break loop
			default:
				b.wait.Add(1)
				keys <- b.next()
			}
		}
	} else {
		for i := 0; i < b.N; i++ {
			b.wait.Add(1)
			keys <- b.next()
		}
		b.wait.Wait()
	}
	t := time.Now().Sub(b.startTime)

	close(keys)
	stat := Statistic(b.latency)
	log.Infof("Concurrency = %d", b.Concurrency)
	log.Infof("Write Ratio = %f", b.W)
	log.Infof("Number of Keys = %d", b.K)
	log.Infof("Benchmark Time = %v\n", t)
	log.Infof("Throughput = %f\n", float64(len(b.latency))/t.Seconds())
	log.Info(stat)

	stat.WriteFile("latency")
	b.History.WriteFile("history")

	if b.LinearizabilityCheck {
		n := b.History.Linearizable()
		if n == 0 {
			log.Info("The execution is linearizable.")
		} else {
			log.Info("The execution is NOT linearizable.")
			log.Infof("Total anomaly read operations are %d", n)
			log.Infof("Anomaly percentage is %f", float64(n)/float64(stat.Size))
		}
	}
}

// generates key based on distribution
func (b *Benchmark) next() int {
	var key int
	switch b.Distribution {
	case "order":
		b.counter = (b.counter + 1) % b.K
		key = b.counter + b.Min

	case "uniform":
		key = rand.Intn(b.K) + b.Min

	case "conflict":
		if rand.Intn(100) < b.Conflicts {
			key = 0
		} else {
			b.counter = (b.counter + 1) % b.K
			key = b.counter + b.Min
		}

	case "normal":
		key = int(rand.NormFloat64()*b.Sigma + b.Mu)
		for key < 0 {
			key += b.K
		}
		for key > b.K {
			key -= b.K
		}

	case "zipfan":
		key = int(b.zipf.Uint64())

	case "exponential":
		key = int(rand.ExpFloat64() / b.Lambda)

	default:
		log.Fatalf("unknown distribution %s", b.Distribution)
	}

	if b.Throttle > 0 {
		b.rate.Wait()
	}

	return key
}

func (b *Benchmark) closedLoopWorker(keys <-chan int, result chan<- time.Duration, db DB) {
	var s time.Time
	var e time.Time
	var v []byte
	var err error
	for k := range keys {
		op := new(operation)
		if rand.Float64() < b.W {
			v = util.GenerateRandVal(b.Bconfig.Size)
			s = time.Now()
			err = db.Write(k, v)
			e = time.Now()
			op.input = v
		} else {
			s = time.Now()
			v, err = db.Read(k)
			e = time.Now()
			op.output = v
		}
		op.start = s.Sub(b.startTime).Nanoseconds()
		if err == nil {
			op.end = e.Sub(b.startTime).Nanoseconds()
			result <- e.Sub(s)
		} else {
			op.end = math.MaxInt64
			log.Error(err)
		}
		b.History.AddOperation(k, op)
	}
}

func (b *Benchmark) openLoopWorker(keys <-chan int, result chan<- time.Duration, db DB, maxOutstanding, openLoopThrottle int) {
	var err error
	var outstanding = make(chan bool, maxOutstanding)
	for k := range keys {

		outstanding <- true
		go func() {
			var v []byte
			var s time.Time
			var e time.Time
			if openLoopThrottle > 0 {
				time.Sleep(time.Duration(openLoopThrottle) * time.Millisecond)
			}
			op := new(operation)
			if rand.Float64() < b.W {
				v = util.GenerateRandVal(b.Bconfig.Size)
				s = time.Now()
				err = db.Write(k, v)
				e = time.Now()
				op.input = v
			} else {
				s = time.Now()
				v, err = db.Read(k)
				e = time.Now()
				op.output = v
			}
			op.start = s.Sub(b.startTime).Nanoseconds()
			if err == nil {
				op.end = e.Sub(b.startTime).Nanoseconds()
				result <- e.Sub(s)
			} else {
				op.end = math.MaxInt64
				log.Error(err)
			}
			b.History.AddOperation(k, op)
			<- outstanding
		}()
	}
}

func (b *Benchmark) collect(latencies <-chan time.Duration) {
	for t := range latencies {
		b.latency = append(b.latency, t)
		b.wait.Done()
	}
}
