package main

import (
	"flag"
	"github.com/acharapko/pbench"
	"github.com/acharapko/pbench/bench"
	"github.com/acharapko/pbench/idservice"
)

var id = flag.String("id", "", "node id this client connects to")
var algorithm = flag.String("algorithm", "", "Client API type [paxos]")
var load = flag.Bool("load", false, "Load K keys into DB")


func main() {
	pbench.Init()
	preferredId := idservice.ID(0)
	if *id != "" {
		preferredId = idservice.NewIDFromString(*id)
	}

	b := bench.NewBenchmark(preferredId, *algorithm)
	if *load {
		b.Load()
	} else {
		b.Run()
	}
}
