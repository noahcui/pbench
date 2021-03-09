package main

import (
	"flag"
	"github.com/acharapko/pbench"
	"github.com/acharapko/pbench/cfg"
	"github.com/acharapko/pbench/idservice"
	"github.com/acharapko/pbench/log"
	"github.com/acharapko/pbench/protocols/batchedpaxos"
	"github.com/acharapko/pbench/protocols/epaxos"
	"github.com/acharapko/pbench/protocols/paxos"
	"sync"
)

var algorithm = flag.String("algorithm", "paxos", "Distributed algorithm")
var id = flag.String("id", "", "NodeId in format of Zone.Node.")
var simulation = flag.Bool("sim", false, "simulation mode")


func replica(id idservice.ID) {

	log.Infof("node %v starting with algorithm %s", id, *algorithm)

	switch *algorithm {

	case "paxos":
		paxos.NewReplica(id).Run()
	case "batchedpaxos":
		batchedpaxos.NewReplica(id).Run()
	case "epaxos":
		epaxos.NewReplica(id).Run()
	default:
		panic("Unknown algorithm")
	}
}

func main() {
	pbench.Init()

	if *simulation {
		var wg sync.WaitGroup
		wg.Add(1)
		//Simulation()
		for id := range cfg.GetConfig().Addrs {
			n := id
			go replica(n)
		}
		wg.Wait()
	} else {
		replica(idservice.NewIDFromString(*id))
		log.Debugf("Server done")
	}
}
