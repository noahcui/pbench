package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/acharapko/pbench"
	"github.com/acharapko/pbench/client"
	"github.com/acharapko/pbench/db"
	"github.com/acharapko/pbench/idservice"
	"os"
	"strconv"
	"strings"
)

var id = flag.String("id", "", "node id this client connects to")
var algorithm = flag.String("algorithm", "", "Client API type [paxos]")

func usage() string {
	s := "Usage:\n"
	s += "\t get key\n"
	s += "\t put key value\n"
	s += "\t consensus key\n"
	s += "\t crash idservice time\n"
	s += "\t partition time ids...\n"
	s += "\t exit\n"
	return s
}

var rwclient client.Client
var admin client.AdminClient

func run(cmd string, args []string) {
	switch cmd {
	case "get":
		if len(args) < 1 {
			fmt.Println("get KEY")
			return
		}
		k, _ := strconv.Atoi(args[0])
		v, _ := rwclient.Get(db.Key(k))
		fmt.Println(string(v))

	case "put":
		if len(args) < 2 {
			fmt.Println("put KEY VALUE")
			return
		}
		k, _ := strconv.Atoi(args[0])
		rwclient.Put(db.Key(k), []byte(args[1]))
		//fmt.Println(string(v))

	case "consensus":
		if len(args) < 1 {
			fmt.Println("consensus KEY")
			return
		}
		k, _ := strconv.Atoi(args[0])
		v := admin.Consensus(db.Key(k))
		fmt.Println(v)

	case "crash":
		if len(args) < 2 {
			fmt.Println("crash idservice time(s)")
			return
		}
		id := idservice.NewIDFromString(args[0])
		time, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Println("second argument should be integer")
			return
		}
		admin.Crash(id, time)

	case "partition":
		if len(args) < 2 {
			fmt.Println("partition time ids...")
			return
		}
		time, err := strconv.Atoi(args[0])
		if err != nil {
			fmt.Println("time argument should be integer")
			return
		}
		ids := make([]idservice.ID, 0)
		for _, s := range args[1:] {
			ids = append(ids, idservice.NewIDFromString(s))
		}
		admin.Partition(time, ids...)

	case "exit":
		os.Exit(0)

	case "help":
		fallthrough
	default:
		fmt.Println(usage())
	}
}

func main() {
	pbench.Init()

	admin = client.NewClient(idservice.NewIDFromString(*id))

	switch *algorithm {
	case "paxos":
	default:
		rwclient = client.NewClient(idservice.NewIDFromString(*id))
	}

	if len(flag.Args()) > 0 {
		run(flag.Args()[0], flag.Args()[1:])
	} else {
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("pbench $ ")
			text, _ := reader.ReadString('\n')
			words := strings.Fields(text)
			if len(words) < 1 {
				continue
			}
			cmd := words[0]
			args := words[1:]

			run(cmd, args)
		}
	}
}
