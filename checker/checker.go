package main

import (
	"flag"
	"fmt"
	"github.com/acharapko/pbench/bench"
	"log"
)

var file = flag.String("log", "log.csv", "")

func main() {
	flag.Parse()

	h := bench.NewHistory()

	err := h.ReadFile(*file)
	if err != nil {
		log.Fatal(err)
	}

	n := h.Linearizable()

	fmt.Println(n)
}
