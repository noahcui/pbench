package pbench

import (
	"flag"
	"github.com/acharapko/pbench/cfg"
	"github.com/acharapko/pbench/log"
)

// Init setup pbench package
func Init() {
	flag.Parse()
	log.Setup()
	cfg.GetConfig().Load()
}
