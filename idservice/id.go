package idservice

import (
	"github.com/acharapko/pbench/log"
	"runtime/debug"
	"strconv"
	"strings"
)

// NodeId represents a generic identifier in format of Zone.Node
type ID uint32

//type NodeId string

// NewID returns a new NodeId type given two int number of zone and node
func NewID(zone, node int) ID {
	return ID(zone<<16 | node)
}

func NewIDFromString(idstr string) ID {
	idParts := strings.Split(idstr, ".")
	if len(idParts) != 2 {
		debug.PrintStack()
		log.Errorf("Invalid idservice: %v", idstr)
		return 0
	}
	zone, err := strconv.Atoi(idParts[0])
	if err!= nil {
		log.Errorf("Invalid idservice: %v", idstr)
		return 0
	}

	node, err := strconv.Atoi(idParts[1])
	if err!= nil {
		log.Errorf("Invalid idservice: %v", idstr)
		return 0
	}

	return NewID(zone, node)
}

// Zone returns Zone NodeId component
func (i ID) Zone() int {
	return int(i>>16)
}

// Node returns Node NodeId component
func (i ID) Node() int {
	var z uint32 = 0x0000FFFF
	return int(z&(uint32(i)))
}

func (i ID) String() string {
	return strconv.Itoa(i.Zone()) + "." + strconv.Itoa(i.Node())
}

type IDs []ID

func (a IDs) Len() int      { return len(a) }
func (a IDs) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a IDs) Less(i, j int) bool {
	if a[i].Zone() < a[j].Zone() {
		return true
	} else if a[i].Zone() > a[j].Zone() {
		return false
	} else {
		return a[i].Node() < a[j].Node()
	}
}
