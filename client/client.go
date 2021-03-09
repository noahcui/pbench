package client

import (
	"github.com/acharapko/pbench/db"
	"github.com/acharapko/pbench/idservice"
)

// Client interface provides get and put for key value store
type Client interface {
	Get(db.Key) (db.Value, error)
	//GetN(k db.Key, n int) (db.Value, error)
	Put(db.Key, db.Value) error
	//PutN(k db.Key, v db.Value, n int) error
}

// AdminClient interface provides fault injection opeartion
type AdminClient interface {
	Consensus(db.Key) bool
	Crash(idservice.ID, int)
	Drop(idservice.ID, idservice.ID, int)
	Partition(int, ...idservice.ID)
}
