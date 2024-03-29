package chaindb

import (
	"github.com/dgraph-io/badger/v4"
)

// ErrKeyNotFound is returned if there is a database get for a key that does not exist
var ErrKeyNotFound = badger.ErrKeyNotFound
