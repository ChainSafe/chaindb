// Copyright 2019 ChainSafe Systems (ON) Corp.
// This file is part of gossamer.
//
// The gossamer library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The gossamer library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the gossamer library. If not, see <http://www.gnu.org/licenses/>.

package chaindb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"sync"
)

// MemDatabase test memory database, data is not persisted
type MemDatabase struct {
	db   map[string][]byte
	lock sync.RWMutex
}

var _ Database = (*MemDatabase)(nil)

// NewMemDatabase returns an initialized mapping used for test database
func NewMemDatabase() *MemDatabase {
	return &MemDatabase{
		db: make(map[string][]byte),
	}
}

// Put puts the given key / value into the mapping
func (db *MemDatabase) Put(k []byte, v []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.db[string(k)] = v
	return nil
}

// Has checks the given key exists already; returning true or false
func (db *MemDatabase) Has(k []byte) (bool, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	_, ok := db.db[string(k)]
	return ok, nil
}

// Get returns the given key []byte
func (db *MemDatabase) Get(k []byte) ([]byte, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if v, ok := db.db[string(k)]; ok {
		return v, nil
	}
	return nil, ErrKeyNotFound
}

// Keys returns [][]byte of mapping keys
func (db *MemDatabase) Keys() [][]byte {
	db.lock.RLock()
	defer db.lock.RUnlock()

	keys := [][]byte{}
	for key := range db.db {
		keys = append(keys, []byte(key))
	}
	return keys
}

// Del removes the key from the mapping
func (db *MemDatabase) Del(key []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	delete(db.db, string(key))
	return nil
}

// Close ...
func (db *MemDatabase) Close() error {
	// do nothing
	return nil
}

// NewBatch ...
func (db *MemDatabase) NewBatch() Batch {
	return nil
}

// NewIterator ...
func (db *MemDatabase) NewIterator() Iterator {
	arr := make([][2][]byte, len(db.db))
	i := 0
	for k, v := range db.db {
		arr[i] = [2][]byte{[]byte(k), v}
		i++
	}
	return &MemDatabaseIterator{
		arr: arr,
		idx: -1,
	}
}

func (db *MemDatabase) NewIteratorWithPrefix(prefix []byte) Iterator {
	arr := [][2][]byte{}
	for k, v := range db.db {
		key := []byte(k)
		if bytes.Equal(key[:len(prefix)], prefix) {
			arr = append(arr, [2][]byte{removePrefix(key, prefix), v})
		}
	}
	return &MemDatabaseIterator{
		arr: arr,
		idx: -1,
	}
}

// Path ...
func (db *MemDatabase) Path() string {
	return fmt.Sprintf("&memDB=%p memDB=%v\n", db.db, db.db)
}

// Flush ...
func (db *MemDatabase) Flush() error {
	return nil
}

var _ Iterator = (*MemDatabaseIterator)(nil)

type MemDatabaseIterator struct {
	arr [][2][]byte
	idx int
}

func (iter *MemDatabaseIterator) Next() bool {
	if iter.idx >= len(iter.arr)-1 {
		return false
	}

	iter.idx++
	return true
}

func (iter *MemDatabaseIterator) Key() []byte {
	return iter.arr[iter.idx][0]
}

func (iter *MemDatabaseIterator) Value() []byte {
	return iter.arr[iter.idx][1]
}

func (iter *MemDatabaseIterator) Release() {}

func (db *MemDatabase) Subscribe(ctx context.Context, cb func(kv *badger.KVList) error, prefixes []byte) error {
	// TODO implement this
	panic("implement me")
}
