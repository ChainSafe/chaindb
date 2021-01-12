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
	"context"
	"os"
	"sync"

	log "github.com/ChainSafe/log15"
	"github.com/dgraph-io/badger/v2"
)

// BadgerDB contains directory path to data and db instance
type BadgerDB struct {
	config Config
	db     *badger.DB
	lock   sync.RWMutex
}

type KVList = badger.KVList

var _ Database = (*BadgerDB)(nil)

// Config defines configurations for BadgerService instance
type Config struct {
	DataDir string
}

// NewBadgerDB initializes badgerDB instance
func NewBadgerDB(file string) (*BadgerDB, error) {
	opts := badger.DefaultOptions(file)
	opts.ValueDir = file
	opts.Logger = nil
	opts.WithSyncWrites(false)
	opts.WithNumCompactors(20)

	if err := os.MkdirAll(file, os.ModePerm); err != nil {
		return nil, err
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &BadgerDB{
		config: Config{
			DataDir: file,
		},
		db: db,
	}, nil
}

// Path returns the path to the database directory.
func (db *BadgerDB) Path() string {
	return db.config.DataDir
}

// Batch struct contains a database instance, key-value mapping for batch writes and length of item value for batch write
type batchWriter struct {
	db   *BadgerDB
	b    map[string][]byte
	size int
	lock sync.RWMutex
}

// NewBatch returns batchWriter with a badgerDB instance and an initialized mapping
func (db *BadgerDB) NewBatch() Batch {
	return &batchWriter{
		db: db,
		b:  make(map[string][]byte),
	}
}

// Put puts the given key / value to the queue
func (db *BadgerDB) Put(key []byte, value []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		return err
	})
}

// Has checks the given key exists already; returning true or false
func (db *BadgerDB) Has(key []byte) (exists bool, err error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	err = db.db.View(func(txn *badger.Txn) error {
		item, errr := txn.Get(key)
		if item != nil {
			exists = true
		}
		if errr == badger.ErrKeyNotFound {
			exists = false
			errr = nil
		}
		return errr
	})
	return exists, err
}

// Get returns the given key
func (db *BadgerDB) Get(key []byte) (data []byte, err error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	err = db.db.View(func(txn *badger.Txn) error {
		item, e := txn.Get(key)
		if e != nil {
			return e
		}
		data, e = item.ValueCopy(nil)
		if e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Del removes the key from the queue and database
func (db *BadgerDB) Del(key []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		if err == badger.ErrKeyNotFound {
			err = nil
		}
		return err
	})
}

// Flush commits pending writes to disk
func (db *BadgerDB) Flush() error {
	return db.db.Sync()
}

// Close closes a DB
func (db *BadgerDB) Close() error {
	db.lock.Lock()
	defer db.lock.Unlock()
	if err := db.db.Close(); err != nil {
		return err
	}
	return nil
}

// Subscribe to watch for changes for the given prefixes
func (db *BadgerDB) Subscribe(ctx context.Context, cb func(kv *KVList) error, prefixes []byte) error {
	return db.db.Subscribe(ctx, cb, prefixes)
}

// BadgerIterator struct contains a transaction, iterator and init.
type BadgerIterator struct {
	txn  *badger.Txn
	iter *badger.Iterator
	init bool
	lock sync.RWMutex
}

// NewIterator returns a new iterator within the Iterator struct along with a new transaction
func (db *BadgerDB) NewIterator() Iterator {
	txn := db.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	iter := txn.NewIterator(opts)
	return &BadgerIterator{
		txn:  txn,
		iter: iter,
	}
}

// Release closes the iterator and discards the created transaction.
func (i *BadgerIterator) Release() {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.iter.Close()
	i.txn.Discard()
}

// Next rewinds the iterator to the zero-th position if uninitialized, and then will advance the iterator by one
// returns bool to ensure access to the item
func (i *BadgerIterator) Next() bool {
	i.lock.RLock()
	defer i.lock.RUnlock()

	if !i.init {
		i.iter.Rewind()
		i.init = true
		return i.iter.Valid()
	}

	if !i.iter.Valid() {
		return false
	}
	i.iter.Next()
	return i.iter.Valid()
}

// Seek will look for the provided key if present and go to that position. If
// absent, it would seek to the next smallest key
func (i *BadgerIterator) Seek(key []byte) {
	i.lock.RLock()
	defer i.lock.RUnlock()
	i.iter.Seek(key)
}

// Key returns an item key
func (i *BadgerIterator) Key() []byte {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.iter.Item().Key()
}

// Value returns a copy of the value of the item
func (i *BadgerIterator) Value() []byte {
	i.lock.RLock()
	defer i.lock.RUnlock()
	val, err := i.iter.Item().ValueCopy(nil)
	if err != nil {
		log.Warn("value retrieval error ", "error", err)
	}
	return val
}

// Put encodes key-values and adds them to a mapping for batch writes, sets the size of item value
func (b *batchWriter) Put(key, value []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.b[string(key)] = value
	b.size += len(value)
	return nil
}

// Flush commits pending writes to disk
func (b *batchWriter) Flush() error {
	b.lock.Lock()
	defer b.lock.Unlock()
	wb := b.db.db.NewWriteBatch()
	defer wb.Cancel()

	for k, v := range b.b {
		if err := wb.Set([]byte(k), v); err != nil {
			return err
		}
	}
	return wb.Flush()
}

// ValueSize returns the amount of data in the batch
func (b *batchWriter) ValueSize() int {
	return b.size
}

// Del removes the key from the batch and database
func (b *batchWriter) Del(key []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	if err := b.db.db.NewWriteBatch().Delete(key); err != nil {
		return err
	}
	b.size++
	return nil
}

// Reset clears batch key-values and resets the size to zero
func (b *batchWriter) Reset() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.b = make(map[string][]byte)
	b.size = 0
}
