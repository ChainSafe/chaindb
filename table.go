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

	log "github.com/ChainSafe/log15"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
)

type table struct {
	db     Database
	prefix string
}

var _ Database = (*table)(nil)
var _ Iterator = (*tableIterator)(nil)

type tableBatch struct {
	batch  Batch
	prefix string
}

// NewTable returns a Database object that prefixes all keys with a given
// string.
func NewTable(db Database, prefix string) Database {
	return &table{db: db, prefix: prefix}
}

// ClearAll - This method is not implemented for table. Use delete instead.
func (dt *table) ClearAll() error {
	return fmt.Errorf("this method is not implemented for table")
}

// Put adds keys with the prefix value given to NewTable
func (dt *table) Put(key []byte, value []byte) error {
	return dt.db.Put(append([]byte(dt.prefix), key...), value)
}

// Has checks keys with the prefix value given to NewTable
func (dt *table) Has(key []byte) (bool, error) {
	return dt.db.Has(append([]byte(dt.prefix), key...))
}

// Get retrieves keys with the prefix value given to NewTable
func (dt *table) Get(key []byte) ([]byte, error) {
	return dt.db.Get(append([]byte(dt.prefix), key...))
}

// Del removes keys with the prefix value given to NewTable
func (dt *table) Del(key []byte) error {
	return dt.db.Del(append([]byte(dt.prefix), key...))
}

// Flush commits pending writes to disk
func (dt *table) Flush() error {
	return dt.db.Flush()
}

// Close closes table db
func (dt *table) Close() error {
	return dt.db.Close()
}

// NewIterator initializes type Iterator
func (dt *table) NewIterator() Iterator {
	if db, ok := dt.db.(*BadgerDB); ok {
		db.lock.Lock()
		defer db.lock.Unlock()

		txn := db.db.NewTransaction(false)
		opts := badger.DefaultIteratorOptions
		iter := txn.NewIterator(opts)
		tableIter := &tableIterator{
			prefix: []byte(dt.prefix),
		}
		tableIter.txn = txn
		tableIter.iter = iter
		iter.Rewind()
		iter.Seek([]byte(dt.prefix))
		return tableIter
	}

	return nil
}

// Path returns table prefix
func (dt *table) Path() string {
	return dt.prefix
}

func removePrefix(key, prefix []byte) []byte {
	if bytes.Equal(key[:len(prefix)], prefix) {
		return key[len(prefix):]
	}

	return key
}

type tableIterator struct {
	BadgerIterator
	prefix []byte
}

// Release closes the iterator and discards the created transaction.
func (i *tableIterator) Release() {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.iter.Close()
	i.txn.Discard()
}

// Valid returns whether the current iterator position has an item.
func (i *tableIterator) Valid() bool {
	return i.iter.Valid()
}

// Next rewinds the iterator to the zero-th position if uninitialized, and then will advance the iterator by one
// returns bool to ensure access to the item
func (i *tableIterator) Next() bool {
	i.lock.RLock()
	defer i.lock.RUnlock()

	for {
		if !i.iter.Valid() {
			break
		}

		i.iter.Next()
		if i.iter.ValidForPrefix(i.prefix) {
			return true
		}
	}

	return false
}

// Seek will look for the provided key if present and go to that position. If
// absent, it would seek to the next smallest key
func (i *tableIterator) Seek(key []byte) {
	i.lock.RLock()
	defer i.lock.RUnlock()
	i.iter.Seek(key)
}

func (i *tableIterator) rawKey() []byte {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.iter.Item().Key()
}

// Key returns an item key without the table prefix
func (i *tableIterator) Key() []byte {
	return removePrefix(i.rawKey(), i.prefix)
}

// Value returns a copy of the value of the item
func (i *tableIterator) Value() []byte {
	i.lock.RLock()
	defer i.lock.RUnlock()
	val, err := i.iter.Item().ValueCopy(nil)
	if err != nil {
		log.Warn("value retrieval error ", "error", err)
	}
	return val
}

// NewTableBatch returns a Batch object which prefixes all keys with a given string.
func NewTableBatch(db Database, prefix string) Batch {
	return &tableBatch{db.NewBatch(), prefix}
}

// NewBatch returns tableBatch with a Batch type and the given prefix
func (dt *table) NewBatch() Batch {
	return &tableBatch{dt.db.NewBatch(), dt.prefix}
}

// Put encodes key-values with prefix given to NewBatchTable and adds them to a mapping for batch writes, sets the size of item value
func (tb *tableBatch) Put(key, value []byte) error {
	return tb.batch.Put(append([]byte(tb.prefix), key...), value)
}

// Flush performs batched writes with the provided prefix
func (tb *tableBatch) Flush() error {
	return tb.batch.Flush()
}

// ValueSize returns the amount of data in the batch accounting for the given prefix
func (tb *tableBatch) ValueSize() int {
	return tb.batch.ValueSize()
}

// Reset clears batch key-values and resets the size to zero
func (tb *tableBatch) Reset() {
	tb.batch.Reset()
}

// Del removes the key from the batch and database
func (tb *tableBatch) Del(k []byte) error {
	return tb.batch.Del(append([]byte(tb.prefix), k...))
}

func (dt *table) Subscribe(ctx context.Context, cb func(kv *KVList) error, prefixes []pb.Match) error {
	return dt.db.Subscribe(ctx, cb, prefixes)
}
