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
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type data struct {
	input    string
	expected string
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	rand.Seed(time.Now().UnixNano())
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func testSetup() []data {
	tests := []data{
		{"camel", "camel"},
		{"walrus", "walrus"},
		{"296204", "296204"},
		{"\x00123\x00", "\x00123\x00"},
	}
	return tests
}

func TestBadgerDB_PutGetDel(t *testing.T) {
	db := newTestBadgerDB(t)

	testPutGetter(db, t)
	testHasGetter(db, t)
	testUpdateGetter(db, t)
	testDelGetter(db, t)
	testGetPath(db, t)
}

func testCompressSetup(t *testing.T) map[string]string {
	t.Helper()
	numEntries := 1000
	testMap := make(map[string]string, numEntries)

	var key string
	var val = randSeq(1 << 15)
	for i := 0; i < numEntries; i++ {
		key = randSeq(64)
		testMap[key] = val
	}
	return testMap
}

func dirSizeMB(t *testing.T, path string) float64 {
	t.Helper()
	var dirSize int64 = 0
	readSize := func(path string, file os.FileInfo, err error) error {
		if !file.IsDir() {
			dirSize += file.Size()
		}
		return nil
	}

	filepath.Walk(path, readSize)
	sizeMB := float64(dirSize) / 1024.0 / 1024.0
	return sizeMB
}

func TestDBCompression(t *testing.T) {
	var tests = testCompressSetup(t)
	test := func(t *testing.T, compress bool, folderName string) {
		db := newBadgerCompressed(t, compress, folderName)
		defer os.RemoveAll(folderName)

		cmpStart := time.Now()
		for k, v := range tests {
			err := db.Put([]byte(k), []byte(v))
			require.NoError(t, err)
		}
		cmpDuration := time.Since(cmpStart)
		t.Logf("%s DB duration in seconds %f", folderName, cmpDuration.Seconds())

		err := db.Close()
		require.NoError(t, err)

		size := dirSizeMB(t, folderName)
		require.NoError(t, err)
		t.Logf("%s DB size in MB %f", folderName, size)
	}

	t.Run("compressed data", func(t *testing.T) {
		test(t, true, "compressed")
	})
	t.Run("uncompressed data", func(t *testing.T) {
		test(t, false, "uncompressed")
	})
}

func testPutGetter(db Database, t *testing.T) {
	tests := testSetup()
	for _, v := range tests {
		err := db.Put([]byte(v.input), []byte(v.input))
		if err != nil {
			t.Fatalf("put failed: %v", err)
		}
		data, err := db.Get([]byte(v.input))
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		if !bytes.Equal(data, []byte(v.expected)) {
			t.Fatalf("get returned wrong result, got %q expected %q", string(data), v.expected)
		}
	}
}

func testHasGetter(db Database, t *testing.T) {
	tests := testSetup()

	for _, v := range tests {
		exists, err := db.Has([]byte(v.input))
		if err != nil {
			t.Fatalf("has operation failed: %v", err)
		}
		if !exists {
			t.Fatalf("has operation returned wrong result, got %t expected %t", exists, true)
		}
	}
}

func testUpdateGetter(db Database, t *testing.T) {
	tests := testSetup()

	for _, v := range tests {
		err := db.Put([]byte(v.input), []byte("?"))
		if err != nil {
			t.Fatalf("put override failed: %v", err)
		}
		data, err := db.Get([]byte(v.input))
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		if !bytes.Equal(data, []byte("?")) {
			t.Fatalf("get returned wrong result, got %q expected ?", string(data))
		}
	}
}

func testDelGetter(db Database, t *testing.T) {
	tests := testSetup()

	for _, v := range tests {
		err := db.Del([]byte(v.input))
		if err != nil {
			t.Fatalf("delete %q failed: %v", v.input, err)
		}
		d, _ := db.Get([]byte(v.input))
		if len(d) > 1 {
			t.Fatalf("failed to delete value %q", v.input)
		}
	}
}

func testGetPath(db Database, t *testing.T) {
	dir := db.Path()
	if len(dir) <= 0 {
		t.Fatalf("failed to set database path")
	}
}

func TestBadgerDB_Batch(t *testing.T) {
	db := newTestBadgerDB(t)
	testBatchPut(db, t)
}

func batchTestSetup(db Database) (func(i int) []byte, func(i int) []byte, Batch) {
	testKey := func(i int) []byte {
		return []byte(fmt.Sprintf("%04d", i))
	}
	testValue := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	b := db.NewBatch()
	return testKey, testValue, b
}

func testBatchPut(db Database, t *testing.T) {
	k, v, b := batchTestSetup(db)

	for i := 0; i < 1000; i++ {
		err := b.Put(k(i), v(i))
		if err != nil {
			t.Fatalf("failed to add key-value to batch mapping  %q", err)
		}
		err = b.Flush()
		if err != nil {
			t.Fatalf("failed to flush batch %q", err)
		}
		size := b.ValueSize()
		if size == 0 {
			t.Fatalf("failed to set size of data in each batch, got %v", size)
		}
		err = b.Del(k(i))
		if err != nil {
			t.Fatalf("failed to delete batch key %v", k(i))
		}
		b.Reset()
		if b.ValueSize() != 0 {
			t.Fatalf("failed to reset batch mapping to zero, got %v, expected %v", b.ValueSize(), 0)
		}
	}
}

func TestBadgerDB_Iterator(t *testing.T) {
	db := newTestBadgerDB(t)

	testNextKeyIterator(db, t)
	testSeekKeyValueIterator(db, t)
}

func testIteratorSetup(db Database, t *testing.T) {
	k, v, b := batchTestSetup(db)

	for i := 0; i < 5; i++ {
		err := b.Put(k(i), v(i))
		if err != nil {
			t.Fatalf("failed to add key-value to batch mapping  %q", err)
		}
		err = b.Flush()
		if err != nil {
			t.Fatalf("failed to flush batch %q", err)
		}
	}
}

func testNextKeyIterator(db Database, t *testing.T) {
	testIteratorSetup(db, t)

	it := db.NewIterator()
	defer it.Release()

	ok := it.Next()
	if !ok {
		t.Fatalf("failed to rewind the iterator to the zero-th position")
	}
	for it.Next() {
		if it.Key() == nil {
			t.Fatalf("failed to retrieve keys %v", it.Key())
		}
	}
}

func testKVData() []data {
	testKeyValue := []data{
		{"0003", "00003"},
		{"0001", "00001"},
		{"0002", "00002"},
		{"0000", "00000"},
		{"0004", "00004"},
	}
	return testKeyValue
}

func testSeekKeyValueIterator(db Database, t *testing.T) {
	testIteratorSetup(db, t)
	kv := testKVData()

	it := db.NewIterator().(*BadgerIterator)
	defer it.Release()

	for _, k := range kv {
		it.Seek([]byte(k.input))
		if !bytes.Equal(it.Key(), []byte(k.input)) {
			t.Fatalf("failed to retrieve presented key, got %v, expected %v", it.Key(), k.input)
		}
		it.Seek([]byte(k.input))
		if !bytes.Equal(it.Value(), []byte(k.expected)) {
			t.Fatalf("failed to retrieve presented key, got %v, expected %v", it.Key(), k.expected)
		}
	}
}

func newTestBadgerDB(t *testing.T) Database {
	dir, err := ioutil.TempDir("", "test_data")
	if err != nil {
		t.Fatal("failed to create temp dir: " + err.Error())
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})

	cfg := &Config{
		DataDir: dir,
	}

	db, err := NewBadgerDB(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func newBadgerCompressed(t *testing.T, compress bool, filePath string) Database {
	t.Helper()

	currentDir, err := os.Getwd()
	require.NoError(t, err)

	fp := filepath.Join(currentDir, fmt.Sprintf("../%s", filePath))
	os.RemoveAll(filepath.Clean(fp))

	_, err = os.Create(filepath.Clean(fp))
	require.NoError(t, err)

	cfg := &Config{
		DataDir:  filePath,
		Compress: compress,
		InMemory: false,
	}

	db, err := NewBadgerDB(cfg)
	require.NoError(t, err)

	return db
}

func ExampleDB_Subscribe() {
	prefix := []byte{'a'}

	// This key should be printed, since it matches the prefix.
	aKey := []byte("a-key")
	aValue := []byte("a-value")

	// This key should not be printed.
	bKey := []byte("b-key")
	bValue := []byte("b-value")

	// Open the DB.
	dir, err := ioutil.TempDir("", "badger-test")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			log.Fatal(err)
		}
	}()
	db, err := NewBadgerDB(&Config{
		DataDir: dir,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create the context here so we can cancel it after sending the writes.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use the WaitGroup to make sure we wait for the subscription to stop before continuing.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cb := func(kvs *KVList) error {
			for _, kv := range kvs.Kv {
				fmt.Printf("%s is now set to %s\n", kv.Key, kv.Value)
			}
			return nil
		}
		if err := db.Subscribe(ctx, cb, prefix); err != nil && err != context.Canceled {
			log.Fatal(err)
		}
		log.Printf("subscription closed")
	}()

	// Write both keys, but only one should be printed in the Output.
	err = db.Put(aKey, aValue)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Put(bKey, bValue)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("stopping subscription")
	cancel()
	log.Printf("waiting for subscription to close")
	wg.Wait()
	// Output:
	// a-key is now set to a-value

}
