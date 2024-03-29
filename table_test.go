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
	"fmt"
	"reflect"
	"testing"
)

func TestBadgerDB_TablePrefixOps(t *testing.T) {
	db := newTestBadgerDB(t)

	testPutTablesWithPrefix(db, t)
	testHasTablesWithPrefix(db, t)
	testDelTablesWithPrefix(db, t)
	testTableClose(db, t)
	testNewTableBatch(db, t)
}

func TestTable_Batch(t *testing.T) {
	db := newTestBadgerDB(t)
	tbl := NewTable(db, "random")
	testBatchPutAndDelete(t, tbl)
}

func testPutTablesWithPrefix(db Database, t *testing.T) {
	data := testKVData()
	ops := NewTable(db, "99")

	for _, v := range data {
		err := ops.Put([]byte(v.input), []byte(v.expected))
		if err != nil {
			t.Fatalf("put failed: %v", err)
		}
		data, err := ops.Get([]byte(v.input))
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		if !bytes.Equal(data, []byte(v.expected)) {
			t.Fatalf("get returned wrong result, got %q expected %q", string(data), v.expected)
		}
	}
}

func testHasTablesWithPrefix(db Database, t *testing.T) {
	data := testKVData()
	ops := NewTable(db, "99")

	for _, v := range data {
		exists, err := ops.Has([]byte(v.input))
		if err != nil {
			t.Fatalf("has operation failed: %v", err)
		}
		if !exists {
			t.Fatalf("has operation returned wrong result, got %t expected %t", exists, true)
		}
	}
}

func testDelTablesWithPrefix(db Database, t *testing.T) {
	data := testKVData()
	ops := NewTable(db, "99")

	for _, v := range data {
		err := ops.Del([]byte(v.input))
		if err != nil {
			t.Fatalf("delete %q failed: %v", v.input, err)
		}
		d, _ := ops.Get([]byte(v.input))
		if len(d) > 1 {
			t.Fatalf("failed to delete value %q", v.input)
		}
	}
}

func testTableClose(db Database, t *testing.T) {
	ops := NewTable(db, "99")

	err := ops.Close()
	if err != nil {
		t.Fatalf("get returned wrong result, got %v expected %v", err, true)
	}
}

func testNewTableBatch(db Database, t *testing.T) {
	ops := NewTable(db, "99")
	b := ops.NewBatch()

	_, ok := b.(Batch)
	if !ok {
		t.Fatalf("get returned wrong result, got %v", ok)
	}
}

func TestBadgerDB_TableBatchWithPrefix(t *testing.T) {
	db := newTestBadgerDB(t)
	testBatchTablePutWithPrefix(db, t)
}

func batchTableWithPrefixTestSetup(db Database) (func(i int) []byte, func(i int) []byte, Batch) {
	testKey := func(i int) []byte {
		return []byte(fmt.Sprintf("%04d", i))
	}
	testValue := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	b := NewTableBatch(db, "98")
	return testKey, testValue, b
}

func testBatchTablePutWithPrefix(db Database, t *testing.T) {
	k, v, b := batchTableWithPrefixTestSetup(db)

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

func TestTable_IteratorWithPrefix(t *testing.T) {
	db := newTestBadgerDB(t)
	_ = db.Put([]byte("noot"), []byte("washere"))
	tbl := NewTable(db, "test")

	tests := [][]byte{
		[]byte("asdfg"),
		[]byte("asghj"),
		[]byte("ertyq"),
		[]byte("uiopl"),
	}

	for _, k := range tests {
		_ = tbl.Put(k, k)
	}

	iter := tbl.NewIterator()
	res := [][]byte{iter.Key()}
	for iter.Next() {
		res = append(res, iter.Key())
	}

	if !reflect.DeepEqual(tests, res) {
		t.Fatalf("iterator did not return expected keys: got %v, expected %v", res, tests)
	}
}
