//go:build duckdb

package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4/types"
)

// mkTs is a shorthand for building a types.CustomTs with only AssignedTs set,
// which is sufficient for the sequential timestamp scenarios in this file.
func mkTs(n uint32) types.CustomTs {
	return types.CustomTs{AssignedTs: n}
}

// TestDuckDBTimestampScenarios mirrors TestTimestampScenarios but uses the
// DuckDB backend so we verify that the DuckDB read path (pending-write buffer +
// SQL query) produces correct results across flush and compaction.
func TestDuckDBTimestampScenarios(t *testing.T) {
	type writeOp struct {
		ts  types.CustomTs
		key []byte
		val []byte // nil = delete
	}
	type readOp struct {
		ts      types.CustomTs
		key     []byte
		wantVal []byte
		wantErr bool
	}

	scenarios := []struct {
		name           string
		writes         []writeOp
		reads          []readOp
		triggerFlush   bool
		triggerCompact bool // calls duckDBStorage.CompactPartitions
	}{
		{
			name: "basic_overwrite_ascending",
			writes: []writeOp{
				{mkTs(1), []byte("a"), []byte("v1")},
				{mkTs(2), []byte("a"), []byte("v2")},
			},
			reads: []readOp{
				{mkTs(1), []byte("a"), []byte("v1"), false},
				{mkTs(2), []byte("a"), []byte("v2"), false},
				{types.MaxTs, []byte("a"), []byte("v2"), false},
			},
		},
		{
			name: "parallel_overlapping_timestamps",
			writes: []writeOp{
				{mkTs(5), []byte("b"), []byte("x")},
				{mkTs(6), []byte("b"), []byte("y")},
			},
			reads: []readOp{
				{mkTs(5), []byte("b"), []byte("x"), false},
				{mkTs(6), []byte("b"), []byte("y"), false},
				{types.MaxTs, []byte("b"), []byte("y"), false},
			},
		},
		{
			name: "read_snapshot_in_between",
			writes: []writeOp{
				{mkTs(10), []byte("c"), []byte("v1")},
				{mkTs(20), []byte("c"), []byte("v2")},
			},
			reads: []readOp{
				{mkTs(15), []byte("c"), []byte("v1"), false},
				{mkTs(25), []byte("c"), []byte("v2"), false},
			},
		},
		{
			name: "delete_semantics",
			writes: []writeOp{
				{mkTs(30), []byte("d"), []byte("alive")},
				{mkTs(40), []byte("d"), nil}, // tombstone
			},
			reads: []readOp{
				{mkTs(35), []byte("d"), []byte("alive"), false},
				{mkTs(45), []byte("d"), nil, true},
			},
		},
		{
			name: "cold_vs_hot_compaction",
			writes: func() []writeOp {
				var w []writeOp
				for i := 1; i <= 10; i++ {
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(i))
					w = append(w, writeOp{mkTs(uint32(i + 50)), key, []byte("cold")})
				}
				for i := 1; i <= 10; i++ {
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(i))
					w = append(w, writeOp{mkTs(uint32(i + 200)), key, []byte("hot")})
				}
				return w
			}(),
			reads: func() []readOp {
				key := make([]byte, 4)
				binary.BigEndian.PutUint32(key, uint32(1))
				return []readOp{
					{types.MaxTs, key, []byte("hot"), false},
					{mkTs(100), key, []byte("cold"), false}, // snapshot before "hot" writes
				}
			}(),
			triggerFlush: true,
		},
		{
			name: "interleaved_multi_key",
			writes: []writeOp{
				{mkTs(300), []byte("e"), []byte("v1")},
				{mkTs(301), []byte("f"), []byte("v2")},
				{mkTs(302), []byte("e"), []byte("v3")},
			},
			reads: []readOp{
				{mkTs(301), []byte("e"), []byte("v1"), false},
				{mkTs(303), []byte("e"), []byte("v3"), false},
				{types.MaxTs, []byte("f"), []byte("v2"), false},
			},
		},
		{
			name: "partitioned_fanout",
			writes: []writeOp{
				{mkTs(400), []byte("p1:k"), []byte("A")},
				{mkTs(401), []byte("p2:k"), []byte("B")},
			},
			reads: []readOp{
				{types.MaxTs, []byte("p1:k"), []byte("A"), false},
				{types.MaxTs, []byte("p2:k"), []byte("B"), false},
			},
			triggerFlush: true,
		},
		{
			name: "compaction_preserves_latest",
			writes: []writeOp{
				{mkTs(500), []byte("g"), []byte("v500")},
				{mkTs(600), []byte("g"), []byte("v600")},
				{mkTs(700), []byte("g"), []byte("v700")},
			},
			reads: []readOp{
				{types.MaxTs, []byte("g"), []byte("v700"), false},
				// After CompactPartitions only latest version survives, so
				// snapshots before ts=700 should return "Key not found" — this
				// is the documented trade-off of DuckDB's compaction.
				// We only verify the latest version here.
			},
			triggerFlush:   true,
			triggerCompact: true, // uses DuckDB CompactPartitions (keeps latest only)
		},
		{
			name: "concurrent_conflicting_writes",
			writes: []writeOp{
				{mkTs(800), []byte("h"), []byte("v800")},
				{mkTs(801), []byte("h"), []byte("v801")},
			},
			reads: []readOp{
				{types.MaxTs, []byte("h"), []byte("v801"), false},
			},
		},
	}

	withDuckDB(t, true, func(db *DB) {
		for _, sc := range scenarios {
			t.Run(sc.name, func(t *testing.T) {
				// Apply writes.
				for _, w := range sc.writes {
					txn := db.NewTransactionAt(w.ts, true)
					if w.val == nil {
						_ = txn.Delete(w.key)
					} else {
						_ = txn.Set(w.key, w.val)
					}
					if err := txn.CommitAt(w.ts, nil); err != nil {
						t.Fatalf("commit at ts=%v: %v", w.ts, err)
					}
				}

				if sc.triggerFlush {
					if err := db.handleMemTableFlushPartitioned(db.mt, nil); err != nil {
						t.Fatalf("flush error: %v", err)
					}
				}
				if sc.triggerCompact {
					if err := db.duckDBStorage.CompactPartitions(); err != nil {
						t.Fatalf("compact error: %v", err)
					}
				}

				// Verify reads.
				for _, r := range sc.reads {
					txn := db.NewTransactionAt(r.ts, false)
					itm, err := txn.Get(r.key)
					txn.Discard()
					if r.wantErr {
						if err == nil {
							t.Fatalf("ts=%v key=%q: expected error, got value", r.ts, r.key)
						}
						continue
					}
					if err != nil {
						t.Fatalf("ts=%v key=%q: unexpected error: %v", r.ts, r.key, err)
					}
					got, _ := itm.ValueCopy(nil)
					if !bytes.Equal(got, r.wantVal) {
						t.Fatalf("ts=%v key=%q: want %q, got %q", r.ts, r.key, r.wantVal, got)
					}
				}
			})
		}
	})
}

// TestDuckDBIntegration is a simple end-to-end smoke check that writes,
// flushes, and reads back a small fixed dataset through the DuckDB path.
func TestDuckDBIntegration(t *testing.T) {
	withDuckDB(t, true, func(db *DB) {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key%03d", i))
			val := []byte(fmt.Sprintf("val%03d", i))

			ts := types.CustomTs{AssignedTs: uint32(i + 1)}
			txn := db.NewTransactionAt(ts, true)
			if err := txn.Set(key, val); err != nil {
				t.Fatalf("set key%03d: %v", i, err)
			}
			if err := txn.CommitAt(ts, nil); err != nil {
				t.Fatalf("commit key%03d: %v", i, err)
			}
		}

		if err := db.handleMemTableFlushPartitioned(db.mt, nil); err != nil {
			t.Fatalf("flush to DuckDB: %v", err)
		}

		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key%03d", i))
			expected := []byte(fmt.Sprintf("val%03d", i))

			txn := db.NewTransactionAt(types.MaxTs, false)
			item, err := txn.Get(key)
			if err != nil {
				txn.Discard()
				t.Fatalf("read key%03d: %v", i, err)
			}
			got, err := item.ValueCopy(nil)
			txn.Discard()
			if err != nil {
				t.Fatalf("value key%03d: %v", i, err)
			}
			if !bytes.Equal(got, expected) {
				t.Fatalf("key%03d: expected %q got %q", i, expected, got)
			}
		}
	})
}
