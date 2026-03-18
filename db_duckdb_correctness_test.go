//go:build duckdb

package badger

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

// TestDuckDBTimestampScenarios mirrors TestTimestampScenarios but uses the
// DuckDB backend so we verify that the DuckDB read path (pending-write buffer +
// SQL query) produces correct results across flush and compaction.
func TestDuckDBTimestampScenarios(t *testing.T) {
	type writeOp struct {
		ts  uint64
		key []byte
		val []byte // nil = delete
	}
	type readOp struct {
		ts      uint64
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
				{1, []byte("a"), []byte("v1")},
				{2, []byte("a"), []byte("v2")},
			},
			reads: []readOp{
				{1, []byte("a"), []byte("v1"), false},
				{2, []byte("a"), []byte("v2"), false},
				{math.MaxUint64, []byte("a"), []byte("v2"), false},
			},
		},
		{
			name: "parallel_overlapping_timestamps",
			writes: []writeOp{
				{5, []byte("b"), []byte("x")},
				{6, []byte("b"), []byte("y")},
			},
			reads: []readOp{
				{5, []byte("b"), []byte("x"), false},
				{6, []byte("b"), []byte("y"), false},
				{math.MaxUint64, []byte("b"), []byte("y"), false},
			},
		},
		{
			name: "read_snapshot_in_between",
			writes: []writeOp{
				{10, []byte("c"), []byte("v1")},
				{20, []byte("c"), []byte("v2")},
			},
			reads: []readOp{
				{15, []byte("c"), []byte("v1"), false},
				{25, []byte("c"), []byte("v2"), false},
			},
		},
		{
			name: "delete_semantics",
			writes: []writeOp{
				{30, []byte("d"), []byte("alive")},
				{40, []byte("d"), nil}, // tombstone
			},
			reads: []readOp{
				{35, []byte("d"), []byte("alive"), false},
				{45, []byte("d"), nil, true},
			},
		},
		{
			name: "cold_vs_hot_compaction",
			writes: func() []writeOp {
				var w []writeOp
				for i := 1; i <= 10; i++ {
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(i))
					w = append(w, writeOp{uint64(i + 50), key, []byte("cold")})
				}
				for i := 1; i <= 10; i++ {
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(i))
					w = append(w, writeOp{uint64(i + 200), key, []byte("hot")})
				}
				return w
			}(),
			reads: func() []readOp {
				key := make([]byte, 4)
				binary.BigEndian.PutUint32(key, uint32(1))
				return []readOp{
					{math.MaxUint64, key, []byte("hot"), false},
					{100, key, []byte("cold"), false}, // snapshot before "hot" writes
				}
			}(),
			triggerFlush: true,
		},
		{
			name: "interleaved_multi_key",
			writes: []writeOp{
				{300, []byte("e"), []byte("v1")},
				{301, []byte("f"), []byte("v2")},
				{302, []byte("e"), []byte("v3")},
			},
			reads: []readOp{
				{301, []byte("e"), []byte("v1"), false},
				{303, []byte("e"), []byte("v3"), false},
				{math.MaxUint64, []byte("f"), []byte("v2"), false},
			},
		},
		{
			name: "partitioned_fanout",
			writes: []writeOp{
				{400, []byte("p1:k"), []byte("A")},
				{401, []byte("p2:k"), []byte("B")},
			},
			reads: []readOp{
				{math.MaxUint64, []byte("p1:k"), []byte("A"), false},
				{math.MaxUint64, []byte("p2:k"), []byte("B"), false},
			},
			triggerFlush: true,
		},
		{
			name: "compaction_preserves_latest",
			writes: []writeOp{
				{500, []byte("g"), []byte("v500")},
				{600, []byte("g"), []byte("v600")},
				{700, []byte("g"), []byte("v700")},
			},
			reads: []readOp{
				{math.MaxUint64, []byte("g"), []byte("v700"), false},
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
				{800, []byte("h"), []byte("v800")},
				{801, []byte("h"), []byte("v801")},
			},
			reads: []readOp{
				{math.MaxUint64, []byte("h"), []byte("v801"), false},
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
						t.Fatalf("commit at ts=%d: %v", w.ts, err)
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
							t.Fatalf("ts=%d key=%q: expected error, got value", r.ts, r.key)
						}
						continue
					}
					if err != nil {
						t.Fatalf("ts=%d key=%q: unexpected error: %v", r.ts, r.key, err)
					}
					got, _ := itm.ValueCopy(nil)
					if !bytes.Equal(got, r.wantVal) {
						t.Fatalf("ts=%d key=%q: want %q, got %q", r.ts, r.key, r.wantVal, got)
					}
				}
			})
		}
	})
}
