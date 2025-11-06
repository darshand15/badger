package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// withDB opens a DB for the duration of fn. Works for *testing.T and *testing.B.
func withDB(tb testing.TB, managed bool, fn func(db *DB)) {
	tb.Helper()

	// Use an isolated temp dir for each test/bench.
	opts := DefaultOptions(tb.TempDir())
	// Keep it simple/fast and reduce background noise.
	opts.NumCompactors = 0
	opts.CompactL0OnClose = false

	var (
		db  *DB
		err error
	)
	if managed {
		db, err = OpenManaged(opts)
	} else {
		db, err = Open(opts)
	}
	if err != nil {
		tb.Fatalf("open DB: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	fn(db)
}
func logLatest(t *testing.T, db *DB, key []byte) {
	t.Helper()

	// Read at "now" to see the latest committed version.
	rtxn := db.NewTransactionAt(math.MaxUint64, false)
	defer rtxn.Discard()

	it, err := rtxn.Get(key)
	if err != nil {
		t.Fatalf("get latest for %q: %v", key, err)
	}
	v, err := it.ValueCopy(nil)
	if err != nil {
		t.Fatalf("read value for %q: %v", key, err)
	}
	// Item.Version() gives the Badger version (timestamp) for the item.
	t.Logf("LATEST key=%q ts=%d val=%q", key, it.Version(), v)
}

// --- 1) Correctness: latest timestamp wins for a key.

func TestLatestWins(t *testing.T) {
	withDB(t, true, func(db *DB) {
		k := []byte("k")
		v1 := []byte("v1")
		v2 := []byte("v2")

		// Write at ts=1
		{
			ts := uint64(1)
			txn := db.NewTransactionAt(ts, true)
			if err := txn.Set(k, v1); err != nil {
				t.Fatalf("set v1: %v", err)
			}
			if err := txn.CommitAt(ts, nil); err != nil {
				t.Fatalf("commit v1: %v", err)
			}
		}
		// Write at ts=2
		{
			ts := uint64(2)
			txn := db.NewTransactionAt(ts, true)
			if err := txn.Set(k, v2); err != nil {
				t.Fatalf("set v2: %v", err)
			}
			if err := txn.CommitAt(ts, nil); err != nil {
				t.Fatalf("commit v2: %v", err)
			}
		}
		logLatest(t, db, k)

		// Read at max timestamp—should see v2.
		rtx := db.NewTransactionAt(math.MaxUint64, false)
		itm, err := rtx.Get(k)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		var got []byte
		if err := itm.Value(func(v []byte) error {
			got = append([]byte{}, v...)
			return nil
		}); err != nil {
			t.Fatalf("read value: %v", err)
		}
		if !bytes.Equal(got, []byte("v2")) {
			t.Fatalf("latest-wins failed: expected v2, got %q", got)
		}
	})

}

// --- 2) Writers are non-blocking (smoke test):
// Spin many writers concurrently; ensure forward progress (commits happen).

func TestWritersAreNonBlocking(t *testing.T) {
	withDB(t, false, func(db *DB) {
		var (
			stop    int32
			commits uint64
			wg      sync.WaitGroup
		)

		nG := 2 * runtime.GOMAXPROCS(0)
		wg.Add(nG)

		for g := 0; g < nG; g++ {
			go func(id int) {
				defer wg.Done()
				k := []byte(fmt.Sprintf("k-%d", id))
				for atomic.LoadInt32(&stop) == 0 {
					txn := db.NewTransaction(true)
					// We intentionally ignore errors here to keep the smoke test simple.
					_ = txn.Set(k, []byte("x"))
					if err := txn.Commit(); err == nil {
						atomic.AddUint64(&commits, 1)
					}
				}
			}(g)
		}

		// Let writers run briefly.
		time.Sleep(2 * time.Second)
		atomic.StoreInt32(&stop, 1)
		wg.Wait()

		if atomic.LoadUint64(&commits) == 0 {
			t.Fatalf("no commits recorded; writers appear stalled")
		}
	})
}

// --- 3) Micro-benchmark: parallel ingest on managed timestamps.

func BenchmarkLockFreeIngest(b *testing.B) {
	withDB(b, true, func(db *DB) {
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			id := rand.Int()
			for pb.Next() {
				// 2-byte key to avoid huge bloom filters; still causes overwrites.
				k := []byte{byte(id), byte(time.Now().Nanosecond())}
				ts := uint64(time.Now().UnixNano())

				txn := db.NewTransactionAt(ts, true)
				_ = txn.Set(k, []byte("v"))
				// We commit; Discard() is only needed on abort/failure.
				_ = txn.CommitAt(ts, nil)
			}
		})
	})
}

func TestTimestampScenarios(t *testing.T) {
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
		triggerCompact bool
	}{
		{
			name: "basic overwrite ascending",
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
			name: "parallel overlapping timestamps",
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
			name: "read snapshot in between",
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
			name: "delete semantics",
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
			name: "cold vs hot compaction",
			writes: func() []writeOp {
				var w []writeOp
				for i := 1; i <= 10; i++ {
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(i))
					// w = append(w, writeOp{uint64(i), []byte(fmt.Sprintf("%d", i)), []byte("cold")})
					w = append(w, writeOp{uint64(i), key, []byte("cold")})
				}
				for i := 100; i < 110; i++ {
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(i-99))
					// w = append(w, writeOp{uint64(i), []byte(fmt.Sprintf("%d", i-99)), []byte("hot")})
					w = append(w, writeOp{uint64(i), key, []byte("hot")})
				}
				return w
			}(),
			reads: func() []readOp {
				var r []readOp
				key := make([]byte, 4)
				binary.BigEndian.PutUint32(key, uint32(1))
				r = append(r, readOp{math.MaxUint64, key, []byte("hot"), false})
				r = append(r, readOp{5, key, []byte("cold"), false})
				return r
			}(),
			triggerFlush:   true,
			triggerCompact: true,
		},
		{
			name: "interleaved multi-key",
			writes: []writeOp{
				{50, []byte("e"), []byte("v1")},
				{51, []byte("f"), []byte("v2")},
				{52, []byte("e"), []byte("v3")},
			},
			reads: []readOp{
				{51, []byte("e"), []byte("v1"), false},
				{53, []byte("e"), []byte("v3"), false},
				{math.MaxUint64, []byte("f"), []byte("v2"), false},
			},
		},
		{
			name: "partitioned fanout (if enabled)",
			writes: []writeOp{
				{60, []byte("p1:k"), []byte("A")},
				{61, []byte("p2:k"), []byte("B")},
			},
			reads: []readOp{
				{math.MaxUint64, []byte("p1:k"), []byte("A"), false},
				{math.MaxUint64, []byte("p2:k"), []byte("B"), false},
			},
			triggerFlush: true,
		},
		{
			name: "compaction preserves latest",
			writes: []writeOp{
				{70, []byte("g"), []byte("v70")},
				{80, []byte("g"), []byte("v80")},
				{90, []byte("g"), []byte("v90")},
			},
			reads: []readOp{
				{math.MaxUint64, []byte("g"), []byte("v90"), false},
				{75, []byte("g"), []byte("v70"), false},
			},
			triggerFlush:   true,
			triggerCompact: true,
		},
		{
			name: "concurrent conflicting writes",
			writes: []writeOp{
				{100, []byte("h"), []byte("v1")},
				{101, []byte("h"), []byte("v2")},
			},
			reads: []readOp{
				{math.MaxUint64, []byte("h"), []byte("v2"), false},
			},
		},
	}

	withDB(t, true, func(db *DB) {
		for _, sc := range scenarios {
			t.Run(sc.name, func(t *testing.T) {
				// writes
				for _, w := range sc.writes {
					txn := db.NewTransactionAt(w.ts, true)
					if w.val == nil {
						_ = txn.Delete(w.key)
					} else {
						_ = txn.Set(w.key, w.val)
					}
					if err := txn.CommitAt(w.ts, nil); err != nil {
						t.Fatalf("commit: %v", err)
					}
				}

				// fmt.Println("Hello World")
				if sc.triggerFlush {
					// head := db.root.Load()
					// for n := head; n != nil; n = n.next {
					// 	// fmt.Printf("Key: %d, Value: %d",)
					// 	for i, ele := range n.kvs {
					// 		if ele != nil {
					// 			userKey := ele.Key
					// 			if len(ele.Key) > 8 {
					// 				userKey = ele.Key[:len(ele.Key)-8]
					// 			}
					// 			// Now we can safely print the parsed userKey with %s.
					// 			fmt.Printf("    - Entry %d -> Key: %-15s Value: %s\n", i, userKey, ele.Value)
					// 		}
					// 	}
					// }
					// simulate flush
					if err := db.handleMemTableFlushPartitioned(); err != nil {
						t.Fatalf("flush error: %v", err)
					}
				}
				if sc.triggerCompact {
					// simulate compaction at L0
					db.lc.checkPartitionOverflow(0)
				}

				// reads
				for _, r := range sc.reads {
					txn := db.NewTransactionAt(r.ts, false)
					itm, err := txn.Get(r.key)
					if r.wantErr {
						if err == nil {
							t.Fatalf("expected error at ts=%d for key=%q", r.ts, r.key)
						}
						continue
					}
					if err != nil {
						t.Fatalf("unexpected get for key=%q, error: %v", r.key, err)
					}
					got, _ := itm.ValueCopy(nil)
					if !bytes.Equal(got, r.wantVal) {
						t.Fatalf("at ts=%d: expected %q, got %q", r.ts, r.wantVal, got)
					}
				}
			})
		}
	})
}
