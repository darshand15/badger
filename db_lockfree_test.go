package badger

import (
	"bytes"
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
    scenarios := []struct {
        name string
        writes []struct {
            ts   uint64
            key  []byte
            val  []byte
        }
        reads []struct {
            ts      uint64
            key     []byte
            wantVal []byte
            wantErr bool
        }
    }{
        {
            name: "historical reads",
            writes: []struct{ts uint64; key, val []byte}{
                {1, []byte("k"), []byte("v1")},
                {2, []byte("k"), []byte("v2")},
                {3, []byte("k"), []byte("v3")},
            },
            reads: []struct{ts uint64; key, wantVal []byte; wantErr bool}{
                {1, []byte("k"), []byte("v1"), false},
                {2, []byte("k"), []byte("v2"), false},
                {3, []byte("k"), []byte("v3"), false},
                {math.MaxUint64, []byte("k"), []byte("v3"), false},
            },
        },
        {
            name: "delete semantics",
            writes: []struct{ts uint64; key, val []byte}{
                {1, []byte("k"), []byte("v1")},
                {2, []byte("k"), nil}, // delete marker
            },
            reads: []struct{ts uint64; key, wantVal []byte; wantErr bool}{
                {1, []byte("k"), []byte("v1"), false},
                {2, []byte("k"), nil, true}, // deleted
                {math.MaxUint64, []byte("k"), nil, true},
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
                        t.Fatalf("unexpected get error: %v", err)
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

