//go:build duckdb

package badger

import (
	"math/rand"
	"testing"
	"time"
)

// withDuckDB opens a DuckDB-backed managed DB for the duration of fn.
// Works for both *testing.T and *testing.B.
func withDuckDB(tb testing.TB, managed bool, fn func(db *DB)) {
	tb.Helper()

	opts := DefaultOptions(tb.TempDir())
	opts.UseDuckDB = true
	opts.PartitionFanOut = 8
	opts.NumCompactors = 0
	opts.CompactL0OnClose = false
	opts.Logger = nil

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
		tb.Fatalf("open DuckDB: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	fn(db)
}

// BenchmarkLockFreeIngest_DuckDB mirrors BenchmarkLockFreeIngest but routes
// writes through the DuckDB backend so the two can be compared with benchstat.
func BenchmarkLockFreeIngest_DuckDB(b *testing.B) {
	withDuckDB(b, true, func(db *DB) {
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			id := rand.Int()
			for pb.Next() {
				k := []byte{byte(id), byte(time.Now().Nanosecond())}
				ts := uint64(time.Now().UnixNano())

				txn := db.NewTransactionAt(ts, true)
				_ = txn.Set(k, []byte("v"))
				_ = txn.CommitAt(ts, nil)
			}
		})
	})
}
