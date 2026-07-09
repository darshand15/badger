//go:build duckdb

package badger

import (
	"strconv"
	"testing"

	"github.com/dgraph-io/badger/v4/types"
)

func benchTs(i int) types.CustomTs {
	u := uint64(i) + 1
	return types.CustomTs{
		EpochID:    uint32(u >> 32),
		AssignedTs: uint32(u),
	}
}

func benchKey(prefix string, i int) []byte {
	buf := make([]byte, len(prefix), len(prefix)+20)
	copy(buf, prefix)
	return strconv.AppendInt(buf, int64(i), 10)
}

// BenchmarkDuckDBManagedCommitPipeline isolates managed-mode commit overhead
// (timestamp registration + commit path + direct append) using prebuilt keys.
func BenchmarkDuckDBManagedCommitPipeline(b *testing.B) {
	withDuckDB(b, true, func(db *DB) {
		const keyCount = 4096
		keys := make([][]byte, keyCount)
		for i := 0; i < keyCount; i++ {
			keys[i] = benchKey("pipe-k-", i)
		}
		value := []byte("v")

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ts := benchTs(i)
			db.RegisterPendingCommit(ts)
			txn := db.NewTransactionAt(ts, true)
			if err := txn.Set(keys[i%keyCount], value); err != nil {
				b.Fatalf("set: %v", err)
			}
			if err := txn.CommitAt(ts, nil); err != nil {
				b.Fatalf("commit: %v", err)
			}
		}
	})
}

// BenchmarkDuckDBDirectAppendPipeline isolates DirectFlush CGo append path by
// bypassing transaction building and feeding prepared entry batches.
func BenchmarkDuckDBDirectAppendPipeline(b *testing.B) {
	withDuckDB(b, true, func(db *DB) {
		const batchSize = 256
		keys := make([][]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			keys[i] = benchKey("direct-k-", i)
		}
		value := []byte("value")

		entries := make([]duckEntry, batchSize)
		for i := 0; i < batchSize; i++ {
			entries[i] = duckEntry{Key: keys[i], Value: value}
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ts := benchTs(i)
			for j := range entries {
				entries[j].Version = ts
				entries[j].Deleted = false
			}
			if err := db.duckDBStorage.DirectFlush(entries); err != nil {
				b.Fatalf("direct flush: %v", err)
			}
		}
	})
}
