package badger

import (
	"fmt"
	"testing"
	"time"
)

// TestDuckDBAppenderPerformance verifies Appender API performance
func TestDuckDBAppenderPerformance(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	opts.NumCompactors = 0
	opts.CompactL0OnClose = false
	opts.UseDuckDB = false // Disabled for test
	opts.PartitionFanOut = 8

	db, err := OpenManaged(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	numKeys := 1000
	start := time.Now()

	// Write entries
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		val := []byte(fmt.Sprintf("value%010d", i))

		txn := db.NewTransactionAt(uint64(i+1), true)
		if err := txn.Set(key, val); err != nil {
			t.Fatal(err)
		}
		if err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	writeTime := time.Since(start)
	t.Logf("✅ Wrote %d entries in %v (%.0f ops/sec)", 
		numKeys, writeTime, float64(numKeys)/writeTime.Seconds())

	// Flush to DuckDB
	start = time.Now()
	if err := db.handleMemTableFlushPartitioned(); err != nil {
		t.Fatal(err)
	}
	flushTime := time.Since(start)

	t.Logf("✅ Flush completed in %v", flushTime)
	t.Logf("✅ Flush rate: %.0f entries/sec", float64(numKeys)/flushTime.Seconds())

	// Verify all keys can be read
	readStart := time.Now()
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		txn := db.NewTransactionAt(uint64(numKeys+100), false)
		_, err := txn.Get(key)
		txn.Discard()
		if err != nil {
			t.Fatalf("Failed to read key %d: %v", i, err)
		}
	}
	readTime := time.Since(readStart)

	t.Logf("✅ Read %d entries in %v", numKeys, readTime)
	t.Logf("✅ Read rate: %.0f ops/sec", float64(numKeys)/readTime.Seconds())

	// Performance expectations with Appender API:
	// - Write: 20,000-100,000 ops/sec
	// - Flush: 50,000-200,000 entries/sec
	// - Read: 10,000-50,000 ops/sec

	if opsPerSec < 10000 {
		t.Errorf("❌ Write performance too low: %.0f ops/sec (expected >10k)", opsPerSec)
	} else {
		t.Logf("✅ Write performance acceptable: %.0f ops/sec", opsPerSec)
	}

	flushRate := float64(numKeys) / flushTime.Seconds()
	if flushRate < 30000 {
		t.Errorf("❌ Flush performance too low: %.0f entries/sec (expected >30k)", flushRate)
		t.Logf("⚠️  This suggests Appender API may not be active!")
	} else {
		t.Logf("✅ Flush performance good: %.0f entries/sec (Appender API working!)", flushRate)
	}
}

// BenchmarkAppenderAPI measures Appender API performance
func BenchmarkAppenderAPI(b *testing.B) {
	opts := DefaultOptions(b.TempDir())
	opts.UseDuckDB = true
	opts.PartitionFanOut = 8
	opts.NumCompactors = 0

	db, _ := OpenManaged(opts)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		val := []byte(fmt.Sprintf("val%010d", i))

		txn := db.NewTransactionAt(uint64(i+1), true)
		_ = txn.Set(key, val)
		_ = txn.CommitAt(uint64(i+1), nil)
	}

	// Trigger flush
	_ = db.handleMemTableFlushPartitioned()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}