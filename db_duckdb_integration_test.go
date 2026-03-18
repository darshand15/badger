//go:build duckdb

package badger

import (
	"fmt"
	"math"
	"testing"
)

func TestDuckDBIntegration(t *testing.T) {
    opts := DefaultOptions(t.TempDir())
    opts.UseDuckDB = true          // ← USE DUCKDB
    opts.PartitionFanOut = 8       // ← 8 partitions
    opts.NumCompactors = 0

    db, err := OpenManaged(opts)
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()

    // Write some data
    for i := 0; i < 100; i++ {
        key := []byte(fmt.Sprintf("key%03d", i))
        val := []byte(fmt.Sprintf("val%03d", i))
        
        txn := db.NewTransactionAt(uint64(i+1), true)
        if err := txn.Set(key, val); err != nil {
            t.Fatal(err)
        }
        if err := txn.CommitAt(uint64(i+1), nil); err != nil {
            t.Fatal(err)
        }
    }

    // Trigger flush to DuckDB
    if err := db.handleMemTableFlushPartitioned(db.mt, nil); err != nil {
        t.Fatal(err)
    }

    t.Log("✅ DuckDB flush succeeded!")

    // Read back data
    for i := 0; i < 100; i++ {
    key := []byte(fmt.Sprintf("key%03d", i))
    expectedVal := []byte(fmt.Sprintf("val%03d", i))
    
    // ✅ FIX: Use NewTransactionAt instead of NewTransaction
    txn := db.NewTransactionAt(math.MaxUint64, false)  // ← CHANGED THIS LINE
    item, err := txn.Get(key)
    if err != nil {
        t.Fatalf("Failed to read key%03d: %v", i, err)
    }
    
    gotVal, _ := item.ValueCopy(nil)
    if string(gotVal) != string(expectedVal) {
        t.Fatalf("Key%03d: expected %s, got %s", i, expectedVal, gotVal)
    }
    	txn.Discard()
	}

    t.Log("✅ All 100 keys read successfully from DuckDB!")
}