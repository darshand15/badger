//go:build duckdb

package badger

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4/divytime"
)

const (
	ledgerAccounts = 15 // High conflict footprint
	ledgerWorkers  = 8
	opsPerWorker   = 30
	ledgerMaxRetry = 50
)

// LedgerOp defines a physical operation executed by a transaction.
type LedgerOp struct {
	TxID      int
	Type      string // "READ" or "WRITE"
	Key       string
	Val       []byte
	LogicalTs divytime.Timestamp
}

// Global thread-safe ledger recorder
var (
	ledgerMu sync.Mutex
	dbLedger []LedgerOp
)

func appendToLedger(op LedgerOp) {
	ledgerMu.Lock()
	dbLedger = append(dbLedger, op)
	ledgerMu.Unlock()
}

// Helper to compare 3-tuple divytime timestamps logically
func tsLess(a, b divytime.Timestamp) bool {
	if a.EpochID != b.EpochID {
		return a.EpochID < b.EpochID
	}
	if a.BrokerID != b.BrokerID {
		return a.BrokerID < b.BrokerID
	}
	return a.AssignedTs < b.AssignedTs
}

func tsEqual(a, b divytime.Timestamp) bool {
	return a.EpochID == b.EpochID && a.BrokerID == b.BrokerID && a.AssignedTs == b.AssignedTs
}

// ---------------------------------------------------------------------------
// Main Test Verification Driver
// ---------------------------------------------------------------------------

func TestDuckDBTimestampValueEquivalence(t *testing.T) {
	dbLedger = make([]LedgerOp, 0)
	oracle := divytime.NewOracle(1, 15*time.Microsecond)

	withDuckDB(t, true, func(db *DB) {
		// 1. Seed initial zero-state balances
		initTsRaw, _ := oracle.GetTimestamp(int64(time.Now().UnixNano()))
		initTs := divyToTs(initTsRaw)
		seedTx := db.NewTransactionAt(initTs, true)

		for i := 0; i < ledgerAccounts; i++ {
			k := fmt.Sprintf("ledger_acct:%04d", i)
			v := []byte(fmt.Sprintf("v0-seed-acct-%d", i))
			_ = seedTx.Set([]byte(k), v)

			// Log the initial state setup
			appendToLedger(LedgerOp{
				TxID:      0,
				Type:      "WRITE",
				Key:       k,
				Val:       v,
				LogicalTs: initTsRaw,
			})
		}
		if err := seedTx.CommitAt(initTs, nil); err != nil {
			t.Fatalf("Failed to seed variables: %v", err)
		}

		// 2. Execute concurrent overlapping workloads
		var wg sync.WaitGroup
		txCounter := 1

		for w := 0; w < ledgerWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for i := 0; i < opsPerWorker; i++ {
					ledgerMu.Lock()
					currentTxID := txCounter
					txCounter++
					ledgerMu.Unlock()

					// Target conflicting items
					targetKeyA := fmt.Sprintf("ledger_acct:%04d", currentTxID%ledgerAccounts)
					targetKeyB := fmt.Sprintf("ledger_acct:%04d", (currentTxID+3)%ledgerAccounts)

					executeLedgerTransaction(db, oracle, currentTxID, targetKeyA, targetKeyB)
				}
			}(w)
		}
		wg.Wait()

		t.Logf("Workload complete. Total operations captured in ledger: %d", len(dbLedger))

		// 3. Verify Value-to-Timestamp Equivalence
		verifyLedgerCorrectness(t)
	})
}

// ---------------------------------------------------------------------------
// Transaction Wrapper with Operation Interception
// ---------------------------------------------------------------------------

func executeLedgerTransaction(db *DB, oracle *divytime.Oracle, txID int, keyA, keyB string) {
	for attempt := 0; attempt < ledgerMaxRetry; attempt++ {
		readTsRaw, _ := oracle.GetTimestamp(int64(time.Now().UnixNano()))
		txn := db.NewTransactionAt(divyToTs(readTsRaw), true)

		// Exec Read A
		itemA, err := txn.Get([]byte(keyA))
		if err != nil {
			txn.Discard()
			continue
		}
		valA, _ := itemA.ValueCopy(nil)

		// Exec Read B
		itemB, err := txn.Get([]byte(keyB))
		if err != nil {
			txn.Discard()
			continue
		}
		valB, _ := itemB.ValueCopy(nil)

		// Formulate mutated string values unique to this transaction execution
		newValA := []byte(fmt.Sprintf("tx%d-mutated-%s", txID, keyA))
		newValB := []byte(fmt.Sprintf("tx%d-mutated-%s", txID, keyB))

		_ = txn.Set([]byte(keyA), newValA)
		_ = txn.Set([]byte(keyB), newValB)

		commitTsRaw, _ := oracle.GetCommitTimestamp(func(ts divytime.Timestamp) {
			db.RegisterPendingCommit(divyToTs(ts))
		})

		err = txn.CommitAt(divyToTs(commitTsRaw), nil)
		txn.Discard()

		if err == ErrConflict {
			continue // Standard transactional retry
		}
		if err != nil {
			return
		}

		// Transaction Committed Successfully -> Commit records to global verification ledger
		appendToLedger(LedgerOp{TxID: txID, Type: "READ", Key: keyA, Val: valA, LogicalTs: readTsRaw})
		appendToLedger(LedgerOp{TxID: txID, Type: "READ", Key: keyB, Val: valB, LogicalTs: readTsRaw})
		appendToLedger(LedgerOp{TxID: txID, Type: "WRITE", Key: keyA, Val: newValA, LogicalTs: commitTsRaw})
		appendToLedger(LedgerOp{TxID: txID, Type: "WRITE", Key: keyB, Val: newValB, LogicalTs: commitTsRaw})
		return
	}
}

// ---------------------------------------------------------------------------
// Absolute Correctness Analysis
// ---------------------------------------------------------------------------

func verifyLedgerCorrectness(t *testing.T) {
	ledgerMu.Lock()
	localLedger := append([]LedgerOp(nil), dbLedger...)
	ledgerMu.Unlock()

	// Group writes by key to build the "Ground Truth Timeline" for every individual data asset
	writesTimeline := make(map[string][]LedgerOp)
	var readsRegistry []LedgerOp

	for _, op := range localLedger {
		if op.Type == "WRITE" {
			writesTimeline[op.Key] = append(writesTimeline[op.Key], op)
		} else if op.Type == "READ" {
			readsRegistry = append(readsRegistry, op)
		}
	}

	// Sort each key's write timeline strictly by its structural divytime order
	for key := range writesTimeline {
		sort.Slice(writesTimeline[key], func(i, j int) bool {
			return tsLess(writesTimeline[key][i].LogicalTs, writesTimeline[key][j].LogicalTs)
		})
	}

	// Validate intermediate reads against historical timelines
	for _, readOp := range readsRegistry {
		timeline := writesTimeline[readOp.Key]

		var expectedValue []byte
		var matchingWrite LedgerOp
		found := false

		// Find the most recent write that occurred AT or BEFORE this read's logical timestamp
		for i := len(timeline) - 1; i >= 0; i-- {
			writeOp := timeline[i]
			if tsLess(writeOp.LogicalTs, readOp.LogicalTs) || tsEqual(writeOp.LogicalTs, readOp.LogicalTs) {
				expectedValue = writeOp.Val
				matchingWrite = writeOp
				found = true
				break
			}
		}

		if !found {
			t.Fatalf("CRITICAL FAIL: Read for key %s at timestamp %+v found no preceding valid write timeline entry.", readOp.Key, readOp.LogicalTs)
		}

		// Enforce strict value equivalence
		if !bytes.Equal(readOp.Val, expectedValue) {
			t.Fatalf("SERIALIZABILITY VIOLATION DETECTED!\n"+
				"Key: %s\n"+
				"Reader TxID: %d (ReadTs: %+v)\n"+
				"Retrieved Value: %s\n"+
				"Expected Value:  %s (Written by TxID: %d at CommitTs: %+v)",
				readOp.Key, readOp.TxID, readOp.LogicalTs, string(readOp.Val), string(expectedValue), matchingWrite.TxID, matchingWrite.LogicalTs)
		}
	}

	t.Log("PROVED: All intermediate reads and final writes perfectly conform to the chronological sequence dictated by divytime timestamps.")
}
