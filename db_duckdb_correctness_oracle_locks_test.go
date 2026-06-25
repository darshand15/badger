//go:build duckdb

package badger

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4/divytime"
	"github.com/dgraph-io/badger/v4/types"
)

const (
	serialAccounts = 20 // Kept small to force maximum data contention/collisions
	serialWorkers  = 16 // Matching your benchmark environment
	// opsPerWorker   = 50
	serialMaxRetry = 100
)

// BoundOp represents a statically assigned transaction mapping to explicit keys.
type BoundOp struct {
	TxID int
	KeyA []byte
	KeyB []byte
}

// TxTrace captures the exact read-set history a transaction observed before writing.
type TxTrace struct {
	TxID  int
	Reads map[string][]int // Key -> Version History Array observed
}

// Global trace registry for verification
var (
	traceMu    sync.Mutex
	txRegistry map[int]TxTrace
)

func encodeHistory(h []int) []byte {
	b, _ := json.Marshal(h)
	return b
}

func decodeHistory(b []byte) []int {
	if len(b) == 0 {
		return []int{}
	}
	var h []int
	_ = json.Unmarshal(b, &h)
	return h
}

func serialKey(i int) []byte {
	return []byte(fmt.Sprintf("serial_acct:%04d", i))
}

// ---------------------------------------------------------------------------
// Test Execution Driver
// ---------------------------------------------------------------------------

func TestDuckDBSerializabilityVerification(t *testing.T) {
	// Initialize tracing architecture
	txRegistry = make(map[int]TxTrace)
	oracle := divytime.NewOracle(1, 20*time.Microsecond) // Low delay to increase racing

	withDuckDB(t, true, func(db *DB) {
		// Phase 1: Seed accounts with an initial state identifier [0]
		ts, _ := oracle.GetTimestamp(int64(time.Now().UnixNano()))
		seedTx := db.NewTransactionAt(divyToTs(ts), true)
		for i := 0; i < serialAccounts; i++ {
			_ = seedTx.Set(serialKey(i), encodeHistory([]int{0}))
		}
		if err := seedTx.CommitAt(divyToTs(ts), nil); err != nil {
			t.Fatalf("Failed to seed initial histories: %v", err)
		}

		// Phase 2: Generate deterministic conflicting transaction workloads
		workloads := make([][]BoundOp, serialWorkers)
		txIDCounter := 1
		for w := 0; w < serialWorkers; w++ {
			for i := 0; i < opsPerWorker; i++ {
				// Deterministic pairing ensures distinct keys collide across workers
				kA := txIDCounter % serialAccounts
				kB := (txIDCounter + 7) % serialAccounts
				if kA == kB {
					kB = (kB + 1) % serialAccounts
				}
				workloads[w] = append(workloads[w], BoundOp{
					TxID: txIDCounter,
					KeyA: serialKey(kA),
					KeyB: serialKey(kB),
				})
				txIDCounter++
			}
		}

		// Phase 3: Run concurrent execution matching original benchmark structure
		var wg sync.WaitGroup
		for w := 0; w < serialWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for _, op := range workloads[workerID] {
					execHistoryTrackingTx(t, db, oracle, op)
				}
			}(w)
		}
		wg.Wait()

		// Phase 4: Gather final histories
		finalStates := make(map[string][]int)
		verifyTx := db.NewTransactionAt(types.MaxTs, false)
		for i := 0; i < serialAccounts; i++ {
			k := serialKey(i)
			item, err := verifyTx.Get(k)
			if err == nil {
				val, _ := item.ValueCopy(nil)
				finalStates[string(k)] = decodeHistory(val)
			}
		}
		verifyTx.Discard()

		// Phase 5: Build Serialization Graph and Check for Cycles
		traceMu.Lock()
		capturedTraces := txRegistry
		traceMu.Unlock()

		validateGraphAcyclic(t, finalStates, capturedTraces)
	})
}

// ---------------------------------------------------------------------------
// Core Transaction Implementation (Reflecting User's Implementation)
// ---------------------------------------------------------------------------

func execHistoryTrackingTx(t *testing.T, db *DB, oracle *divytime.Oracle, op BoundOp) {
	for attempt := 0; attempt < serialMaxRetry; attempt++ {
		// Call 1: Snapshot read timestamp
		readTsRaw, _ := oracle.GetTimestamp(int64(time.Now().UnixNano()))
		readTs := divyToTs(readTsRaw)

		txn := db.NewTransactionAt(readTs, true)

		// Process Read Set
		itemA, err := txn.Get(op.KeyA)
		if err != nil {
			txn.Discard()
			continue
		}
		valA, _ := itemA.ValueCopy(nil)
		histA := decodeHistory(valA)

		itemB, err := txn.Get(op.KeyB)
		if err != nil {
			txn.Discard()
			continue
		}
		valB, _ := itemB.ValueCopy(nil)
		histB := decodeHistory(valB)

		// Record what this specific transaction observed before it appends updates
		trace := TxTrace{
			TxID: op.TxID,
			Reads: map[string][]int{
				string(op.KeyA): append([]int(nil), histA...),
				string(op.KeyB): append([]int(nil), histB...),
			},
		}

		// Process Write Set (Append current transaction ID to lineage)
		newA := append(histA, op.TxID)
		newB := append(histB, op.TxID)

		_ = txn.Set(op.KeyA, encodeHistory(newA))
		_ = txn.Set(op.KeyB, encodeHistory(newB))

		// Call 2: Commit timestamp with pending commit tracking closure
		commitTsRaw, _ := oracle.GetCommitTimestamp(func(ts divytime.Timestamp) {
			db.RegisterPendingCommit(divyToTs(ts))
		})
		commitTs := divyToTs(commitTsRaw)

		commitErr := txn.CommitAt(commitTs, nil)
		txn.Discard()

		if commitErr == ErrConflict {
			continue // Retry on OCC conflict
		}
		if commitErr != nil {
			return
		}

		// Save the trace on successful commit
		traceMu.Lock()
		txRegistry[op.TxID] = trace
		traceMu.Unlock()
		return
	}
}

// ---------------------------------------------------------------------------
// Graph Analysis Logic
// ---------------------------------------------------------------------------

func validateGraphAcyclic(t *testing.T, finalStates map[string][]int, reads map[int]TxTrace) {
	adj := make(map[int]map[int]bool)
	addEdge := func(from, to int) {
		if from == 0 || to == 0 || from == to {
			return
		}
		if adj[from] == nil {
			adj[from] = make(map[int]bool)
		}
		adj[from][to] = true
	}

	// 1. Write-After-Write (WAW) dependencies
	for _, hist := range finalStates {
		for i := 0; i < len(hist)-1; i++ {
			addEdge(hist[i], hist[i+1])
		}
	}

	// 2. Read-After-Write (RAW) and Write-After-Read (WAR) dependencies
	for txID, trace := range reads {
		for key, readHist := range trace.Reads {
			if len(readHist) > 0 {
				lastRead := readHist[len(readHist)-1]
				addEdge(lastRead, txID) // RAW

				keyFinalHist := finalStates[key]
				for i, v := range keyFinalHist {
					if v == lastRead {
						if i+1 < len(keyFinalHist) {
							overwriter := keyFinalHist[i+1]
							addEdge(txID, overwriter) // WAR (anti-dependency)
						}
						break
					}
				}
			}
		}
	}

	// 3. Cycle Detection via DFS
	visited := make(map[int]int) // 0: unvisited, 1: visiting, 2: stable

	var dfs func(node int) []int
	dfs = func(node int) []int {
		if visited[node] == 1 {
			return []int{node}
		}
		if visited[node] == 2 {
			return nil
		}
		visited[node] = 1
		for neighbor := range adj[node] {
			if path := dfs(neighbor); path != nil {
				return append(path, node)
			}
		}
		visited[node] = 2
		return nil
	}

	for txID := range reads {
		if visited[txID] == 0 {
			if cyclePath := dfs(txID); cyclePath != nil {
				t.Fatalf("SERIALIZABILITY VIOLATION: Dependency cycle found: %v. The timestamp sequencing is leaky without locks.", cyclePath)
			}
		}
	}

	t.Log("PROVED: Execution graph is perfectly acyclic. Schedule matches a valid serial execution sequence.")
}
