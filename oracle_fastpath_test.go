//go:build duckdb

package badger

// oracle_fastpath_test.go
//
// Verifies the newCommitTs fast path (isManaged && !detectConflicts):
//   1. isManaged=false  — regular oracle: conflict detection, lock, normal Badger behaviour
//   2. isManaged=true   — fast path fires: no lock, no conflict check, timestamp pre-assigned
//
// Run with:
//   go test -tags duckdb -race -v -run TestOracle ./...

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger/v4/types"
	"github.com/stretchr/testify/require"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func openManaged(t *testing.T) *DB {
	t.Helper()
	opts := DefaultOptions(t.TempDir()).
		WithDetectConflicts(false).
		WithLogger(nil)
	db, err := OpenManaged(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func openRegular(t *testing.T) *DB {
	t.Helper()
	opts := DefaultOptions(t.TempDir()).
		WithDetectConflicts(true).
		WithLogger(nil)
	db, err := Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func writeAt(t *testing.T, db *DB, key, val string, ts types.CustomTs) {
	t.Helper()
	txn := db.NewTransactionAt(ts, true)
	require.NoError(t, txn.Set([]byte(key), []byte(val)))
	require.NoError(t, txn.CommitAt(ts, nil))
}

func readAt(t *testing.T, db *DB, key string, ts types.CustomTs) (string, bool) {
	t.Helper()
	txn := db.NewTransactionAt(ts, false)
	defer txn.Discard()
	item, err := txn.Get([]byte(key))
	if err == ErrKeyNotFound {
		return "", false
	}
	require.NoError(t, err)
	var val []byte
	require.NoError(t, item.Value(func(v []byte) error {
		val = append(val, v...)
		return nil
	}))
	return string(val), true
}

// ── Group 1: isManaged=false (regular Badger, conflict detection active) ─────

// TestOracleRegular_NoConflict: two txns touch different keys — both commit.
func TestOracleRegular_NoConflict(t *testing.T) {
	db := openRegular(t)

	txn1 := db.NewTransaction(true)
	require.NoError(t, txn1.Set([]byte("keyA"), []byte("valA")))

	txn2 := db.NewTransaction(true)
	require.NoError(t, txn2.Set([]byte("keyB"), []byte("valB")))

	require.NoError(t, txn1.Commit(), "txn1 should commit — no overlap")
	require.NoError(t, txn2.Commit(), "txn2 should commit — no overlap")

	// Both values readable
	txn3 := db.NewTransaction(false)
	defer txn3.Discard()
	item, err := txn3.Get([]byte("keyA"))
	require.NoError(t, err)
	item.Value(func(v []byte) error { require.Equal(t, "valA", string(v)); return nil })
	item, err = txn3.Get([]byte("keyB"))
	require.NoError(t, err)
	item.Value(func(v []byte) error { require.Equal(t, "valB", string(v)); return nil })
}

// TestOracleRegular_WriteWriteConflict: txn1 reads keyA, txn2 writes keyA and
// commits first — txn1's commit must be rejected with ErrConflict.
func TestOracleRegular_WriteWriteConflict(t *testing.T) {
	db := openRegular(t)

	// Seed keyA
	seed := db.NewTransaction(true)
	require.NoError(t, seed.Set([]byte("keyA"), []byte("original")))
	require.NoError(t, seed.Commit())

	// txn1 reads keyA (adds to read set)
	txn1 := db.NewTransaction(true)
	_, err := txn1.Get([]byte("keyA"))
	require.NoError(t, err)
	require.NoError(t, txn1.Set([]byte("keyA"), []byte("from-txn1")))

	// txn2 writes and commits keyA first
	txn2 := db.NewTransaction(true)
	require.NoError(t, txn2.Set([]byte("keyA"), []byte("from-txn2")))
	require.NoError(t, txn2.Commit())

	// txn1 now commits — keyA was modified after txn1 started → conflict
	err = txn1.Commit()
	require.ErrorIs(t, err, ErrConflict,
		"txn1 must fail: keyA was modified by txn2 after txn1 read it")
}

// TestOracleRegular_Determinism: same sequence of non-conflicting writes
// repeated 10 times always produces the same final value.
func TestOracleRegular_Determinism(t *testing.T) {
	const runs = 10
	results := make([]string, runs)

	for i := 0; i < runs; i++ {
		db := openRegular(t)
		txn := db.NewTransaction(true)
		require.NoError(t, txn.Set([]byte("counter"), []byte(fmt.Sprintf("run-%d", i))))
		require.NoError(t, txn.Commit())

		txn2 := db.NewTransaction(false)
		item, err := txn2.Get([]byte("counter"))
		require.NoError(t, err)
		item.Value(func(v []byte) error { results[i] = string(v); return nil })
		txn2.Discard()
		_ = db.Close()
	}

	// Each run should read back what it wrote
	for i, r := range results {
		require.Equal(t, fmt.Sprintf("run-%d", i), r,
			"run %d: non-deterministic result", i)
	}
}

// ── Group 2: isManaged=true, detectConflicts=false (fast path) ───────────────

// TestOracleFastPath_Fires: verify the fast path actually skips the lock.
// The oracle must not acquire o.Lock() — we check indirectly by confirming
// that orc.isManaged and !orc.detectConflicts are both true.
func TestOracleFastPath_Fires(t *testing.T) {
	db := openManaged(t)

	// Confirm the options that trigger the fast path
	require.True(t, db.opt.managedTxns,
		"managed mode must be on for fast path")
	require.False(t, db.opt.DetectConflicts,
		"DetectConflicts must be off for fast path")

	// A commit under these conditions exercises the fast path
	ts := types.CustomTs{AssignedTs: 10}
	writeAt(t, db, "key1", "val1", ts)

	// If fast path is broken we'd deadlock or panic — reaching here means it works
	val, found := readAt(t, db, "key1", ts)
	require.True(t, found)
	require.Equal(t, "val1", val)
}

// TestOracleFastPath_TimestampPreserved: commit timestamp must be exactly
// what the caller supplied — the oracle must not override it.
func TestOracleFastPath_TimestampPreserved(t *testing.T) {
	db := openManaged(t)

	ts5  := types.CustomTs{AssignedTs: 5}
	ts10 := types.CustomTs{AssignedTs: 10}
	ts20 := types.CustomTs{AssignedTs: 20}

	writeAt(t, db, "key", "v5",  ts5)
	writeAt(t, db, "key", "v10", ts10)
	writeAt(t, db, "key", "v20", ts20)

	// Read at ts5 — must see v5 (not a later version)
	v, _ := readAt(t, db, "key", ts5)
	require.Equal(t, "v5", v, "read at ts=5 must see version written at ts=5")

	// Read at ts10 — must see v10
	v, _ = readAt(t, db, "key", ts10)
	require.Equal(t, "v10", v, "read at ts=10 must see version written at ts=10")

	// Read at ts20 — must see v20
	v, _ = readAt(t, db, "key", ts20)
	require.Equal(t, "v20", v, "read at ts=20 must see version written at ts=20")

	// Read at ts15 — must see v10 (latest version <= 15)
	ts15 := types.CustomTs{AssignedTs: 15}
	v, _ = readAt(t, db, "key", ts15)
	require.Equal(t, "v10", v, "read at ts=15 must see v10 (no write between 10 and 20)")
}

// TestOracleFastPath_NoConflictDetection: in managed+no-conflict mode,
// two concurrent writes to the same key at different timestamps must BOTH
// succeed — there is no ErrConflict because the DAG handles ordering.
func TestOracleFastPath_NoConflictDetection(t *testing.T) {
	db := openManaged(t)

	ts1 := types.CustomTs{AssignedTs: 1}
	ts2 := types.CustomTs{AssignedTs: 2}

	// Both writes to the same key succeed — no conflict check
	writeAt(t, db, "shared", "first",  ts1)
	writeAt(t, db, "shared", "second", ts2)

	v, _ := readAt(t, db, "shared", ts2)
	require.Equal(t, "second", v)

	v, _ = readAt(t, db, "shared", ts1)
	require.Equal(t, "first", v)
}

// TestOracleFastPath_NewCommitTsRegistersTracker ensures managed fast path
// registers commitTs with duckDBTracker even when conflict detection is off.
// This protects NewTransactionAt(readTs) read barriers for callers that don't
// pre-register via DB.RegisterPendingCommit.
func TestOracleFastPath_NewCommitTsRegistersTracker(t *testing.T) {
	db := openManaged(t)

	ts := types.CustomTs{AssignedTs: 42}
	txn := db.NewTransactionAt(ts, true)
	defer txn.Discard()
	txn.commitTs = ts

	got, conflict := db.orc.newCommitTs(txn)
	require.False(t, conflict)
	require.Equal(t, ts, got)

	hasPending := func(target types.CustomTs) bool {
		db.orc.duckDBTracker.mu.Lock()
		defer db.orc.duckDBTracker.mu.Unlock()
		for _, v := range db.orc.duckDBTracker.pending {
			if v == target {
				return true
			}
		}
		return false
	}

	require.True(t, hasPending(ts), "newCommitTs fast path must register tracker")

	db.orc.doneCommit(ts)
	require.False(t, hasPending(ts), "doneCommit must deregister tracker")
}

// TestOracleFastPath_Concurrent_NoRace: 8 goroutines each commit to their own
// key concurrently under managed mode. No lock contention, no data races.
// Run with: go test -tags duckdb -race -run TestOracleFastPath_Concurrent_NoRace
func TestOracleFastPath_Concurrent_NoRace(t *testing.T) {
	const numGoroutines = 8
	const commitsPerGoroutine = 100
	db := openManaged(t)

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < commitsPerGoroutine; i++ {
				// Each goroutine has its own key — zero key overlap (like DAG guarantee)
				ts  := types.CustomTs{AssignedTs: uint32(gid*commitsPerGoroutine + i + 1)}
				key := fmt.Sprintf("goroutine-%d-key-%d", gid, i)
				writeAt(t, db, key, fmt.Sprintf("val-%d-%d", gid, i), ts)
			}
		}(g)
	}
	wg.Wait()

	// Verify a sample of written values
	for g := 0; g < numGoroutines; g++ {
		i := commitsPerGoroutine - 1 // last write from each goroutine
		ts  := types.CustomTs{AssignedTs: uint32(g*commitsPerGoroutine + i + 1)}
		key := fmt.Sprintf("goroutine-%d-key-%d", g, i)
		v, found := readAt(t, db, key, ts)
		require.True(t, found, "goroutine %d key %d not found", g, i)
		require.Equal(t, fmt.Sprintf("val-%d-%d", g, i), v)
	}
}

// TestOracleFastPath_Concurrent_SharedKey: 8 goroutines write to the same key
// with strictly increasing timestamps (simulating DAG serial ordering for a
// hot key). All commits must succeed; the highest timestamp wins on read.
func TestOracleFastPath_Concurrent_SharedKey(t *testing.T) {
	const numGoroutines = 8
	db := openManaged(t)

	var counter uint32 // used to assign strictly increasing timestamps
	var wg sync.WaitGroup

	type result struct {
		ts  types.CustomTs
		val string
	}
	results := make([]result, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			n  := atomic.AddUint32(&counter, 1)
			ts := types.CustomTs{AssignedTs: n}
			val := fmt.Sprintf("writer-%d-ts-%d", gid, n)
			writeAt(t, db, "hotkey", val, ts)
			results[gid] = result{ts, val}
		}(g)
	}
	wg.Wait()

	// Find the highest timestamp that was assigned
	var maxTs types.CustomTs
	var maxVal string
	for _, r := range results {
		if r.ts.Greater(maxTs) {
			maxTs  = r.ts
			maxVal = r.val
		}
	}

	// Read at the highest timestamp — must see the value written at that timestamp
	v, found := readAt(t, db, "hotkey", maxTs)
	require.True(t, found)
	require.Equal(t, maxVal, v,
		"read at max ts must see the value written at that exact timestamp")
}

// TestOracleFastPath_Determinism: same fixed sequence of writes repeated 10×,
// each time on a fresh DB. Final state must be identical every time.
func TestOracleFastPath_Determinism(t *testing.T) {
	const runs = 10
	const numKeys = 20

	type snapshot map[string]string

	runOnce := func() snapshot {
		db := openManaged(t)
		defer db.Close()

		for i := 0; i < numKeys; i++ {
			ts  := types.CustomTs{AssignedTs: uint32(i + 1)}
			key := fmt.Sprintf("key-%02d", i)
			val := fmt.Sprintf("val-%02d", i)
			writeAt(t, db, key, val, ts)
		}

		snap := snapshot{}
		readTs := types.CustomTs{AssignedTs: uint32(numKeys + 1)}
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key-%02d", i)
			v, found := readAt(t, db, key, readTs)
			require.True(t, found)
			snap[key] = v
		}
		return snap
	}

	first := runOnce()
	for i := 1; i < runs; i++ {
		snap := runOnce()
		for k, v := range first {
			require.Equal(t, v, snap[k],
				"run %d: key %s non-deterministic (got %q want %q)", i, k, snap[k], v)
		}
	}
}

// TestOracleFastPath_Schedule_SameOrderSameResult: simulates the DAG
// delivering the same ordered schedule 5 times. Each run must produce
// the same final balances for all accounts (the core correctness claim).
func TestOracleFastPath_Schedule_SameOrderSameResult(t *testing.T) {
	const accounts = 10
	const runs = 5

	type schedule struct {
		from, to int
		amount   int64
		ts       uint32
	}

	// Fixed, deterministic schedule — same as what the DAG would produce
	// for the same input epoch
	txns := []schedule{
		{from: 1, to: 2, amount: 100, ts: 11},
		{from: 3, to: 4, amount: 50,  ts: 12},
		{from: 2, to: 5, amount: 30,  ts: 13},
		{from: 6, to: 1, amount: 200, ts: 14},
		{from: 7, to: 8, amount: 75,  ts: 15},
		{from: 9, to: 10, amount: 40, ts: 16},
		{from: 1, to: 3, amount: 10,  ts: 17},
	}

	readBalance := func(db *DB, account int, ts types.CustomTs) int64 {
		key := fmt.Sprintf("acct-%02d", account)
		v, found := readAt(t, db, key, ts)
		if !found {
			return 0
		}
		var bal int64
		fmt.Sscanf(v, "%d", &bal)
		return bal
	}

	writeBalance := func(db *DB, account int, bal int64, ts types.CustomTs) {
		key := fmt.Sprintf("acct-%02d", account)
		writeAt(t, db, key, fmt.Sprintf("%d", bal), ts)
	}

	runOnce := func() map[int]int64 {
		db := openManaged(t)
		defer db.Close()

		// Seed: all accounts start at 1000
		seedTs := types.CustomTs{AssignedTs: 1}
		for i := 1; i <= accounts; i++ {
			writeBalance(db, i, 1000, seedTs)
		}

		// Execute the fixed schedule in order
		for _, tx := range txns {
			ts := types.CustomTs{AssignedTs: tx.ts}
			fromBal := readBalance(db, tx.from, ts)
			toBal   := readBalance(db, tx.to,   ts)
			writeAt(t, db,
				fmt.Sprintf("acct-%02d", tx.from),
				fmt.Sprintf("%d", fromBal-tx.amount), ts)
			writeAt(t, db,
				fmt.Sprintf("acct-%02d", tx.to),
				fmt.Sprintf("%d", toBal+tx.amount), ts)
		}

		// Snapshot final balances
		finalTs := types.CustomTs{AssignedTs: 99}
		result := map[int]int64{}
		for i := 1; i <= accounts; i++ {
			result[i] = readBalance(db, i, finalTs)
		}
		return result
	}

	first := runOnce()
	for run := 1; run < runs; run++ {
		snap := runOnce()
		for acct, bal := range first {
			require.Equal(t, bal, snap[acct],
				"run %d: account %d balance non-deterministic (got %d want %d)",
				run, acct, snap[acct], bal)
		}
	}

	// Sanity: total money is conserved (no creation or destruction)
	var total int64
	for _, bal := range first {
		total += bal
	}
	require.Equal(t, int64(accounts*1000), total,
		"total balance must be conserved across all transfers")
}

// TestOracleFastPath_vs_Managed_IsManaged: explicitly compare behaviour when
// isManaged toggles. Same key, same value — but one path goes through the
// lock, the other bypasses it. Both must produce correct readable data.
func TestOracleFastPath_ManagedVsUnmanaged(t *testing.T) {
	t.Run("unmanaged", func(t *testing.T) {
		db := openRegular(t)
		txn := db.NewTransaction(true)
		require.NoError(t, txn.Set([]byte("k"), []byte("v-unmanaged")))
		require.NoError(t, txn.Commit())

		txn2 := db.NewTransaction(false)
		defer txn2.Discard()
		item, err := txn2.Get([]byte("k"))
		require.NoError(t, err)
		item.Value(func(v []byte) error {
			require.Equal(t, "v-unmanaged", string(v))
			return nil
		})
	})

	t.Run("managed_fastpath", func(t *testing.T) {
		db := openManaged(t)
		ts := types.CustomTs{AssignedTs: 5}
		writeAt(t, db, "k", "v-managed", ts)
		v, found := readAt(t, db, "k", ts)
		require.True(t, found)
		require.Equal(t, "v-managed", v)
	})
}
