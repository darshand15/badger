//go:build duckdb

package badger

// Side-by-side comparison of the regular Badger backend vs the DuckDB backend
// for both the bank workload and the SmallBank mixed workload.
//
// Run with:
//
//	go test -v -tags duckdb -run TestBankBadgerVsDuckDB   -timeout 120s
//	go test -v -tags duckdb -run TestSmallBankBadgerVsDuckDB -timeout 300s

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4/divytime"
)

// ---------------------------------------------------------------------------
// helpers shared by both comparison tests
// ---------------------------------------------------------------------------

// seedBadgerAccounts seeds numBankAccounts in a regular (non-DuckDB) DB.
func seedBadgerAccounts(tb testing.TB, db *DB, oracle *divytime.Oracle) {
	tb.Helper()
	for i := 0; i < numBankAccounts; i++ {
		ts, _ := oracle.GetTimestamp(int64(i) + 1)
		txn := db.NewTransactionAt(divyToTs(ts), true)
		if err := txn.Set(bankKey(i), bankEncodeUint64(initialBankBal)); err != nil {
			tb.Fatalf("seed badger account %d: %v", i, err)
		}
		if err := txn.CommitAt(divyToTs(ts), nil); err != nil {
			tb.Fatalf("seed badger commit %d: %v", i, err)
		}
	}
}

// execBadgerTransfer performs one transfer on a regular Badger DB.
// It is intentionally identical to execTransfer but uses its own oracle call.
func execBadgerTransfer(
	tb testing.TB,
	db *DB,
	oracle *divytime.Oracle,
	rng *rand.Rand,
) time.Duration {
	start := time.Now()
	from := rng.Intn(numBankAccounts)
	to := rng.Intn(numBankAccounts)
	for to == from {
		to = rng.Intn(numBankAccounts)
	}

	ts, _ := oracle.GetTimestamp(int64(time.Now().UnixNano()))
	readTs := divyToTs(ts)
	txn := db.NewTransactionAt(readTs, true)
	defer txn.Discard()

	fromItem, err := txn.Get(bankKey(from))
	if err != nil {
		return time.Since(start)
	}
	fromBal, _ := fromItem.ValueCopy(nil)
	if bankDecodeUint64(fromBal) < transferAmount {
		return time.Since(start)
	}

	toItem, err := txn.Get(bankKey(to))
	if err != nil {
		return time.Since(start)
	}
	toBal, _ := toItem.ValueCopy(nil)

	_ = txn.Set(bankKey(from), bankEncodeUint64(bankDecodeUint64(fromBal)-transferAmount))
	_ = txn.Set(bankKey(to), bankEncodeUint64(bankDecodeUint64(toBal)+transferAmount))
	_ = txn.CommitAt(readTs, nil)
	return time.Since(start)
}

// backendResult holds TPS and latency metrics for one backend run.
type backendResult struct {
	backend string
	tps     float64
	avg     time.Duration
	p90     time.Duration
	p99     time.Duration
	min     time.Duration
	max     time.Duration
}

// runBankOnBackend runs a bank transfer workload on the given db (either
// regular Badger or DuckDB) and returns metrics.
func runBankOnBackend(
	t *testing.T,
	db *DB,
	oracle *divytime.Oracle,
	backend string,
	seedFn func(tb testing.TB, db *DB, oracle *divytime.Oracle),
	xferFn func(tb testing.TB, db *DB, oracle *divytime.Oracle, rng *rand.Rand) time.Duration,
	dur time.Duration,
	workers int,
) backendResult {
	t.Helper()

	seedFn(t, db, oracle)

	stats := newBankStats()
	var totalOps atomic.Int64
	var stop int32
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for atomic.LoadInt32(&stop) == 0 {
				d := xferFn(t, db, oracle, rng)
				stats.record(txTransfer, d)
				totalOps.Add(1)
			}
		}(w)
	}

	time.Sleep(dur)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
	elapsed := time.Since(start)

	s := stats.summarize(txTransfer)
	return backendResult{
		backend: backend,
		tps:     float64(totalOps.Load()) / elapsed.Seconds(),
		avg:     s.avg,
		p90:     s.p90,
		p99:     s.p99,
		min:     s.min,
		max:     s.max,
	}
}

// ---------------------------------------------------------------------------
// TestBankBadgerVsDuckDB
// ---------------------------------------------------------------------------

// TestBankBadgerVsDuckDB runs the identical bank transfer workload on the
// regular Badger backend and the DuckDB backend, then prints a side-by-side
// comparison table.
//
// Both runs use:
//   - numBankAccounts accounts
//   - numBankWorkers concurrent goroutines
//   - bankRunDuration seconds each
//   - zero oracle simulated delay
//
// Run with:
//
//	go test -v -tags duckdb -run TestBankBadgerVsDuckDB -timeout 120s
func TestBankBadgerVsDuckDB(t *testing.T) {
	const (
		cmpDuration = 2 * time.Second
		cmpWorkers  = 16
	)

	var badgerResult, duckdbResult backendResult

	// ── Regular Badger ────────────────────────────────────────────────────
	t.Log("[comparison] running bank workload on regular Badger …")
	withDB(t, true, func(db *DB) {
		oracle := divytime.NewOracle(1, 0)
		badgerResult = runBankOnBackend(
			t, db, oracle, "Badger",
			seedBadgerAccounts,
			execBadgerTransfer,
			cmpDuration, cmpWorkers,
		)
	})

	// ── DuckDB ────────────────────────────────────────────────────────────
	t.Log("[comparison] running bank workload on DuckDB …")
	withDuckDB(t, true, func(db *DB) {
		oracle := divytime.NewOracle(1, 0)
		duckdbResult = runBankOnBackend(
			t, db, oracle, "DuckDB",
			seedDuckDBAccounts,
			func(tb testing.TB, db *DB, oracle *divytime.Oracle, rng *rand.Rand) time.Duration {
				return execTransfer(tb, db, oracle, rng)
			},
			cmpDuration, cmpWorkers,
		)
	})

	// ── Print comparison table ────────────────────────────────────────────
	printBackendComparison(t, []backendResult{badgerResult, duckdbResult})
}

// TestBankBadgerVsDuckDBWithDelay repeats the comparison with a 50 µs oracle
// delay so the relative DuckDB overhead (or benefit) is visible when the
// timestamp-oracle cost is non-trivial.
//
// Run with:
//
//	go test -v -tags duckdb -run TestBankBadgerVsDuckDBWithDelay -timeout 120s
func TestBankBadgerVsDuckDBWithDelay(t *testing.T) {
	const (
		cmpDuration = 2 * time.Second
		cmpWorkers  = 16
		oracleDelay = 50 * time.Microsecond
	)

	var badgerResult, duckdbResult backendResult

	t.Logf("[comparison] oracle simulated delay: %v", oracleDelay)

	withDB(t, true, func(db *DB) {
		oracle := divytime.NewOracle(1, oracleDelay)
		badgerResult = runBankOnBackend(
			t, db, oracle, "Badger (50µs delay)",
			seedBadgerAccounts,
			execBadgerTransfer,
			cmpDuration, cmpWorkers,
		)
	})

	withDuckDB(t, true, func(db *DB) {
		oracle := divytime.NewOracle(1, oracleDelay)
		duckdbResult = runBankOnBackend(
			t, db, oracle, "DuckDB (50µs delay)",
			seedDuckDBAccounts,
			func(tb testing.TB, db *DB, oracle *divytime.Oracle, rng *rand.Rand) time.Duration {
				return execTransfer(tb, db, oracle, rng)
			},
			cmpDuration, cmpWorkers,
		)
	})

	printBackendComparison(t, []backendResult{badgerResult, duckdbResult})
}

// printBackendComparison prints a formatted comparison table.
func printBackendComparison(t *testing.T, results []backendResult) {
	t.Helper()
	t.Logf("")
	t.Logf("=== Backend Comparison: Bank Transfer Workload ===")
	t.Logf("  %-26s  %-12s  %-12s  %-12s  %-12s  %-12s  %-12s",
		"Backend", "TPS", "Avg Latency", "p90 Latency", "p99 Latency", "Min Latency", "Max Latency")
	t.Logf("  %s", "-------------------------------------------------------------------------------------------------")
	for _, r := range results {
		t.Logf("  %-26s  %-12.0f  %-12v  %-12v  %-12v  %-12v  %-12v",
			r.backend,
			r.tps,
			r.avg.Round(time.Microsecond),
			r.p90.Round(time.Microsecond),
			r.p99.Round(time.Microsecond),
			r.min.Round(time.Microsecond),
			r.max.Round(time.Microsecond))
	}
	t.Logf("")

	if len(results) == 2 {
		ratio := results[1].tps / results[0].tps
		if ratio >= 1.0 {
			t.Logf("  DuckDB is %.2fx faster than regular Badger", ratio)
		} else {
			t.Logf("  Regular Badger is %.2fx faster than DuckDB (expected for in-memory workload)", 1/ratio)
		}
	}
}

// ---------------------------------------------------------------------------
// TestSmallBankBadgerVsDuckDB
// ---------------------------------------------------------------------------

// TestSmallBankBadgerVsDuckDB runs a mixed SmallBank workload (BenchBase
// weights 15/15/15/25/15/15) on both backends for sbBenchDur each and
// prints a side-by-side per-type TPS comparison.
//
// Run with:
//
//	go test -v -tags duckdb -run TestSmallBankBadgerVsDuckDB -timeout 300s
func TestSmallBankBadgerVsDuckDB(t *testing.T) {
	const cmpWorkers = 16
	cmpDur := sbBenchDur // reuse SmallBank constant (10s)

	type typeResult struct {
		name string
		tps  float64
		mean time.Duration
	}

	runMixed := func(db *DB, oracle *divytime.Oracle) []typeResult {
		fns := []sbTxFn{
			sbAmalgamate, sbBalance, sbDepositChecking,
			sbSendPayment, sbTransactSavings, sbWriteCheck,
		}
		names := []string{
			"Amalgamate", "Balance", "DepositChecking",
			"SendPayment", "TransactSavings", "WriteCheck",
		}
		// Build cumulative weights (same as TestSmallBankDuckDBMixed).
		cumulative := make([]int, len(sbWeights))
		cumulative[0] = sbWeights[0]
		for i := 1; i < len(sbWeights); i++ {
			cumulative[i] = cumulative[i-1] + sbWeights[i]
		}
		total := cumulative[len(cumulative)-1]

		perType := make([]*sbStats, len(fns))
		for i := range perType {
			perType[i] = &sbStats{}
		}

		var stop int32
		var wg sync.WaitGroup
		start := time.Now()

		for w := 0; w < cmpWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for atomic.LoadInt32(&stop) == 0 {
					pick := rng.Intn(total)
					idx := 0
					for idx < len(cumulative)-1 && pick >= cumulative[idx] {
						idx++
					}
					d, err := fns[idx](db, oracle, rng)
					perType[idx].record(d, err)
				}
			}()
		}

		time.Sleep(cmpDur)
		atomic.StoreInt32(&stop, 1)
		wg.Wait()
		elapsed := time.Since(start)

		var out []typeResult
		for i, name := range names {
			r := perType[i].result(elapsed)
			out = append(out, typeResult{name, r.tps, r.mean})
		}
		return out
	}

	// ── Regular Badger ────────────────────────────────────────────────────
	t.Log("[comparison] seeding and running SmallBank on regular Badger …")
	var badgerTypes []typeResult
	withDB(t, true, func(db *DB) {
		oracle := divytime.NewOracle(1, 0)
		sbSeedBadger(t, db, oracle)
		badgerTypes = runMixed(db, oracle)
	})

	// ── DuckDB ────────────────────────────────────────────────────────────
	t.Log("[comparison] seeding and running SmallBank on DuckDB …")
	var duckdbTypes []typeResult
	withDuckDB(t, true, func(db *DB) {
		oracle := divytime.NewOracle(1, 0)
		sbSeed(t, db, oracle)
		duckdbTypes = runMixed(db, oracle)
	})

	// ── Print table ───────────────────────────────────────────────────────
	t.Logf("")
	t.Logf("=== SmallBank Mixed Workload: Badger vs DuckDB ===")
	t.Logf("  %-20s  %-16s  %-16s  %-16s  %-16s  %s",
		"Transaction", "Badger TPS", "DuckDB TPS", "Badger Mean", "DuckDB Mean", "Ratio (DuckDB/Badger)")
	t.Logf("  %s", "-------------------------------------------------------------------------------------------------------")

	for i := range badgerTypes {
		bt := badgerTypes[i]
		dt := duckdbTypes[i]
		ratio := 0.0
		if bt.tps > 0 {
			ratio = dt.tps / bt.tps
		}
		t.Logf("  %-20s  %-16.0f  %-16.0f  %-16v  %-16v  %.2fx",
			bt.name,
			bt.tps, dt.tps,
			bt.mean.Round(time.Microsecond),
			dt.mean.Round(time.Microsecond),
			ratio)
	}
	t.Logf("")
}

// sbSeedBadger seeds numSmallBank accounts into a regular Badger DB.
// Uses the same key format as sbSeed so the workload functions are portable.
func sbSeedBadger(tb testing.TB, db *DB, oracle *divytime.Oracle) {
	tb.Helper()
	tb.Log("[smallbank] seeding", sbNumCustomers, "accounts into Badger…")
	start := time.Now()

	for i := int64(0); i < sbNumCustomers; i++ {
		ts := sbTs(oracle)
		txn := db.NewTransactionAt(ts, true)
		_ = txn.Set(sbAccountKey(i), []byte("cust"))
		_ = txn.Set(sbSavingsKey(i), sbEncode(sbInitBal))
		_ = txn.Set(sbCheckingKey(i), sbEncode(sbInitBal))
		if err := txn.CommitAt(ts, nil); err != nil {
			tb.Fatalf("sbSeedBadger commit i=%d: %v", i, err)
		}
	}
	tb.Logf("[smallbank] seeded in %v", time.Since(start).Round(time.Millisecond))
	// Silence the oracle parameter (it is unused but kept for API parity with sbSeed).
	_ = oracle
}
