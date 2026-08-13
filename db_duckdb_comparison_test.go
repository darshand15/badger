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
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4/divytime"
	"github.com/dgraph-io/badger/v4/types"
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

// ---------------------------------------------------------------------------
// TestReadHeavyBalanceBadgerVsDuckDB
// ---------------------------------------------------------------------------

// TestReadHeavyBalanceBadgerVsDuckDB compares a read-heavy SmallBank pattern
// (Balance txn: 3 snapshot reads, 0 writes). This is a workload shape where
// the DuckDB backend is expected to be competitive.
func TestReadHeavyBalanceBadgerVsDuckDB(t *testing.T) {
	const (
		cmpDuration = 2 * time.Second
		cmpWorkers  = 8
	)

	type analyticalResult struct {
		backend string
		opS     float64
		avg     time.Duration
		p90     time.Duration
	}

	runAnalytical := func(
		t *testing.T,
		backend string,
		runFn func(rng *rand.Rand) (time.Duration, error),
	) analyticalResult {
		t.Helper()
		stats := newBankStats()
		var totalOps atomic.Int64
		var stop int32
		var wg sync.WaitGroup

		start := time.Now()
		for w := 0; w < cmpWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
				for atomic.LoadInt32(&stop) == 0 {
					d, err := runFn(rng)
					if err == nil {
						stats.record(txSumCheck, d)
						totalOps.Add(1)
					}
				}
			}(w)
		}

		time.Sleep(cmpDuration)
		atomic.StoreInt32(&stop, 1)
		wg.Wait()
		elapsed := time.Since(start)

		s := stats.summarize(txSumCheck)
		return analyticalResult{
			backend: backend,
			opS:     float64(totalOps.Load()) / elapsed.Seconds(),
			avg:     s.avg,
			p90:     s.p90,
		}
	}

	var badgerResult analyticalResult
	withDB(t, true, func(db *DB) {
		oracle := divytime.NewOracle(1, 0)
		sbSeedBadger(t, db, oracle)
		badgerResult = runAnalytical(t, "Badger (Balance)", func(rng *rand.Rand) (time.Duration, error) {
			return sbBalance(db, oracle, rng)
		})
	})

	var duckdbResult analyticalResult
	withDuckDB(t, true, func(db *DB) {
		oracle := divytime.NewOracle(1, 0)
		sbSeed(t, db, oracle)
		duckdbResult = runAnalytical(t, "DuckDB (Balance)", func(rng *rand.Rand) (time.Duration, error) {
			return sbBalance(db, oracle, rng)
		})
	})

	t.Logf("")
	t.Logf("=== Read-Heavy Comparison (SmallBank Balance txn) ===")
	t.Logf("  %-26s  %-12s  %-12s  %-12s", "Backend", "Ops/sec", "Avg Latency", "p90 Latency")
	t.Logf("  %s", "------------------------------------------------------------------")
	t.Logf("  %-26s  %-12.1f  %-12v  %-12v",
		badgerResult.backend,
		badgerResult.opS,
		badgerResult.avg.Round(time.Microsecond),
		badgerResult.p90.Round(time.Microsecond),
	)
	t.Logf("  %-26s  %-12.1f  %-12v  %-12v",
		duckdbResult.backend,
		duckdbResult.opS,
		duckdbResult.avg.Round(time.Microsecond),
		duckdbResult.p90.Round(time.Microsecond),
	)

	if badgerResult.opS > 0 {
		ratio := duckdbResult.opS / badgerResult.opS
		if ratio >= 1 {
			t.Logf("  DuckDB is %.2fx faster for this read-heavy pattern", ratio)
		} else {
			t.Logf("  Badger is %.2fx faster for this read-heavy pattern", 1/ratio)
		}
	}
}

// seedSmallBankN seeds exactly n customers using the SmallBank key layout.
func seedSmallBankN(tb testing.TB, db *DB, oracle *divytime.Oracle, n int64) {
	tb.Helper()
	const custPrefix = "cust_"
	seedMode := strings.ToLower(strings.TrimSpace(os.Getenv("BADGER_DUCKDB_SEED_KEY_MODE")))
	fullSeed := seedMode != "checking-only" && seedMode != "checking"
	accountValueMode := strings.ToLower(strings.TrimSpace(os.Getenv("BADGER_DUCKDB_ACCOUNT_VALUE_MODE")))
	compactAccountValue := accountValueMode == "compact" || (accountValueMode == "" && n >= 10_000_000)
	batchSize := parsePositiveIntEnv("BADGER_DUCKDB_SEED_BATCH_SIZE", 0)
	if batchSize <= 0 {
		if n >= 40_000_000 {
			batchSize = 2000
		} else if n >= 1_000_000 {
			batchSize = 1000
		} else {
			batchSize = 1
		}
	}
	checkingValue := sbEncode(sbInitBal)
	savingsValue := sbEncode(sbInitBal)
	compactAccountValueBytes := []byte("cust")

	for start := int64(0); start < n; start += int64(batchSize) {
		end := start + int64(batchSize)
		if end > n {
			end = n
		}

		ts := sbTs(oracle)
		txn := db.NewTransactionAt(ts, true)
		nameBuf := make([]byte, 0, len(custPrefix)+20)
		accountValue := compactAccountValueBytes
		for i := start; i < end; i++ {
			if fullSeed {
				if !compactAccountValue {
					name := nameBuf[:0]
					name = append(name, custPrefix...)
					name = strconv.AppendInt(name, i, 10)
					accountValue = append([]byte(nil), name...)
				}
				if err := txn.Set(sbAccountKey(i), accountValue); err != nil {
					txn.Discard()
					tb.Fatalf("seed set account i=%d: %v", i, err)
				}
				if err := txn.Set(sbSavingsKey(i), savingsValue); err != nil {
					txn.Discard()
					tb.Fatalf("seed set savings i=%d: %v", i, err)
				}
			}
			if err := txn.Set(sbCheckingKey(i), checkingValue); err != nil {
				txn.Discard()
				tb.Fatalf("seed set checking i=%d: %v", i, err)
			}
		}

		if err := txn.CommitAt(ts, nil); err != nil {
			txn.Discard()
			tb.Fatalf("seed commit customers [%d,%d): %v", start, end, err)
		}
		txn.Discard()

		if n >= 10_000_000 && (end == n || end%5_000_000 == 0) {
			if fullSeed {
				tb.Logf("  seeded %d/%d customers (batch_size=%d, keys=full)", end, n, batchSize)
			} else {
				tb.Logf("  seeded %d/%d customers (batch_size=%d, keys=checking-only)", end, n, batchSize)
			}
		}
	}
}

func parsePositiveIntEnv(name string, def int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func parseDurationEnvCmp(name string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

func parseInt64ListEnv(name string, defaults []int64) []int64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return append([]int64(nil), defaults...)
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\t' || r == '\n'
	})
	if len(parts) == 0 {
		return append([]int64(nil), defaults...)
	}
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.ParseInt(strings.TrimSpace(p), 10, 64)
		if err != nil || n <= 0 {
			continue
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return append([]int64(nil), defaults...)
	}
	return out
}

func parseIntListEnv(name string, defaults []int) []int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return append([]int(nil), defaults...)
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\t' || r == '\n'
	})
	if len(parts) == 0 {
		return append([]int(nil), defaults...)
	}
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || n <= 0 {
			continue
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return append([]int(nil), defaults...)
	}
	return out
}

func parseBoolEnv(name string, def bool) bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	if raw == "" {
		return def
	}
	switch raw {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		return def
	}
}

type readHeavyResult struct {
	backend string
	ops     float64
	opsCount int64
	elapsed  time.Duration
	avg     time.Duration
	p90     time.Duration
}

func sbKeyInto(buf []byte, id int64, suffix string) []byte {
	buf = buf[:0]
	buf = strconv.AppendInt(buf, id, 10)
	buf = append(buf, ':')
	buf = append(buf, suffix...)
	return buf
}

func runBalanceReadHeavy(
	t *testing.T,
	backend string,
	db *DB,
	oracle *divytime.Oracle,
	numCustomers int64,
	dur time.Duration,
	workers int,
) readHeavyResult {
	t.Helper()
	stats := newBankStats()
	var totalOps atomic.Int64
	var stop int32
	var wg sync.WaitGroup
	readMode := strings.ToLower(strings.TrimSpace(os.Getenv("BADGER_DUCKDB_READ_HEAVY_KEY_MODE")))
	checkingOnly := readMode == "checking-only" || readMode == "checking"
	useFixedSnapshot := parseBoolEnv("BADGER_DUCKDB_READ_HEAVY_FIXED_SNAPSHOT", true)
	var fixedReadTs types.CustomTs
	if useFixedSnapshot {
		fixedReadTs = sbTs(oracle)
	}

	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			chkKey := make([]byte, 0, 32)
			acctKey := make([]byte, 0, 32)
			savKey := make([]byte, 0, 32)
			prefetch := make([][]byte, 0, 3)
			for atomic.LoadInt32(&stop) == 0 {
				id := rng.Int63n(numCustomers)
				chkKey = sbKeyInto(chkKey, id, "checking_bal")
				prefetch = prefetch[:0]
				if !checkingOnly {
					acctKey = sbKeyInto(acctKey, id, "accounts_id")
					savKey = sbKeyInto(savKey, id, "savings_bal")
					prefetch = append(prefetch, acctKey, savKey)
				}
				prefetch = append(prefetch, chkKey)

				ts := fixedReadTs
				if !useFixedSnapshot {
					ts = sbTs(oracle)
				}
				t0 := time.Now()

				ok := false
				if db.duckDBStorage != nil && useFixedSnapshot {
					vChk, _, errChk := db.duckDBStorage.Read(chkKey, ts)
					if errChk == nil && vChk != nil {
						if checkingOnly {
							ok = true
						} else {
							vAcc, _, errAcc := db.duckDBStorage.Read(acctKey, ts)
							vSav, _, errSav := db.duckDBStorage.Read(savKey, ts)
							ok = errAcc == nil && errSav == nil && vAcc != nil && vSav != nil
						}
					}
				} else {
					txn := db.NewTransactionAt(ts, false)
					_ = txn.PrefetchKeys(prefetch)
					_, errC := txn.Get(chkKey)
					errA := error(nil)
					errS := error(nil)
					if !checkingOnly {
						_, errA = txn.Get(acctKey)
						_, errS = txn.Get(savKey)
					}
					txn.Discard()
					ok = errA == nil && errS == nil && errC == nil
				}

				if ok {
					d := time.Since(t0)
					stats.record(txReadOnly, d)
					totalOps.Add(1)
				}
			}
		}(w)
	}

	time.Sleep(dur)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
	elapsed := time.Since(start)

	s := stats.summarize(txReadOnly)
	total := totalOps.Load()
	return readHeavyResult{
		backend: backend,
		ops:     float64(total) / elapsed.Seconds(),
		opsCount: total,
		elapsed: elapsed,
		avg:     s.avg,
		p90:     s.p90,
	}
}

func runBalanceReadHeavyMinOps(
	t *testing.T,
	backend string,
	db *DB,
	oracle *divytime.Oracle,
	numCustomers int64,
	chunkDur time.Duration,
	maxDuration time.Duration,
	workers int,
	minOps int64,
) readHeavyResult {
	t.Helper()
	if chunkDur <= 0 {
		chunkDur = 2 * time.Second
	}
	if maxDuration < chunkDur {
		maxDuration = chunkDur
	}

	var (
		totalOps     int64
		totalElapsed time.Duration
		weightedAvg  float64
		maxP90       time.Duration
	)

	for totalElapsed < maxDuration && totalOps < minOps {
		roundDur := chunkDur
		if remain := maxDuration - totalElapsed; roundDur > remain {
			roundDur = remain
		}
		r := runBalanceReadHeavy(t, backend, db, oracle, numCustomers, roundDur, workers)
		totalOps += r.opsCount
		totalElapsed += r.elapsed
		if r.opsCount > 0 {
			weightedAvg += float64(r.avg.Nanoseconds()) * float64(r.opsCount)
		}
		if r.p90 > maxP90 {
			maxP90 = r.p90
		}
		if r.opsCount == 0 && roundDur == (maxDuration-totalElapsed+r.elapsed) {
			break
		}
	}

	avg := time.Duration(0)
	if totalOps > 0 {
		avg = time.Duration(weightedAvg / float64(totalOps))
	}
	opsPerSec := 0.0
	if totalElapsed > 0 {
		opsPerSec = float64(totalOps) / totalElapsed.Seconds()
	}

	return readHeavyResult{
		backend: backend,
		ops:     opsPerSec,
		opsCount: totalOps,
		elapsed: totalElapsed,
		avg:     avg,
		p90:     maxP90,
	}
}

// TestReadHeavyBalanceCardinalitySweepBadgerVsDuckDB produces crossover-style
// data by sweeping customer-cardinality for a read-heavy Balance workload.
func TestReadHeavyBalanceCardinalitySweepBadgerVsDuckDB(t *testing.T) {
	const (
		cmpWorkers = 8
	)
	cmpDuration := parseDurationEnvCmp("BADGER_DUCKDB_READHEAVY_DURATION", 2*time.Second)
	maxDuration := parseDurationEnvCmp("BADGER_DUCKDB_READHEAVY_MAX_DURATION", 10*time.Minute)
	minOps := int64(parsePositiveIntEnv("BADGER_DUCKDB_READHEAVY_MIN_OPS", 30))
	postSeedFlattenWorkers := parsePositiveIntEnv("BADGER_DUCKDB_POST_SEED_FLATTEN_WORKERS", 0)

	cardinalities := parseInt64ListEnv("BADGER_DUCKDB_SWEEP_CARDINALITIES", []int64{1_000, 5_000, 20_000, 100_000})
	backendMode := strings.ToLower(strings.TrimSpace(os.Getenv("BADGER_DUCKDB_SWEEP_BACKEND")))
	if backendMode == "" {
		backendMode = "both"
	}
	if backendMode != "both" && backendMode != "badger" && backendMode != "duckdb" {
		t.Fatalf("invalid BADGER_DUCKDB_SWEEP_BACKEND=%q (expected both|badger|duckdb)", backendMode)
	}
	type csvRow struct {
		customers      int64
		badgerOps      float64
		duckdbOps      float64
		badgerOpsCount int64
		duckdbOpsCount int64
		badgerElapsed  time.Duration
		duckdbElapsed  time.Duration
		ratio          float64
		badgerAvg      time.Duration
		badgerP90      time.Duration
		duckdbAvg      time.Duration
		duckdbP90      time.Duration
	}
	var csvRows []csvRow

	t.Logf("")
	t.Logf("=== Read-Heavy Balance Cardinality Sweep (Badger vs DuckDB) ===")
	t.Logf("  config: chunk_duration=%v max_duration=%v min_ops_per_backend=%d workers=%d backend_mode=%s post_seed_flatten_workers=%d", cmpDuration, maxDuration, minOps, cmpWorkers, backendMode, postSeedFlattenWorkers)
	t.Logf("  %-10s  %-14s  %-14s  %-18s", "Customers", "Badger Ops/s", "DuckDB Ops/s", "DuckDB/Badger")
	t.Logf("  %s", "----------------------------------------------------------------")

	firstDuckdbWin := int64(-1)

	for _, n := range cardinalities {
		badger := readHeavyResult{backend: "Badger"}
		if backendMode == "both" || backendMode == "badger" {
			withDB(t, true, func(db *DB) {
				oracle := divytime.NewOracle(1, 0)
				seedSmallBankN(t, db, oracle, n)
				if postSeedFlattenWorkers > 0 {
					startFlatten := time.Now()
					if err := db.Flatten(postSeedFlattenWorkers); err != nil {
						t.Fatalf("post-seed flatten (workers=%d): %v", postSeedFlattenWorkers, err)
					}
					t.Logf("  post-seed flatten complete for badger customers=%d workers=%d elapsed=%v", n, postSeedFlattenWorkers, time.Since(startFlatten).Round(time.Millisecond))
				}
				badger = runBalanceReadHeavyMinOps(t, "Badger", db, oracle, n, cmpDuration, maxDuration, cmpWorkers, minOps)
			})
		}
		runtime.GC()
		debug.FreeOSMemory()

		duckdb := readHeavyResult{backend: "DuckDB"}
		if backendMode == "both" || backendMode == "duckdb" {
			withDuckDB(t, true, func(db *DB) {
				oracle := divytime.NewOracle(1, 0)
				seedSmallBankN(t, db, oracle, n)
				duckdb = runBalanceReadHeavyMinOps(t, "DuckDB", db, oracle, n, cmpDuration, maxDuration, cmpWorkers, minOps)
			})
		}
		runtime.GC()
		debug.FreeOSMemory()

		if (backendMode == "both" || backendMode == "badger") && badger.opsCount < minOps {
			t.Fatalf("insufficient samples for badger at customers=%d: ops=%d min_ops=%d elapsed=%v. Increase BADGER_DUCKDB_READHEAVY_MAX_DURATION or reduce cardinality.",
				n,
				badger.opsCount,
				minOps,
				badger.elapsed.Round(time.Millisecond),
			)
		}
		if (backendMode == "both" || backendMode == "duckdb") && duckdb.opsCount < minOps {
			t.Fatalf("insufficient samples at customers=%d: badger_ops=%d duckdb_ops=%d min_ops=%d (badger_elapsed=%v duckdb_elapsed=%v). Increase BADGER_DUCKDB_READHEAVY_MAX_DURATION or reduce cardinality.",
				n,
				badger.opsCount,
				duckdb.opsCount,
				minOps,
				badger.elapsed.Round(time.Millisecond),
				duckdb.elapsed.Round(time.Millisecond),
			)
		}

		ratio := 0.0
		if backendMode == "both" && badger.ops > 0 {
			ratio = duckdb.ops / badger.ops
		}
		if backendMode == "both" && firstDuckdbWin < 0 && ratio >= 1.0 {
			firstDuckdbWin = n
		}
		csvRows = append(csvRows, csvRow{
			customers:      n,
			badgerOps:      badger.ops,
			duckdbOps:      duckdb.ops,
			badgerOpsCount: badger.opsCount,
			duckdbOpsCount: duckdb.opsCount,
			badgerElapsed:  badger.elapsed,
			duckdbElapsed:  duckdb.elapsed,
			ratio:          ratio,
			badgerAvg:      badger.avg,
			badgerP90:      badger.p90,
			duckdbAvg:      duckdb.avg,
			duckdbP90:      duckdb.p90,
		})

		t.Logf("  %-10d  %-14.1f  %-14.1f  %-18.2fx", n, badger.ops, duckdb.ops, ratio)
		t.Logf("    sample_ops: badger=%d duckdb=%d (badger_elapsed=%v duckdb_elapsed=%v)",
			badger.opsCount,
			duckdb.opsCount,
			badger.elapsed.Round(time.Millisecond),
			duckdb.elapsed.Round(time.Millisecond),
		)
		t.Logf("    badger avg=%v p90=%v | duckdb avg=%v p90=%v",
			badger.avg.Round(time.Microsecond),
			badger.p90.Round(time.Microsecond),
			duckdb.avg.Round(time.Microsecond),
			duckdb.p90.Round(time.Microsecond),
		)
	}

	if backendMode == "both" && firstDuckdbWin > 0 {
		t.Logf("  Crossover observed: DuckDB first wins at %d customers", firstDuckdbWin)
	} else if backendMode == "both" {
		t.Logf("  No crossover in this sweep: Badger remains ahead at tested cardinalities")
	} else {
		t.Logf("  Backend-isolated mode (%s): ratio/crossover intentionally not computed in-process", backendMode)
	}

	if outPath := os.Getenv("BADGER_DUCKDB_SWEEP_CSV"); outPath != "" {
		csv := "customers,badger_ops_per_sec,duckdb_ops_per_sec,duckdb_over_badger,badger_total_ops,duckdb_total_ops,badger_elapsed_ns,duckdb_elapsed_ns,badger_avg_ns,badger_p90_ns,duckdb_avg_ns,duckdb_p90_ns\n"
		for _, r := range csvRows {
			csv += fmt.Sprintf("%d,%.3f,%.3f,%.6f,%d,%d,%d,%d,%d,%d,%d,%d\n",
				r.customers,
				r.badgerOps,
				r.duckdbOps,
				r.ratio,
				r.badgerOpsCount,
				r.duckdbOpsCount,
				r.badgerElapsed.Nanoseconds(),
				r.duckdbElapsed.Nanoseconds(),
				r.badgerAvg.Nanoseconds(),
				r.badgerP90.Nanoseconds(),
				r.duckdbAvg.Nanoseconds(),
				r.duckdbP90.Nanoseconds(),
			)
		}
		if err := os.WriteFile(outPath, []byte(csv), 0644); err != nil {
			t.Fatalf("write sweep csv: %v", err)
		}
		t.Logf("  Wrote sweep CSV: %s", outPath)
	}

	if outPath := os.Getenv("BADGER_DUCKDB_SWEEP_PARTIAL_CSV"); outPath != "" {
		csv := "customers,backend,ops_per_sec,total_ops,elapsed_ns,avg_ns,p90_ns\n"
		for _, r := range csvRows {
			if backendMode == "both" || backendMode == "badger" {
				csv += fmt.Sprintf("%d,badger,%.3f,%d,%d,%d,%d\n",
					r.customers,
					r.badgerOps,
					r.badgerOpsCount,
					r.badgerElapsed.Nanoseconds(),
					r.badgerAvg.Nanoseconds(),
					r.badgerP90.Nanoseconds(),
				)
			}
			if backendMode == "both" || backendMode == "duckdb" {
				csv += fmt.Sprintf("%d,duckdb,%.3f,%d,%d,%d,%d\n",
					r.customers,
					r.duckdbOps,
					r.duckdbOpsCount,
					r.duckdbElapsed.Nanoseconds(),
					r.duckdbAvg.Nanoseconds(),
					r.duckdbP90.Nanoseconds(),
				)
			}
		}
		if err := os.WriteFile(outPath, []byte(csv), 0644); err != nil {
			t.Fatalf("write sweep partial csv: %v", err)
		}
		t.Logf("  Wrote sweep partial CSV: %s", outPath)
	}
}

// TestReadHeavyBalanceCardinalityConcurrencySweepBadgerVsDuckDB sweeps both
// customer cardinality and worker concurrency for the Balance transaction.
// It emits a matrix suitable for crossover plots and tuning decisions.
func TestReadHeavyBalanceCardinalityConcurrencySweepBadgerVsDuckDB(t *testing.T) {
	const cmpDuration = 1 * time.Second

	cardinalities := parseInt64ListEnv("BADGER_DUCKDB_SWEEP_CONC_CARDINALITIES", []int64{5_000, 20_000, 100_000})
	workers := parseIntListEnv("BADGER_DUCKDB_SWEEP_WORKERS", []int{4, 8, 16, 32, 64, 128})

	type matrixRow struct {
		customers int64
		workers   int
		badgerOps float64
		duckdbOps float64
		ratio     float64
	}
	var rows []matrixRow

	t.Logf("")
	t.Logf("=== Read-Heavy Balance Cardinality x Concurrency Sweep ===")
	t.Logf("  %-10s  %-8s  %-14s  %-14s  %-14s", "Customers", "Workers", "Badger Ops/s", "DuckDB Ops/s", "DuckDB/Badger")
	t.Logf("  %s", "--------------------------------------------------------------------------")

	for _, n := range cardinalities {
		withDB(t, true, func(db *DB) {
			oracle := divytime.NewOracle(1, 0)
			seedSmallBankN(t, db, oracle, n)
			for _, w := range workers {
				badger := runBalanceReadHeavy(t, "Badger", db, oracle, n, cmpDuration, w)

				withDuckDB(t, true, func(ddb *DB) {
					doracle := divytime.NewOracle(1, 0)
					seedSmallBankN(t, ddb, doracle, n)
					duck := runBalanceReadHeavy(t, "DuckDB", ddb, doracle, n, cmpDuration, w)

					ratio := 0.0
					if badger.ops > 0 {
						ratio = duck.ops / badger.ops
					}
					rows = append(rows, matrixRow{
						customers: n,
						workers:   w,
						badgerOps: badger.ops,
						duckdbOps: duck.ops,
						ratio:     ratio,
					})

					t.Logf("  %-10d  %-8d  %-14.1f  %-14.1f  %-14.2fx", n, w, badger.ops, duck.ops, ratio)
				})
			}
		})
	}

	if outPath := os.Getenv("BADGER_DUCKDB_SWEEP_CONC_CSV"); outPath != "" {
		csv := "customers,workers,badger_ops_per_sec,duckdb_ops_per_sec,duckdb_over_badger\n"
		for _, r := range rows {
			csv += fmt.Sprintf("%d,%d,%.3f,%.3f,%.6f\n", r.customers, r.workers, r.badgerOps, r.duckdbOps, r.ratio)
		}
		if err := os.WriteFile(outPath, []byte(csv), 0644); err != nil {
			t.Fatalf("write concurrency sweep csv: %v", err)
		}
		t.Logf("  Wrote concurrency sweep CSV: %s", outPath)
	}
}
