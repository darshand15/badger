//go:build duckdb

package badger

// db_duckdb_stress_test.go — sweeps worker counts to find peak TPS and verify
// the balance invariant holds under higher concurrency.
//
// Run with:
//
//	go test -v -tags duckdb -run TestDuckDBBankStress -timeout 300s
//
// Each sub-test runs for stressDuration seconds then verifies total == 1,000,000.
// Results are printed per sub-test and summarised at the end.

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"math/rand"

	"github.com/dgraph-io/badger/v4/divytime"
	"github.com/dgraph-io/badger/v4/types"
)

const stressDuration = 2 * time.Second

// stressResult holds the numbers we care about for one configuration.
type stressResult struct {
	label       string
	workers     int
	delay       time.Duration
	totalOps    int64
	transferOps int64
	tps         float64
	transferTPS float64
	transferAvg time.Duration
	transferP90 time.Duration
	sumAvg      time.Duration
	invariantOK bool
}

// runStressConfig executes the bank workload for one (workers, delay) pair and
// returns the collected metrics.
func runStressConfig(t *testing.T, workers int, delay time.Duration) stressResult {
	t.Helper()

	oracle := divytime.NewOracle(1, delay)
	label := fmt.Sprintf("workers=%d delay=%v", workers, delay)

	var result stressResult
	result.label = label
	result.workers = workers
	result.delay = delay

	withDuckDB(t, true, func(db *DB) {
		// Seed accounts (reuse existing helper).
		seedDuckDBAccounts(t, db, oracle)

		stats := newBankStats()
		var (
			transferOps atomic.Int64
			readOps     atomic.Int64
			sumChecks   atomic.Int64
			stop        int32
			wg          sync.WaitGroup
		)

		startTime := time.Now()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

				for atomic.LoadInt32(&stop) == 0 {
					r := rng.Intn(100)
					switch {
					case r < 70: // 70 % transfers
						d := execTransfer(t, db, oracle, rng)
						stats.record(txTransfer, d)
						transferOps.Add(1)

					case r < 95: // 25 % read-only
						start := time.Now()
						ts, _ := oracle.GetTimestamp(int64(time.Now().UnixNano()))
						txn := db.NewTransactionAt(divyToTs(ts), false)
						acc := rng.Intn(numBankAccounts)
						item, err := txn.Get(bankKey(acc))
						if err == nil {
							_, _ = item.ValueCopy(nil)
						}
						txn.Discard()
						stats.record(txReadOnly, time.Since(start))
						readOps.Add(1)

					default: // 5 % sum checks
						start := time.Now()
						// Single ScanPrefix (1 query/partition) instead of 1,000
						// point reads; invariant is asserted in the final verify.
						_, _ = execSumCheck(db, oracle)
						stats.record(txSumCheck, time.Since(start))
						sumChecks.Add(1)
					}
				}
			}(w)
		}

		time.Sleep(stressDuration)
		atomic.StoreInt32(&stop, 1)
		wg.Wait()
		elapsed := time.Since(startTime)

		totalOps := transferOps.Load() + readOps.Load() + sumChecks.Load()
		tps := float64(totalOps) / elapsed.Seconds()
		xferTPS := float64(transferOps.Load()) / elapsed.Seconds()

		xferStats := stats.summarize(txTransfer)
		sumStats := stats.summarize(txSumCheck)

		result.totalOps = totalOps
		result.transferOps = transferOps.Load()
		result.tps = tps
		result.transferTPS = xferTPS
		result.transferAvg = xferStats.avg
		result.transferP90 = xferStats.p90
		result.sumAvg = sumStats.avg

		t.Logf("--- %s ---", label)
		t.Logf("  Total ops:    %d (%.0f TPS)", totalOps, tps)
		t.Logf("  Transfers:    %d (%.0f TPS)  avg=%v  p90=%v",
			transferOps.Load(), xferTPS,
			xferStats.avg.Round(time.Microsecond),
			xferStats.p90.Round(time.Microsecond))
		t.Logf("  SUM_CHECK:    count=%d  avg=%v  p90=%v",
			sumStats.count,
			sumStats.avg.Round(time.Millisecond),
			sumStats.p90.Round(time.Millisecond))

		// Correctness check.
		txn := db.NewTransactionAt(types.MaxTs, false)
		defer txn.Discard()
		var total uint64
		for i := 0; i < numBankAccounts; i++ {
			item, err := txn.Get(bankKey(i))
			if err != nil {
				t.Errorf("verify: get account %d: %v", i, err)
				continue
			}
			v, _ := item.ValueCopy(nil)
			total += bankDecodeUint64(v)
		}
		expected := uint64(numBankAccounts) * initialBankBal
		result.invariantOK = total == expected
		if !result.invariantOK {
			t.Errorf("  INVARIANT VIOLATED: want=%d got=%d delta=%d",
				expected, total, int64(expected)-int64(total))
		} else {
			t.Logf("  Invariant: OK (total=%d)", total)
		}
	})

	return result
}

// TestDuckDBBankStress sweeps worker counts for both no-delay and 50 µs oracle
// delay scenarios, printing a summary table at the end.
func TestDuckDBBankStress(t *testing.T) {
	workerCounts := []int{4, 8, 16, 32, 64, 128}

	type config struct {
		delay time.Duration
		tag   string
	}
	configs := []config{
		{0, "no-delay"},
		{50 * time.Microsecond, "50µs-delay"},
	}

	var results []stressResult

	for _, cfg := range configs {
		for _, w := range workerCounts {
			name := fmt.Sprintf("%s/workers=%d", cfg.tag, w)
			t.Run(name, func(t *testing.T) {
				r := runStressConfig(t, w, cfg.delay)
				results = append(results, r)
			})
		}
	}

	// Print summary table.
	t.Log("")
	t.Log("============================================================")
	t.Log("  STRESS TEST SUMMARY")
	t.Log("============================================================")
	t.Logf("  %-32s  %6s  %8s  %8s  %10s  %10s  %8s",
		"Configuration", "Workers", "Total TPS", "Xfer TPS", "Xfer avg", "Xfer p90", "SUM avg")
	t.Log("  " + fmt.Sprintf("%s", "----------------------------------------------------------------"))
	for _, r := range results {
		status := "✓"
		if !r.invariantOK {
			status = "✗ FAIL"
		}
		t.Logf("  %-32s  %6d  %8.0f  %8.0f  %10v  %10v  %8v  %s",
			fmt.Sprintf("delay=%v", r.delay),
			r.workers,
			r.tps,
			r.transferTPS,
			r.transferAvg.Round(time.Microsecond),
			r.transferP90.Round(time.Microsecond),
			r.sumAvg.Round(time.Millisecond),
			status)
	}
	t.Log("============================================================")
}

// ---------------------------------------------------------------------------
// Epoch stress tests (merged from db_duckdb_epoch_stress_test.go)
// ---------------------------------------------------------------------------

// epochBatchOracle wraps a real divytime.Oracle but reserves N AssignedTs
// slots per oracle call, amortising its simulated latency across N
// transactions.
type epochBatchOracle struct {
	inner     *divytime.Oracle
	batchSize int64

	mu       sync.Mutex
	curEpoch int64
	nextSlot int64
	batchEnd int64
}

func newEpochBatchOracle(inner *divytime.Oracle, batchSize int) *epochBatchOracle {
	return &epochBatchOracle{inner: inner, batchSize: int64(batchSize)}
}

func (o *epochBatchOracle) GetTimestamp() types.CustomTs {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.nextSlot >= o.batchEnd {
		o.curEpoch++
		ts, _ := o.inner.GetTimestamp(o.curEpoch)
		o.nextSlot = ts.AssignedTs
		o.batchEnd = ts.AssignedTs + o.batchSize
	}

	assigned := o.nextSlot
	o.nextSlot++
	return types.CustomTs{
		EpochID:    uint32(o.curEpoch),
		BrokerID:   1,
		AssignedTs: uint32(assigned),
	}
}

func runEpochBankWorkload(
	t *testing.T,
	oracle *epochBatchOracle,
	dur time.Duration,
	workers int,
) (tps float64, p90 time.Duration) {
	t.Helper()

	withDuckDB(t, true, func(db *DB) {
		seedOracle := divytime.NewOracle(99, 0)
		for i := 0; i < numBankAccounts; i++ {
			ts, _ := seedOracle.GetTimestamp(int64(i) + 1)
			txn := db.NewTransactionAt(divyToTs(ts), true)
			if err := txn.Set(bankKey(i), bankEncodeUint64(initialBankBal)); err != nil {
				t.Fatalf("seed account %d: %v", i, err)
			}
			if err := txn.CommitAt(divyToTs(ts), nil); err != nil {
				t.Fatalf("seed commit %d: %v", i, err)
			}
		}

		stats := newBankStats()
		var (
			totalXfers atomic.Int64
			stop       int32
			wg         sync.WaitGroup
		)

		start := time.Now()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for atomic.LoadInt32(&stop) == 0 {
					ts := oracle.GetTimestamp()
					d := execEpochTransfer(t, db, ts)
					stats.record(txTransfer, d)
					totalXfers.Add(1)
				}
			}()
		}

		time.Sleep(dur)
		atomic.StoreInt32(&stop, 1)
		wg.Wait()
		elapsed := time.Since(start)

		tps = float64(totalXfers.Load()) / elapsed.Seconds()
		p90 = stats.summarize(txTransfer).p90
	})
	return
}

func execEpochTransfer(tb testing.TB, db *DB, ts types.CustomTs) time.Duration {
	tb.Helper()
	start := time.Now()

	rng := newWorkerRng()
	from := rng.Intn(numBankAccounts)
	to := rng.Intn(numBankAccounts)
	for to == from {
		to = rng.Intn(numBankAccounts)
	}

	txn := db.NewTransactionAt(ts, true)
	defer txn.Discard()

	fromKey, toKey := bankKey(from), bankKey(to)
	if err := txn.PrefetchKeys([][]byte{fromKey, toKey}); err != nil {
		return time.Since(start)
	}

	fromItem, err := txn.Get(fromKey)
	if err != nil {
		return time.Since(start)
	}
	fromBal, _ := fromItem.ValueCopy(nil)
	if bankDecodeUint64(fromBal) < transferAmount {
		return time.Since(start)
	}

	toItem, err := txn.Get(toKey)
	if err != nil {
		return time.Since(start)
	}
	toBal, _ := toItem.ValueCopy(nil)

	if err := txn.Set(fromKey, bankEncodeUint64(bankDecodeUint64(fromBal)-transferAmount)); err != nil {
		return time.Since(start)
	}
	if err := txn.Set(toKey, bankEncodeUint64(bankDecodeUint64(toBal)+transferAmount)); err != nil {
		return time.Since(start)
	}
	_ = txn.CommitAt(ts, nil)
	return time.Since(start)
}

type workerRng struct{ seed uint64 }

func newWorkerRng() *workerRng {
	return &workerRng{seed: uint64(time.Now().UnixNano())}
}

func (r *workerRng) Intn(n int) int {
	r.seed ^= r.seed << 13
	r.seed ^= r.seed >> 7
	r.seed ^= r.seed << 17
	return int(r.seed>>1) % n
}

// TestDuckDBBankEpochStress sweeps over epoch batch sizes [1, 2, 4, 8, 16, 32]
// and measures bank transfer TPS and p90 latency for each.
func TestDuckDBBankEpochStress(t *testing.T) {
	const (
		oracleDelay = 50 * time.Microsecond
		runDur      = 1 * time.Second
		workers     = 16
	)

	type result struct {
		batchSize int
		tps       float64
		p90       time.Duration
	}

	batchSizes := []int{1, 2, 4, 8, 16, 32}
	var results []result

	for _, bs := range batchSizes {
		inner := divytime.NewOracle(1, oracleDelay)
		bOracle := newEpochBatchOracle(inner, bs)

		t.Logf("  running batchSize=%d ...", bs)
		tps, p90 := runEpochBankWorkload(t, bOracle, runDur, workers)
		results = append(results, result{bs, tps, p90})
	}

	t.Logf("")
	t.Logf("=== DuckDB Epoch Stress Results ===")
	t.Logf("  Oracle simulated delay: %v", oracleDelay)
	t.Logf("  Workers: %d  |  Run duration per batch size: %v", workers, runDur)
	t.Logf("")
	t.Logf("  %-12s  %-14s  %-14s", "BatchSize", "TPS", "p90 Latency")
	t.Logf("  %s", "--------------------------------------------")
	for _, r := range results {
		t.Logf("  %-12d  %-14.0f  %v", r.batchSize, r.tps, r.p90.Round(time.Microsecond))
	}
}

// TestDuckDBBankEpochStressNoDelay runs the same sweep but with zero oracle
// latency to show the pure DuckDB throughput ceiling.
func TestDuckDBBankEpochStressNoDelay(t *testing.T) {
	const (
		runDur  = 1 * time.Second
		workers = 16
	)

	batchSizes := []int{1, 4, 16, 64}
	t.Logf("=== DuckDB Epoch Stress (zero oracle delay) ===")
	t.Logf("  %-12s  %-14s  %-14s", "BatchSize", "TPS", "p90 Latency")
	t.Logf("  %s", "--------------------------------------------")

	for _, bs := range batchSizes {
		inner := divytime.NewOracle(1, 0)
		bOracle := newEpochBatchOracle(inner, bs)
		tps, p90 := runEpochBankWorkload(t, bOracle, runDur, workers)
		t.Logf("  %-12d  %-14.0f  %v", bs, tps, p90.Round(time.Microsecond))
	}
}

func parseDurationEnv(name string, def time.Duration) time.Duration {
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

func parseInt64Env(name string, def int64) int64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func parseIntEnv(name string, def int) int {
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

type saturationRow struct {
	workers                           int
	ops                               float64
	avg, p90                          time.Duration
	runWall                           time.Duration
	goroutinesBefore, goroutinesAfter int
	heapMBBefore, heapMBAfter         float64
	openConns, inUse, idle            int
	waitCount                         int64
	waitDuration                      time.Duration
}

func parseStressBoolEnv(name string, def bool) bool {
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

func sampleSaturationKeyReadTimings(
	t *testing.T,
	db *DB,
	readTs types.CustomTs,
	numCustomers int64,
	samples int,
	includeFullKeys bool,
) {
	t.Helper()
	if db.duckDBStorage == nil || samples <= 0 || numCustomers <= 0 {
		return
	}

	var (
		chkSum, savSum, accSum     time.Duration
		chkErr, savErr, accErr     int
		chkFound, savFound, accFound int
	)

	for i := 0; i < samples; i++ {
		id := int64(i) % numCustomers

		t0 := time.Now()
		vChk, _, errChk := db.duckDBStorage.Read(sbCheckingKey(id), readTs)
		chkSum += time.Since(t0)
		if errChk != nil {
			chkErr++
		} else if len(vChk) > 0 {
			chkFound++
		}

		if includeFullKeys {
			t1 := time.Now()
			vSav, _, errSav := db.duckDBStorage.Read(sbSavingsKey(id), readTs)
			savSum += time.Since(t1)
			if errSav != nil {
				savErr++
			} else if len(vSav) > 0 {
				savFound++
			}

			t2 := time.Now()
			vAcc, _, errAcc := db.duckDBStorage.Read(sbAccountKey(id), readTs)
			accSum += time.Since(t2)
			if errAcc != nil {
				accErr++
			} else if len(vAcc) > 0 {
				accFound++
			}
		}
	}

	t.Logf("  [diag] per-key read timing over %d samples @fixed-ts", samples)
	t.Logf("  [diag] checking_bal avg=%v found=%d/%d err=%d",
		(chkSum / time.Duration(samples)).Round(time.Microsecond), chkFound, samples, chkErr)
	if includeFullKeys {
		t.Logf("  [diag] savings_bal  avg=%v found=%d/%d err=%d",
			(savSum / time.Duration(samples)).Round(time.Microsecond), savFound, samples, savErr)
		t.Logf("  [diag] accounts_id  avg=%v found=%d/%d err=%d",
			(accSum / time.Duration(samples)).Round(time.Microsecond), accFound, samples, accErr)
	}
}

func TestDuckDBSaturationProbe(t *testing.T) {
	cardinality := parseInt64Env("BADGER_DUCKDB_SATURATION_CARDINALITY", 200_000)
	workers := parseIntListEnv("BADGER_DUCKDB_SATURATION_WORKERS",
		[]int{16, 32, 64, 128, 256, 512})
	dur := parseDurationEnv("BADGER_DUCKDB_SATURATION_DURATION", 2*time.Second)
	warmupDur := parseDurationEnv("BADGER_DUCKDB_SATURATION_WARMUP", 0)
	if warmupDur <= 0 && cardinality >= 40_000_000 {
		warmupDur = 2 * time.Second
	}
	phaseDiag := parseStressBoolEnv("BADGER_DUCKDB_SATURATION_PHASE_DIAG", false)
	keyTimingSamples := parseIntEnv("BADGER_DUCKDB_SATURATION_KEYTIMING_SAMPLES", 200)
	readMode := strings.ToLower(strings.TrimSpace(os.Getenv("BADGER_DUCKDB_READ_HEAVY_KEY_MODE")))
	checkingOnly := readMode == "checking-only" || readMode == "checking"

	t.Logf("")
	t.Logf("=== DuckDB Saturation Probe (cardinality=%d) ===", cardinality)
	t.Logf("  measurement duration=%v, warmup duration=%v", dur, warmupDur)
	t.Logf("  %-8s  %-14s  %-16s  %-16s  %-10s  %-10s  %-10s",
		"Workers", "DuckDB Ops/s", "Goroutines b->a", "HeapMB b->a", "OpenConns", "InUse/Idle", "WaitCount")
	t.Logf("  %s", strings.Repeat("-", 100))

	var rows []saturationRow
	var seedElapsed time.Duration

	withDuckDB(t, true, func(db *DB) {
		oracle := divytime.NewOracle(1, 0)
		seedStart := time.Now()
		seedSmallBankN(t, db, oracle, cardinality)
		seedElapsed = time.Since(seedStart)
		if db.duckDBStorage != nil {
			flushStart := time.Now()
			if err := db.duckDBStorage.FlushAllPending(); err != nil {
				t.Fatalf("flush all pending after seed: %v", err)
			}
			if phaseDiag {
				t.Logf("  [diag] post-seed flush elapsed=%v", time.Since(flushStart).Round(time.Millisecond))
			}
		}

		if phaseDiag {
			t.Logf("  [diag] seed phase: elapsed=%v (%.0f customers/sec)",
				seedElapsed.Round(time.Millisecond), float64(cardinality)/seedElapsed.Seconds())
			readTs := sbTs(oracle)
			sampleSaturationKeyReadTimings(t, db, readTs, cardinality, keyTimingSamples, !checkingOnly)
		}

		runtime.GC()
		debug.FreeOSMemory()

		for _, w := range workers {
			if warmupDur > 0 {
				_ = runBalanceReadHeavy(t, "DuckDB-warmup", db, oracle, cardinality, warmupDur, w)
			}

			runtime.GC()
			var msBefore runtime.MemStats
			runtime.ReadMemStats(&msBefore)
			goroutinesBefore := runtime.NumGoroutine()

			runStart := time.Now()
			result := runBalanceReadHeavy(t, "DuckDB", db, oracle, cardinality, dur, w)
			runWall := time.Since(runStart)

			var msAfter runtime.MemStats
			runtime.ReadMemStats(&msAfter)
			goroutinesAfter := runtime.NumGoroutine()

			r := saturationRow{
				workers:          w,
				ops:              result.ops,
				avg:              result.avg,
				p90:              result.p90,
				runWall:          runWall,
				goroutinesBefore: goroutinesBefore,
				goroutinesAfter:  goroutinesAfter,
				heapMBBefore:     float64(msBefore.HeapAlloc) / (1 << 20),
				heapMBAfter:      float64(msAfter.HeapAlloc) / (1 << 20),
			}
			if db.duckDBStorage != nil {
				ps := db.duckDBStorage.PoolStats()
				r.openConns = ps.OpenConnections
				r.inUse = ps.InUse
				r.idle = ps.Idle
				r.waitCount = ps.WaitCount
				r.waitDuration = ps.WaitDuration
			}
			rows = append(rows, r)

			t.Logf("  %-8d  %-14.1f  %-16s  %-16s  %-10d  %d/%-8d  %-10d",
				w, r.ops,
				fmt.Sprintf("%d->%d", r.goroutinesBefore, r.goroutinesAfter),
				fmt.Sprintf("%.1f->%.1f", r.heapMBBefore, r.heapMBAfter),
				r.openConns, r.inUse, r.idle, r.waitCount)
			if phaseDiag {
				t.Logf("    [diag] workers=%d run-phase elapsed=%v", w, r.runWall.Round(time.Millisecond))
			}
		}
	})

	if phaseDiag && len(rows) > 0 {
		var totalRunPhase time.Duration
		for _, r := range rows {
			totalRunPhase += r.runWall
		}
		t.Logf("  [diag] phase totals: seed=%v read-phase-total=%v overall=%v",
			seedElapsed.Round(time.Millisecond),
			totalRunPhase.Round(time.Millisecond),
			(seedElapsed + totalRunPhase).Round(time.Millisecond))
	}

	ceilingAt := -1
	for i := 1; i < len(rows); i++ {
		if rows[i].ops < rows[i-1].ops*1.05 {
			ceilingAt = rows[i-1].workers
			break
		}
	}
	if ceilingAt > 0 {
		t.Logf("  Throughput plateau observed at/after %d workers "+
			"(less than 5%% gain per level beyond this point)", ceilingAt)
	} else if len(rows) > 0 {
		t.Logf("  Throughput was still scaling at the highest tested level (%d workers) -- "+
			"true ceiling not reached; rerun with a higher BADGER_DUCKDB_SATURATION_WORKERS value",
			rows[len(rows)-1].workers)
	}

	sawWait := false
	for _, r := range rows {
		if r.waitCount > 0 {
			sawWait = true
			t.Logf("  NOTE: at %d workers, %d connection-pool waits were observed "+
				"(total wait %v) -- this points at BADGER_DUCKDB_READ_POOL_SIZE as "+
				"the bottleneck, not CPU/hardware", r.workers, r.waitCount, r.waitDuration)
		}
	}
	if !sawWait && len(rows) > 0 {
		t.Logf("  No connection-pool waits observed at any tested worker level -- " +
			"any plateau found above is CPU/scheduler-bound, not pool-exhaustion-bound")
	}

	if outPath := os.Getenv("BADGER_DUCKDB_SATURATION_CSV"); outPath != "" {
		csv := "workers,duckdb_ops_per_sec,avg_ns,p90_ns,goroutines_before,goroutines_after," +
			"heap_mb_before,heap_mb_after,open_conns,in_use,idle,wait_count,wait_duration_ns,run_wall_ns\n"
		for _, r := range rows {
			csv += fmt.Sprintf("%d,%.3f,%d,%d,%d,%d,%.2f,%.2f,%d,%d,%d,%d,%d,%d\n",
				r.workers, r.ops, r.avg.Nanoseconds(), r.p90.Nanoseconds(),
				r.goroutinesBefore, r.goroutinesAfter,
				r.heapMBBefore, r.heapMBAfter,
				r.openConns, r.inUse, r.idle, r.waitCount, r.waitDuration.Nanoseconds(), r.runWall.Nanoseconds())
		}
		if err := os.WriteFile(outPath, []byte(csv), 0644); err != nil {
			t.Fatalf("write saturation csv: %v", err)
		}
		t.Logf("  Wrote saturation CSV: %s", outPath)
	}
}

func TestDuckDBBankSoak(t *testing.T) {
	// Keep defaults bounded so `go test ./...` remains reliable under the
	// package-level timeout. Long CloudLab runs should set env overrides.
	dur := parseDurationEnv("BADGER_DUCKDB_SOAK_DURATION", 90*time.Second)
	checkInterval := parseDurationEnv("BADGER_DUCKDB_SOAK_CHECK_INTERVAL", 10*time.Second)
	workers := parseIntEnv("BADGER_DUCKDB_SOAK_WORKERS", 8)

	t.Logf("")
	t.Logf("=== DuckDB Bank Soak Test ===")
	t.Logf("  duration=%v check_interval=%v workers=%d", dur, checkInterval, workers)

	withDuckDB(t, true, func(db *DB) {
		oracle := divytime.NewOracle(1, 0)
		seedDuckDBAccounts(t, db, oracle)

		var (
			transferOps       atomic.Int64
			invariantChecks   atomic.Int64
			invariantFailures atomic.Int64
			stop              int32
			wg                sync.WaitGroup
		)

		startGoroutines := runtime.NumGoroutine()

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
				for atomic.LoadInt32(&stop) == 0 {
					execTransfer(t, db, oracle, rng)
					transferOps.Add(1)
				}
			}(w)
		}

		runStart := time.Now()
		checkerDone := make(chan struct{})
		go func() {
			defer close(checkerDone)
			ticker := time.NewTicker(checkInterval)
			defer ticker.Stop()
			for range ticker.C {
				invariantChecks.Add(1)
				elapsed := time.Since(runStart).Round(time.Second)
				tsRaw, _ := oracle.GetTimestamp(int64(time.Now().UnixNano()))
				if bankInvariantHoldsAt(t, db, divyToTs(tsRaw)) {
					t.Logf("  [soak] invariant holds at t=%v (ops so far=%d, goroutines=%d)",
						elapsed, transferOps.Load(), runtime.NumGoroutine())
				} else {
					invariantFailures.Add(1)
					t.Logf("  [soak] invariant check FAILED at t=%v (ops so far=%d)",
						elapsed, transferOps.Load())
				}
				if time.Since(runStart) >= dur {
					return
				}
			}
		}()

		time.Sleep(dur)
		atomic.StoreInt32(&stop, 1)
		wg.Wait()
		<-checkerDone

		endGoroutines := runtime.NumGoroutine()

		t.Logf("=== Soak Summary ===")
		t.Logf("  total transfer ops: %d (%.0f TPS avg)",
			transferOps.Load(), float64(transferOps.Load())/dur.Seconds())
		t.Logf("  invariant checks during run: %d, failures: %d",
			invariantChecks.Load(), invariantFailures.Load())
		t.Logf("  goroutines before=%d after=%d (delta=%d)",
			startGoroutines, endGoroutines, endGoroutines-startGoroutines)

		if invariantFailures.Load() > 0 {
			t.Errorf("balance invariant failed %d/%d live checks during the soak run -- "+
				"see the [soak] log lines above for timing", invariantFailures.Load(), invariantChecks.Load())
		}

		if delta := endGoroutines - startGoroutines; delta > workers {
			t.Errorf("goroutine count grew by %d (before=%d after=%d) after all workers stopped -- "+
				"possible goroutine leak", delta, startGoroutines, endGoroutines)
		}

		verifyBankTotal(t, db)
	})
}

func bankInvariantHoldsAt(t *testing.T, db *DB, readTs types.CustomTs) bool {
	t.Helper()
	txn := db.NewTransactionAt(readTs, false)
	defer txn.Discard()
	results, err := db.duckDBStorage.ScanPrefix([]byte(bankKeyPrefix), txn.readTs)
	if err != nil {
		t.Logf("  [soak] scan error during live check: %v", err)
		return false
	}
	var total uint64
	for _, r := range results {
		if r.Found {
			total += bankDecodeUint64(r.Value)
		}
	}
	return total == uint64(numBankAccounts)*initialBankBal
}
