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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4/divytime"
	"github.com/dgraph-io/badger/v4/types"
	"math/rand"
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
