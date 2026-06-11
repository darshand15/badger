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

const stressDuration = 15 * time.Second

// stressResult holds the numbers we care about for one configuration.
type stressResult struct {
	label        string
	workers      int
	delay        time.Duration
	totalOps     int64
	transferOps  int64
	tps          float64
	transferTPS  float64
	transferAvg  time.Duration
	transferP90  time.Duration
	sumAvg       time.Duration
	invariantOK  bool
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
						ts, _ := oracle.GetTimestamp(int64(time.Now().UnixNano()))
						txn := db.NewTransactionAt(divyToTs(ts), false)
						var total uint64
						for i := 0; i < numBankAccounts; i++ {
							item, gerr := txn.Get(bankKey(i))
							if gerr != nil {
								continue
							}
							v, _ := item.ValueCopy(nil)
							total += bankDecodeUint64(v)
						}
						txn.Discard()
						_ = total
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
	workerCounts := []int{4, 8, 16, 32, 64}

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
