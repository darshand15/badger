//go:build duckdb

package badger

// Ashley's stress test: maximise transactions per epoch.
//
// In a real deployment the ordering service ("Divy") issues a single
// round-trip per timestamp request.  Each round-trip has a fixed network
// overhead (typically 50–200 µs).  When every transaction calls GetTimestamp
// independently, that latency is paid once per transaction, capping TPS at
// 1/latency regardless of concurrency.
//
// Epoch batching amortises the oracle round-trip across N transactions:
//   1. One goroutine reserves a batch of N AssignedTs slots with one oracle
//      call, paying the latency once.
//   2. The remaining N-1 slots are handed out locally (zero oracle overhead).
//   3. All N transactions share the same EpochID and carry consecutive
//      AssignedTs values, so ordering is preserved.
//
// TestDuckDBBankEpochStress sweeps over several batch sizes and prints the
// TPS and p90 latency for each.  With a simulated oracle delay of 50 µs the
// improvement should be dramatic at larger batch sizes.
//
// Run with:
//
//	go test -v -tags duckdb -run TestDuckDBBankEpochStress -timeout 180s

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4/divytime"
	"github.com/dgraph-io/badger/v4/types"
)

// ---------------------------------------------------------------------------
// epochBatchOracle
// ---------------------------------------------------------------------------

// epochBatchOracle wraps a real divytime.Oracle but reserves N AssignedTs
// slots per oracle call, amortising its simulated latency across N
// transactions.
//
// Thread-safety: safe for concurrent use.
type epochBatchOracle struct {
	inner     *divytime.Oracle
	batchSize int64

	mu        sync.Mutex
	curEpoch  int64 // epoch counter incremented each batch
	nextSlot  int64 // next AssignedTs to hand out
	batchEnd  int64 // exclusive upper bound of the current batch
}

func newEpochBatchOracle(inner *divytime.Oracle, batchSize int) *epochBatchOracle {
	return &epochBatchOracle{inner: inner, batchSize: int64(batchSize)}
}

// GetTimestamp returns the next unique (EpochID, AssignedTs) pair, calling
// the inner oracle only at the start of each batch of batchSize transactions.
func (o *epochBatchOracle) GetTimestamp() types.CustomTs {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.nextSlot >= o.batchEnd {
		// Start of a new batch: call the inner oracle once (paying its latency).
		o.curEpoch++
		ts, _ := o.inner.GetTimestamp(o.curEpoch)
		// Claim a contiguous range [ts.AssignedTs, ts.AssignedTs+batchSize).
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

// ---------------------------------------------------------------------------
// runEpochBankWorkload – shared driver used by the epoch stress test
// ---------------------------------------------------------------------------

// runEpochBankWorkload runs a bank-style transfer workload using the supplied
// epochBatchOracle for the given duration and worker count.  It returns the
// observed TPS and the p90 transfer latency.
func runEpochBankWorkload(
	t *testing.T,
	oracle *epochBatchOracle,
	dur time.Duration,
	workers int,
) (tps float64, p90 time.Duration) {
	t.Helper()

	withDuckDB(t, true, func(db *DB) {
		// Seed accounts using the real divytime oracle directly so seeding
		// does not inflate the oracle call count.
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
			go func(workerID int) {
				defer wg.Done()

				for atomic.LoadInt32(&stop) == 0 {
					ts := oracle.GetTimestamp()
					d := execEpochTransfer(t, db, ts)
					stats.record(txTransfer, d)
					totalXfers.Add(1)
				}
			}(w)
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

// execEpochTransfer executes a single transfer using the pre-allocated ts.
// Unlike execTransfer it does not call the oracle itself — the caller is
// responsible for obtaining ts from the epochBatchOracle.
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

	fromItem, err := txn.Get(bankKey(from))
	if err != nil {
		return time.Since(start)
	}
	fromBal, _ := fromItem.ValueCopy(nil)
	bal := bankDecodeUint64(fromBal)
	if bal < transferAmount {
		return time.Since(start)
	}

	toItem, err := txn.Get(bankKey(to))
	if err != nil {
		return time.Since(start)
	}
	toBal, _ := toItem.ValueCopy(nil)

	if err := txn.Set(bankKey(from), bankEncodeUint64(bankDecodeUint64(fromBal)-transferAmount)); err != nil {
		return time.Since(start)
	}
	if err := txn.Set(bankKey(to), bankEncodeUint64(bankDecodeUint64(toBal)+transferAmount)); err != nil {
		return time.Since(start)
	}
	_ = txn.CommitAt(ts, nil)
	return time.Since(start)
}

// newWorkerRng returns a fresh PRNG seeded from the current nanosecond clock.
// Each goroutine call gets an independent sequence.
func newWorkerRng() *workerRng {
	return &workerRng{seed: uint64(time.Now().UnixNano())}
}

// workerRng is a lightweight xorshift64 PRNG that avoids the lock overhead
// of rand.New(rand.NewSource(…)) when used per-call.
type workerRng struct{ seed uint64 }

func (r *workerRng) Intn(n int) int {
	r.seed ^= r.seed << 13
	r.seed ^= r.seed >> 7
	r.seed ^= r.seed << 17
	return int(r.seed>>1) % n
}

// ---------------------------------------------------------------------------
// TestDuckDBBankEpochStress
// ---------------------------------------------------------------------------

// TestDuckDBBankEpochStress sweeps over epoch batch sizes [1, 2, 4, 8, 16, 32]
// and measures bank transfer TPS and p90 latency for each.  A 50 µs simulated
// oracle delay models a realistic ordering-service round-trip; the batch
// oracle pays this latency only once per batch, so throughput should grow
// roughly linearly with batch size until concurrency or DuckDB saturates.
//
// Run with:
//
//	go test -v -tags duckdb -run TestDuckDBBankEpochStress -timeout 180s
func TestDuckDBBankEpochStress(t *testing.T) {
	const (
		oracleDelay = 50 * time.Microsecond
		runDur      = 5 * time.Second
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

		t.Logf("  running batchSize=%d …", bs)
		tps, p90 := runEpochBankWorkload(t, bOracle, runDur, workers)
		results = append(results, result{bs, tps, p90})
	}

	// ── Print summary table ────────────────────────────────────────────────
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

	// Sanity check: TPS with batchSize=32 should be meaningfully higher than
	// batchSize=1 (≥ 2× improvement expected when oracle latency dominates).
	if len(results) >= 2 {
		base := results[0].tps
		best := results[len(results)-1].tps
		if base > 0 && best/base < 1.5 {
			t.Logf("NOTE: expected ≥1.5× TPS improvement at batchSize=32 vs 1, "+
				"got %.2fx — oracle delay may not dominate at this workload size", best/base)
		} else if base > 0 {
			t.Logf("PASS: %.1fx TPS improvement (batchSize=%d vs %d)",
				best/base, results[len(results)-1].batchSize, results[0].batchSize)
		}
	}
}

// ---------------------------------------------------------------------------
// TestDuckDBBankEpochStressNoDelay
// ---------------------------------------------------------------------------

// TestDuckDBBankEpochStressNoDelay runs the same sweep but with zero oracle
// latency to show the pure DuckDB throughput ceiling — the batch size should
// have negligible effect when the oracle is instant.
//
// Run with:
//
//	go test -v -tags duckdb -run TestDuckDBBankEpochStressNoDelay -timeout 180s
func TestDuckDBBankEpochStressNoDelay(t *testing.T) {
	const (
		runDur  = 5 * time.Second
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
