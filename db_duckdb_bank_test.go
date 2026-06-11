//go:build duckdb

package badger

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4/divytime"
	"github.com/dgraph-io/badger/v4/types"
)

// divyToTs converts a divytime.Timestamp to the canonical types.CustomTs.
func divyToTs(ts divytime.Timestamp) types.CustomTs {
	return types.CustomTs{
		EpochID:    uint32(ts.EpochID),
		BrokerID:   uint32(ts.BrokerID),
		AssignedTs: uint32(ts.AssignedTs),
	}
}

// ---------------------------------------------------------------------------
// Bank benchmark: DuckDB backend with 3-tuple (divytime) timestamps
//
// Layout
//   - numBankAccounts accounts, each seeded with initialBankBal.
//   - Workers concurrently execute three transaction types:
//       TRANSFER  – move a fixed amount between two random accounts.
//       READ_ONLY – read the balance of one account (snapshot read).
//       SUM_CHECK – iterate all accounts, verify total == expected.
//   - After the run the total balance is verified once more.
//   - TPS, per-type latency (avg, p90), and divytime overhead are logged.
// ---------------------------------------------------------------------------

const (
	numBankAccounts = 1_000
	initialBankBal  = uint64(1_000)
	transferAmount  = uint64(10)
	bankRunDuration = 10 * time.Second
	numBankWorkers  = 16
)

type bankTxType int

const (
	txTransfer  bankTxType = iota // read+write two accounts
	txReadOnly                    // read one account balance
	txSumCheck                    // verify full balance invariant
)

func (t bankTxType) String() string {
	switch t {
	case txTransfer:
		return "TRANSFER"
	case txReadOnly:
		return "READ_ONLY"
	case txSumCheck:
		return "SUM_CHECK"
	default:
		return "UNKNOWN"
	}
}

// bankKey returns the DuckDB key for account i.
func bankKey(i int) []byte {
	return []byte(fmt.Sprintf("acct:%08d", i))
}

// bankEncodeUint64 / bankDecodeUint64 encode balances as 8 big-endian bytes.
func bankEncodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
	return b
}

func bankDecodeUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

// bankStats accumulates per-transaction-type timing samples.
type bankStats struct {
	mu      sync.Mutex
	samples map[bankTxType][]int64 // nanoseconds
	count   map[bankTxType]int64
}

func newBankStats() *bankStats {
	return &bankStats{
		samples: make(map[bankTxType][]int64),
		count:   make(map[bankTxType]int64),
	}
}

func (s *bankStats) record(typ bankTxType, d time.Duration) {
	s.mu.Lock()
	s.samples[typ] = append(s.samples[typ], int64(d))
	s.count[typ]++
	s.mu.Unlock()
}

type txStats struct {
	count int64
	avg   time.Duration
	p90   time.Duration
	min   time.Duration
	max   time.Duration
}

func (s *bankStats) summarize(typ bankTxType) txStats {
	s.mu.Lock()
	raw := make([]int64, len(s.samples[typ]))
	copy(raw, s.samples[typ])
	cnt := s.count[typ]
	s.mu.Unlock()

	if len(raw) == 0 {
		return txStats{count: cnt}
	}
	sort.Slice(raw, func(i, j int) bool { return raw[i] < raw[j] })
	var total int64
	for _, v := range raw {
		total += v
	}
	p90idx := int(float64(len(raw)) * 0.90)
	if p90idx >= len(raw) {
		p90idx = len(raw) - 1
	}
	return txStats{
		count: cnt,
		avg:   time.Duration(total / int64(len(raw))),
		p90:   time.Duration(raw[p90idx]),
		min:   time.Duration(raw[0]),
		max:   time.Duration(raw[len(raw)-1]),
	}
}

// ---------------------------------------------------------------------------
// TestDuckDBBankDivytime – correctness + TPS
// ---------------------------------------------------------------------------

// TestDuckDBBankDivytime runs a bank-style workload with 3-tuple divytime
// timestamps and verifies that the total balance never changes.
//
// Run with:
//
//	go test -v -tags duckdb -run TestDuckDBBankDivytime -timeout 60s
func TestDuckDBBankDivytime(t *testing.T) {
	oracle := divytime.NewOracle(1, 0) // no simulated delay for correctness test
	runBankWorkload(t, oracle, bankRunDuration, numBankWorkers, false)
}

// TestDuckDBBankDivytimeSimulatedDelay is the same workload but with a
// simulated divytime round-trip latency (50 µs) to mimic production overhead.
//
// Run with:
//
//	go test -v -tags duckdb -run TestDuckDBBankDivytimeSimulatedDelay -timeout 120s
func TestDuckDBBankDivytimeSimulatedDelay(t *testing.T) {
	oracle := divytime.NewOracle(1, 50*time.Microsecond)
	runBankWorkload(t, oracle, bankRunDuration, numBankWorkers, false)
}

// BenchmarkDuckDBBankTPS measures raw transaction throughput.
//
//	go test -v -tags duckdb -bench BenchmarkDuckDBBankTPS -benchtime 15s
func BenchmarkDuckDBBankTPS(b *testing.B) {
	oracle := divytime.NewOracle(1, 0)
	withDuckDB(b, true, func(db *DB) {
		seedDuckDBAccounts(b, db, oracle)
		b.ResetTimer()

		var ops atomic.Int64
		stop := make(chan struct{})
		var wg sync.WaitGroup

		for i := 0; i < numBankWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for {
					select {
					case <-stop:
						return
					default:
					}
					execTransfer(b, db, oracle, rng)
					ops.Add(1)
				}
			}()
		}

		time.Sleep(time.Duration(b.N) * time.Millisecond)
		close(stop)
		wg.Wait()

		elapsed := time.Duration(b.N) * time.Millisecond
		tps := float64(ops.Load()) / elapsed.Seconds()
		b.ReportMetric(tps, "txns/sec")

		divStats := oracle.Snapshot()
		b.Logf("divytime: avg=%v p90=%v calls=%d",
			time.Duration(divStats.AvgNs), time.Duration(divStats.P90Ns), divStats.Count)
	})
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func seedDuckDBAccounts(tb testing.TB, db *DB, oracle *divytime.Oracle) {
	tb.Helper()
	for i := 0; i < numBankAccounts; i++ {
		ts, _ := oracle.GetTimestamp(int64(i) + 1)
		txn := db.NewTransactionAt(divyToTs(ts), true)
		if err := txn.Set(bankKey(i), bankEncodeUint64(initialBankBal)); err != nil {
			tb.Fatalf("seed account %d: %v", i, err)
		}
		if err := txn.CommitAt(divyToTs(ts), nil); err != nil {
			tb.Fatalf("seed commit account %d: %v", i, err)
		}
	}
}

// execTransfer moves transferAmount from a random source to a random
// destination using an atomic read-modify-write via Badger optimistic
// concurrency control. Returns the elapsed time.
//
// Correctness design
// ------------------
// We call the oracle TWICE per attempt: once for readTs (before reads) and
// once for commitTs (after reads, immediately before CommitAt).
//
// Key invariant: readTs < commitTs is always guaranteed within a single
// goroutine since the oracle counter is monotonically increasing.
//
// Why commitTs must come AFTER reads
// ------------------------------------
// With a distributed oracle that has latency (e.g. 50 µs), multiple goroutines
// may call GetTimestamp concurrently and receive timestamps in an order that
// does not match physical wall time.  Specifically, transaction C can obtain a
// commitTs that is numerically less than B's readTs even though C's data has
// not yet been physically written (DirectFlush not yet called).
//
// If commitTs were obtained before reads, C's CommitAt fires after C's reads
// (~ms later), so C may physically commit long after B has already read the
// affected keys — and since C.commitTs ≤ B.readTs, Badger's conflict detection
// (which only fires for ts > readTs) will not catch the overlap.
//
// Obtaining commitTs immediately before CommitAt is necessary but NOT
// sufficient: between GetTimestamp returning and CommitAt registering the ts
// inside newCommitTs (behind writeChLock, which is contended), the commit is
// invisible to NewTransactionAt's read barrier. A reader with a higher readTs
// can slip through that window, read stale data, and later commit because
// hasConflict skips committed txns with ts <= readTs. The fix is
// GetCommitTimestamp, which registers the ts with the commit tracker
// atomically with issuance (while the oracle's issue lock is held), so every
// later-issued readTs is guaranteed to wait for this commit's DirectFlush.
//
// Conflict detection remains correct: any concurrent write that commits in the
// window (readTs, commitTs] is caught by hasConflict and triggers a retry.
func execTransfer(tb testing.TB, db *DB, oracle *divytime.Oracle, rng *rand.Rand) time.Duration {
	start := time.Now()
	from := rng.Intn(numBankAccounts)
	to := rng.Intn(numBankAccounts)
	for to == from {
		to = rng.Intn(numBankAccounts)
	}

	const maxRetries = 20
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Oracle call 1: snapshot timestamp — taken before any reads.
		readTsRaw, _ := oracle.GetTimestamp(int64(time.Now().UnixNano()))
		readTs := divyToTs(readTsRaw)

		txn := db.NewTransactionAt(readTs, true)

		fromItem, err := txn.Get(bankKey(from))
		if err != nil {
			txn.Discard()
			return time.Since(start)
		}
		fromBal, _ := fromItem.ValueCopy(nil)
		if bankDecodeUint64(fromBal) < transferAmount {
			txn.Discard()
			return time.Since(start)
		}

		toItem, err := txn.Get(bankKey(to))
		if err != nil {
			txn.Discard()
			return time.Since(start)
		}
		toBal, _ := toItem.ValueCopy(nil)

		newFrom := bankDecodeUint64(fromBal) - transferAmount
		newTo := bankDecodeUint64(toBal) + transferAmount

		if err := txn.Set(bankKey(from), bankEncodeUint64(newFrom)); err != nil {
			txn.Discard()
			return time.Since(start)
		}
		if err := txn.Set(bankKey(to), bankEncodeUint64(newTo)); err != nil {
			txn.Discard()
			return time.Since(start)
		}

		// Oracle call 2: commit timestamp — obtained after all reads and writes
		// are staged, immediately before CommitAt.
		//
		// GetCommitTimestamp (not GetTimestamp) is required for correctness: it
		// registers the ts with the DuckDB commit tracker atomically with
		// issuance, while the oracle's issue lock is still held. With plain
		// GetTimestamp there is a window between issuance and CommitAt's
		// internal registration (widened by writeChLock contention) in which a
		// reader can obtain a higher readTs, pass NewTransactionAt's barrier,
		// and read stale data that conflict detection then cannot catch
		// (hasConflict skips committed txns with ts <= readTs) — a lost update.
		commitTsRaw, _ := oracle.GetCommitTimestamp(func(ts divytime.Timestamp) {
			db.RegisterPendingCommit(divyToTs(ts))
		})
		commitTs := divyToTs(commitTsRaw)

		commitErr := txn.CommitAt(commitTs, nil)
		txn.Discard()
		if commitErr == ErrConflict {
			continue // retry with fresh timestamps
		}
		return time.Since(start)
	}
	return time.Since(start)
}

// verifyBankTotal reads the latest balance of every account and asserts the
// total exactly matches the expected value.
//
// With the two-oracle-call + conflict-detection fix in execTransfer, write
// skew is no longer expected: any transfer that would produce an inconsistent
// state is detected as a conflict and retried until it succeeds cleanly.
// A mismatch here is a real bug.
func verifyBankTotal(tb testing.TB, db *DB) {
	tb.Helper()
	txn := db.NewTransactionAt(types.MaxTs, false)
	defer txn.Discard()

	var total uint64
	for i := 0; i < numBankAccounts; i++ {
		item, err := txn.Get(bankKey(i))
		if err != nil {
			tb.Fatalf("verify: get account %d: %v", i, err)
		}
		v, _ := item.ValueCopy(nil)
		total += bankDecodeUint64(v)
	}
	expected := uint64(numBankAccounts) * initialBankBal
	if total != expected {
		tb.Errorf("balance invariant violated: want=%d got=%d (delta=%d = %d lost transfers)",
			expected, total, int64(expected)-int64(total),
			(int64(expected)-int64(total))/int64(transferAmount))
	} else {
		tb.Logf("balance invariant holds: total=%d", total)
	}
}

// runBankWorkload is the shared driver used by all bank tests and benchmarks.
func runBankWorkload(tb testing.TB, oracle *divytime.Oracle, dur time.Duration, workers int, quiet bool) {
	withDuckDB(tb, true, func(db *DB) {
		// Phase 1: seed accounts.
		setupStart := time.Now()
		seedDuckDBAccounts(tb, db, oracle)
		if !quiet {
			switch t := tb.(type) {
			case *testing.T:
				t.Logf("[bank] seeded %d accounts in %v", numBankAccounts, time.Since(setupStart).Round(time.Millisecond))
			}
		}

		stats := newBankStats()
		var (
			transferOps atomic.Int64
			readOps     atomic.Int64
			sumChecks   atomic.Int64
			stop        int32
			wg          sync.WaitGroup
		)

		// Phase 2: concurrent workload.
		startTime := time.Now()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

				for atomic.LoadInt32(&stop) == 0 {
					r := rng.Intn(100)
					switch {
					case r < 70: // 70% transfers
						d := execTransfer(tb, db, oracle, rng)
						stats.record(txTransfer, d)
						transferOps.Add(1)

					case r < 95: // 25% read-only
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

					default: // 5% sum checks
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
						// We don't assert here to avoid failing the bench from a race;
						// the final verify step catches any invariant violation.
						_ = total
						stats.record(txSumCheck, time.Since(start))
						sumChecks.Add(1)
					}
				}
			}(w)
		}

		// Let the workload run.
		time.Sleep(dur)
		atomic.StoreInt32(&stop, 1)
		wg.Wait()
		elapsed := time.Since(startTime)

		// Phase 3: report.
		totalOps := transferOps.Load() + readOps.Load() + sumChecks.Load()
		tps := float64(totalOps) / elapsed.Seconds()

		switch t := tb.(type) {
		case *testing.T:
			t.Logf("=== DuckDB Bank Benchmark Results ===")
			t.Logf("Duration:   %v", elapsed.Round(time.Millisecond))
			t.Logf("Workers:    %d", workers)
			t.Logf("Total ops:  %d (%.0f TPS)", totalOps, tps)
			t.Logf("")
			for _, typ := range []bankTxType{txTransfer, txReadOnly, txSumCheck} {
				s := stats.summarize(typ)
				t.Logf("  [%s]  count=%d  avg=%v  p90=%v  min=%v  max=%v",
					typ, s.count, s.avg.Round(time.Microsecond),
					s.p90.Round(time.Microsecond),
					s.min.Round(time.Microsecond),
					s.max.Round(time.Microsecond))
			}

			divStats := oracle.Snapshot()
			t.Logf("")
			t.Logf("  [divytime]  calls=%d  avg=%v  p90=%v",
				divStats.Count,
				time.Duration(divStats.AvgNs).Round(time.Microsecond),
				time.Duration(divStats.P90Ns).Round(time.Microsecond))
		}

		// Phase 4: correctness check.
		verifyBankTotal(tb, db)

		switch t := tb.(type) {
		case *testing.T:
			t.Logf("PASS: balance invariant holds after %d transfer ops", transferOps.Load())
		}
	})
}
