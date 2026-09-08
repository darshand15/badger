//go:build duckdb

package badger

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4/divytime"
	"github.com/dgraph-io/badger/v4/types"
)

// withDuckDB opens a DuckDB-backed managed DB for the duration of fn.
// Works for both *testing.T and *testing.B.
func withDuckDB(tb testing.TB, managed bool, fn func(db *DB)) {
	tb.Helper()

	partitionFanOut := 8
	if raw := strings.TrimSpace(os.Getenv("BADGER_DUCKDB_PARTITION_FANOUT")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			partitionFanOut = n
		}
	}

	opts := DefaultOptions(tb.TempDir())
	opts.UseDuckDB = true
	opts.PartitionFanOut = partitionFanOut
	opts.NumCompactors = 0
	opts.CompactL0OnClose = false
	opts.Logger = nil

	var (
		db  *DB
		err error
	)
	if managed {
		db, err = OpenManaged(opts)
	} else {
		db, err = Open(opts)
	}
	if err != nil {
		tb.Fatalf("open DuckDB: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	fn(db)
}

// divyToTs converts a divytime.Timestamp to the canonical types.CustomTs.
func divyToTs(ts divytime.Timestamp) types.CustomTs {
	return types.CustomTs{
		EpochID:    0,
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
	bankRunDuration = 2 * time.Second
	numBankWorkers  = 16
)

type bankTxType int

const (
	txTransfer bankTxType = iota // read+write two accounts
	txReadOnly                   // read one account balance
	txSumCheck                   // verify full balance invariant
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

// bankKeyPrefix is the common prefix of all account keys.
//
// IMPORTANT: it deliberately contains no ':'. The partition calculator
// co-locates keys by hashing only the bytes before the first ':' — with the
// previous "acct:%08d" naming, every account hashed to the SAME partition, so
// PartitionFanOut=8 provided zero write parallelism and all reads/flushes
// contended on one partition RWMutex. Hashing the full key spreads accounts
// across all partitions. Cross-partition snapshot consistency is guaranteed by
// the NewTransactionAt read barrier (duckDBTracker).
const bankKeyPrefix = "acct-"

var (
	bankKeysOnce sync.Once
	bankKeys     [][]byte
)

func initBankKeys() {
	bankKeys = make([][]byte, numBankAccounts)
	prefixLen := len(bankKeyPrefix)
	for i := 0; i < numBankAccounts; i++ {
		buf := make([]byte, prefixLen+8)
		copy(buf, bankKeyPrefix)
		n := i
		for p := len(buf) - 1; p >= prefixLen; p-- {
			buf[p] = byte('0' + (n % 10))
			n /= 10
		}
		bankKeys[i] = buf
	}
}

// bankKey returns the DuckDB key for account i.
func bankKey(i int) []byte {
	if i >= 0 && i < numBankAccounts {
		bankKeysOnce.Do(initBankKeys)
		return bankKeys[i]
	}
	return []byte(fmt.Sprintf(bankKeyPrefix+"%08d", i))
}

// execSumCheck reads every account balance at a fresh snapshot with a single
// ScanPrefix call (one SQL query per partition) instead of numBankAccounts
// point reads, and returns the total. CPU profiling showed the old per-key
// loop (1,000 prepared SELECTs through database/sql + CGo per check) consumed
// ~78% of all CPU in the stress test.
func execSumCheck(db *DB, oracle *divytime.Oracle) (uint64, error) {
	ts, _ := oracle.GetTimestamp(int64(time.Now().UnixNano()))
	txn := db.NewTransactionAt(divyToTs(ts), false) // passes the read barrier
	defer txn.Discard()

	results, err := db.duckDBStorage.ScanPrefix([]byte(bankKeyPrefix), txn.readTs)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, r := range results {
		if r.Found {
			total += bankDecodeUint64(r.Value)
		}
	}
	return total, nil
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
	p99   time.Duration
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
	p99idx := int(float64(len(raw)) * 0.99)
	if p99idx >= len(raw) {
		p99idx = len(raw) - 1
	}
	return txStats{
		count: cnt,
		avg:   time.Duration(total / int64(len(raw))),
		p90:   time.Duration(raw[p90idx]),
		p99:   time.Duration(raw[p99idx]),
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
		// nil recorder: this benchmark measures raw TPS, and per-read
		// recording allocations would perturb the number it reports.
		seedDuckDBAccountsRec(b, db, oracle, nil)
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
					execTransferRec(b, db, oracle, rng, nil)
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

// BenchmarkLockFreeIngest_DuckDB mirrors BenchmarkLockFreeIngest but routes
// writes through the DuckDB backend so the two can be compared with benchstat.
func BenchmarkLockFreeIngest_DuckDB(b *testing.B) {
	withDuckDB(b, true, func(db *DB) {
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			id := rand.Int()
			for pb.Next() {
				k := []byte{byte(id), byte(time.Now().Nanosecond())}
				ts := types.CustomTs{AssignedTs: uint32(time.Now().UnixNano())}

				txn := db.NewTransactionAt(ts, true)
				_ = txn.Set(k, []byte("v"))
				_ = txn.CommitAt(ts, nil)
			}
		})
	})
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// seedDuckDBAccounts keeps the original three-argument shape so it can still be
// passed as a function value to runBankOnBackend in the comparison tests.
func seedDuckDBAccounts(tb testing.TB, db *DB, oracle *divytime.Oracle) {
	tb.Helper()
	seedDuckDBAccountsRec(tb, db, oracle, nil)
}

// seedDuckDBAccountsRec writes the initial balance for every account.
//
// When rec is non-nil each seed write is recorded as a committed, read-free
// transaction. The initial state MUST be part of the recorded history: without
// it the sequential replay starts from an empty map and every subsequent read
// is reported as a found-mismatch against nothing.
//
// Note these seed transactions deliberately reuse one timestamp for both readTs
// and commitTs. That is fine here because they read nothing — Verify only
// requires commitTs > readTs for transactions that actually took a snapshot.
func seedDuckDBAccountsRec(tb testing.TB, db *DB, oracle *divytime.Oracle, rec *RFRecorder) {
	tb.Helper()
	for i := 0; i < numBankAccounts; i++ {
		ts, _ := oracle.GetTimestamp(int64(i) + 1)
		cts := divyToTs(ts)
		txn := db.NewTransactionAt(cts, true)
		val := bankEncodeUint64(initialBankBal)
		if err := txn.Set(bankKey(i), val); err != nil {
			tb.Fatalf("seed account %d: %v", i, err)
		}
		if err := txn.CommitAt(cts, nil); err != nil {
			tb.Fatalf("seed commit account %d: %v", i, err)
		}
		if rec != nil {
			rec.Add(&RFTxn{
				ID:        rec.NextID(),
				Label:     "SEED",
				CommitTs:  cts,
				Committed: true,
				Writes:    []RFWrite{{Key: bankKey(i), Value: val}},
			})
		}
	}
}

// Transfer correctness design (applies to execTransferRec below)
// ==============================================================
// execTransferRec moves transferAmount from a random source to a random
// destination using an atomic read-modify-write via Badger optimistic
// concurrency control.
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
//
// IMPORTANT: this reasoning holds only because the bank tests open with
// DefaultOptions (DetectConflicts=true). The deployed server opens with
// WithDetectConflicts(false), where newCommitTs returns before hasConflict is
// reached and none of the above applies. See abort_stats.go.

// transferOutcome classifies how a transfer attempt ended. Before this, every
// return path out of execTransfer looked identical to the caller — a successful
// transfer, a transfer skipped for insufficient funds, and a transfer abandoned
// because a read failed were all just "one transfer op". That made the op count
// an upper bound on work actually done, and made read failures invisible.
type transferOutcome int

const (
	transferCommitted transferOutcome = iota
	transferInsufficientFunds
	transferReadFailed
	transferPrefetchFailed
	transferSetFailed
	transferCommitFailed
	transferRetriesExhausted
)

func (o transferOutcome) String() string {
	switch o {
	case transferCommitted:
		return "committed"
	case transferInsufficientFunds:
		return "insufficient-funds"
	case transferReadFailed:
		return "read-failed"
	case transferPrefetchFailed:
		return "prefetch-failed"
	case transferSetFailed:
		return "set-failed"
	case transferCommitFailed:
		return "commit-failed"
	case transferRetriesExhausted:
		return "retries-exhausted"
	default:
		return "unknown"
	}
}

// execTransfer keeps the original signature (duration only) so it can still be
// used as a function value by the comparison and stress tests.
func execTransfer(tb testing.TB, db *DB, oracle *divytime.Oracle, rng *rand.Rand) time.Duration {
	d, _ := execTransferRec(tb, db, oracle, rng, nil)
	return d
}

// execTransferRec moves transferAmount between two random accounts and reports
// how the attempt ended.
//
// rec may be nil. When non-nil, EVERY attempt is recorded — including attempts
// that aborted on conflict — because the reads-from checker needs to exclude
// aborted attempts from the reference execution rather than never learn they
// happened.
func execTransferRec(tb testing.TB, db *DB, oracle *divytime.Oracle, rng *rand.Rand, rec *RFRecorder) (time.Duration, transferOutcome) {
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

		// Vectorize: batch both account reads into one SQL round-trip instead
		// of two sequential Get() calls, each paying its own CGo boundary
		// cost. No-op on the regular Badger backend (PrefetchKeys only acts
		// when the DuckDB backend is active), so this is safe either way.
		fromKey, toKey := bankKey(from), bankKey(to)
		if err := txn.PrefetchKeys([][]byte{fromKey, toKey}); err != nil {
			txn.Discard()
			return time.Since(start), transferPrefetchFailed
		}

		fromItem, err := txn.Get(fromKey)
		if err != nil {
			txn.Discard()
			return time.Since(start), transferReadFailed
		}
		fromBal, _ := fromItem.ValueCopy(nil)
		// Capture the reads-from edge: item.Version() is the commit timestamp
		// of the transaction whose write this read landed on.
		reads := []RFRead{{
			Key: fromKey, Found: true, Value: fromBal,
			ObservedVersion: fromItem.Version(),
		}}

		if bankDecodeUint64(fromBal) < transferAmount {
			txn.Discard()
			// Read-only outcome, but the read still happened and must be
			// checked — a stale read here is just as much a violation.
			if rec != nil {
				rec.Add(&RFTxn{
					ID: rec.NextID(), Label: "TRANSFER-SKIPPED",
					ReadTs: readTs, Committed: true, Reads: reads,
				})
			}
			return time.Since(start), transferInsufficientFunds
		}

		toItem, err := txn.Get(toKey)
		if err != nil {
			txn.Discard()
			return time.Since(start), transferReadFailed
		}
		toBal, _ := toItem.ValueCopy(nil)
		reads = append(reads, RFRead{
			Key: toKey, Found: true, Value: toBal,
			ObservedVersion: toItem.Version(),
		})

		newFrom := bankDecodeUint64(fromBal) - transferAmount
		newTo := bankDecodeUint64(toBal) + transferAmount

		fromVal, toVal := bankEncodeUint64(newFrom), bankEncodeUint64(newTo)
		if err := txn.Set(bankKey(from), fromVal); err != nil {
			txn.Discard()
			return time.Since(start), transferSetFailed
		}
		if err := txn.Set(bankKey(to), toVal); err != nil {
			txn.Discard()
			return time.Since(start), transferSetFailed
		}
		writes := []RFWrite{
			{Key: fromKey, Value: fromVal},
			{Key: toKey, Value: toVal},
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

		if rec != nil {
			rec.Add(&RFTxn{
				ID:        rec.NextID(),
				Label:     "TRANSFER",
				ReadTs:    readTs,
				CommitTs:  commitTs,
				Committed: commitErr == nil,
				Reads:     reads,
				Writes:    writes,
			})
		}

		if commitErr == ErrConflict {
			continue // retry with fresh timestamps
		}
		if commitErr != nil {
			return time.Since(start), transferCommitFailed
		}
		return time.Since(start), transferCommitted
	}
	return time.Since(start), transferRetriesExhausted
}

// verifyBankTotal reads the latest balance of every account and asserts the
// total exactly matches the expected value.
//
// With the two-oracle-call + conflict-detection fix in execTransfer, write
// skew is no longer expected: any transfer that would produce an inconsistent
// state is detected as a conflict and retried until it succeeds cleanly.
// A mismatch here is a real bug.
//
// Vectorized like execSumCheck: one ScanPrefix call (one SQL query per
// partition) instead of numBankAccounts sequential point Get()s. Each point
// Get crosses the CGo boundary into DuckDB once; profiling showed that
// per-key round-trip tax at ~57% of CPU in this loop. Fetching the whole
// key range per partition in a single call amortizes that tax across all
// accounts in the partition instead of paying it per key.
func verifyBankTotal(tb testing.TB, db *DB) {
	tb.Helper()
	txn := db.NewTransactionAt(types.MaxTs, false)
	defer txn.Discard()

	results, err := db.duckDBStorage.ScanPrefix([]byte(bankKeyPrefix), txn.readTs)
	if err != nil {
		tb.Fatalf("verify: scan accounts: %v", err)
	}
	var total uint64
	for _, r := range results {
		if r.Found {
			total += bankDecodeUint64(r.Value)
		}
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

// rfRecorderCap bounds the recorded history. At 1,000 accounts and a few
// seconds of 16-worker traffic this is comfortably above the real transaction
// count, so runs are verified end-to-end rather than on a prefix. It exists so
// that pointing this driver at a longer run degrades to INCONCLUSIVE instead of
// exhausting memory. Override with BADGER_RF_MAX_TXNS.
const rfRecorderCap = 2_000_000

// newBankRecorder returns a recorder unless reads-from checking is disabled.
// Recording is on by default for the correctness tests (they are short) and can
// be turned off with BADGER_RF_CHECK=0 when measuring throughput, since the
// per-read allocations do perturb TPS.
func newBankRecorder() *RFRecorder {
	if os.Getenv("BADGER_RF_CHECK") == "0" {
		return nil
	}
	limit := rfRecorderCap
	if raw := strings.TrimSpace(os.Getenv("BADGER_RF_MAX_TXNS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			limit = n
		}
	}
	return NewRFRecorder(limit)
}

// runBankWorkload is the shared driver used by all bank tests and benchmarks.
func runBankWorkload(tb testing.TB, oracle *divytime.Oracle, dur time.Duration, workers int, quiet bool) {
	ResetAbortStats()
	rec := newBankRecorder()

	withDuckDB(tb, true, func(db *DB) {
		// Phase 1: seed accounts.
		setupStart := time.Now()
		seedDuckDBAccountsRec(tb, db, oracle, rec)
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

			// Outcome breakdown for transfers. Without this, "N transfer ops"
			// silently mixes committed transfers with attempts that returned
			// early on a failed read or insufficient funds.
			transferOutcomes [transferRetriesExhausted + 1]atomic.Int64

			// Read-only failures were previously swallowed by `if err == nil`.
			// In the deployed server this same shape (a Get that finds no
			// visible version) is the dominant transaction failure, so it must
			// be counted here too.
			readOnlyNotFound atomic.Int64
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
						d, outcome := execTransferRec(tb, db, oracle, rng, rec)
						stats.record(txTransfer, d)
						transferOps.Add(1)
						transferOutcomes[outcome].Add(1)

					case r < 95: // 25% read-only
						start := time.Now()
						ts, _ := oracle.GetTimestamp(int64(time.Now().UnixNano()))
						readTs := divyToTs(ts)
						txn := db.NewTransactionAt(readTs, false)
						acc := rng.Intn(numBankAccounts)
						key := bankKey(acc)
						item, err := txn.Get(key)
						if err == nil {
							val, _ := item.ValueCopy(nil)
							if rec != nil {
								rec.Add(&RFTxn{
									ID: rec.NextID(), Label: "READ_ONLY",
									ReadTs: readTs, Committed: true,
									Reads: []RFRead{{
										Key: key, Found: true, Value: val,
										ObservedVersion: item.Version(),
									}},
								})
							}
						} else {
							readOnlyNotFound.Add(1)
							// A not-found on a seeded account is itself an
							// anomaly; record it so the replay can say whether
							// the key should have been visible at this readTs.
							if rec != nil {
								rec.Add(&RFTxn{
									ID: rec.NextID(), Label: "READ_ONLY-NOTFOUND",
									ReadTs: readTs, Committed: true,
									Reads: []RFRead{{Key: key, Found: false}},
								})
							}
						}
						txn.Discard()
						stats.record(txReadOnly, time.Since(start))
						readOps.Add(1)

					default: // 5% sum checks
						start := time.Now()
						// We don't assert here to avoid failing the bench from a race;
						// the final verify step catches any invariant violation.
						_, _ = execSumCheck(db, oracle)
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

			// Transfer outcome breakdown: distinguishes work done from work
			// attempted. transferOps alone cannot.
			t.Logf("")
			t.Logf("  transfer outcomes:")
			for o := transferCommitted; o <= transferRetriesExhausted; o++ {
				t.Logf("    %-20s %d", o.String(), transferOutcomes[o].Load())
			}
			t.Logf("    %-20s %d", "read-only-not-found", readOnlyNotFound.Load())

			// Abort breakdown from the storage layer itself.
			t.Logf("")
			t.Logf("%s", SnapshotAbortStats().Report())
		}

		// Phase 4a: the weak invariant (sum of balances).
		verifyBankTotal(tb, db)

		// Phase 4b: the strong invariant — reads-from equivalence to the
		// commit-timestamp-ordered sequential execution. This subsumes 4a: a
		// lost update that happens to preserve the total passes 4a and fails
		// here on the read-version comparison.
		if rec != nil {
			verifyReadsFrom(tb, db, rec)
		}

		switch t := tb.(type) {
		case *testing.T:
			t.Logf("PASS: balance invariant holds after %d transfer ops", transferOps.Load())
		}
	})
}

// verifyReadsFrom snapshots the final database state and runs the reads-from
// equivalence check against the recorded history.
func verifyReadsFrom(tb testing.TB, db *DB, rec *RFRecorder) {
	tb.Helper()

	// Final state via one ScanPrefix per partition at MaxTs, matching how
	// verifyBankTotal reads it.
	txn := db.NewTransactionAt(types.MaxTs, false)
	defer txn.Discard()

	results, err := db.duckDBStorage.ScanPrefix([]byte(bankKeyPrefix), txn.readTs)
	if err != nil {
		tb.Fatalf("reads-from: scan final state: %v", err)
	}
	final := make(map[string][]byte, len(results))
	for _, r := range results {
		if r.Found {
			buf := make([]byte, len(r.Value))
			copy(buf, r.Value)
			final[string(r.Key)] = buf
		}
	}

	res := rec.Verify(final)

	switch t := tb.(type) {
	case *testing.T:
		t.Logf("%s", res.Report(20))
	}

	switch {
	case res.Truncated:
		// Not a pass and not a failure of the engine: the harness gave up
		// recording. Say so loudly rather than letting a green test imply a
		// verified execution.
		tb.Errorf("reads-from check INCONCLUSIVE: history truncated (%d txns dropped). "+
			"Raise BADGER_RF_MAX_TXNS or shorten the run.", res.DroppedTxns)
	case len(res.Violations) > 0:
		tb.Errorf("reads-from equivalence violated: %d counterexample(s); "+
			"the concurrent execution is not equivalent to the commit-ts-ordered "+
			"sequential execution", len(res.Violations))
	}
}
