//go:build duckdb

package badger

// SmallBank microbenchmark for the DuckDB backend.
//
// Mirrors Divy's per-transaction-type measurement table for regular badger vs
// dd_exp, but targets the DuckDB backend so the three can be compared directly.
//
// Key structure matches lock-free-machine/services/server/smallbank_transactions.go:
//   accounts_id_{custId}  → customer name  ([]byte)
//   savings_bal_{custId}  → balance        (int64, little-endian)
//   checking_bal_{custId} → balance        (int64, little-endian)
//
// Run individual transaction-type benchmarks:
//
//	go test -v -tags duckdb -run TestSmallBankDuckDB -timeout 300s
//
// Run concurrent mixed workload (BenchBase weights 15,15,15,25,15,15):
//
//	go test -v -tags duckdb -run TestSmallBankDuckDBMixed -timeout 300s
//
// Run phase-timing breakdown (read vs write vs commit per type):
//
//	go test -v -tags duckdb -run TestSmallBankDuckDBPhases -timeout 300s

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4/divytime"
	"github.com/dgraph-io/badger/v4/types"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	sbNumCustomers = 100_000
	sbInitBal      = int64(1_000_000)
	sbTxAmount     = int64(100)
	sbBenchDur     = 2 * time.Second
	sbWorkers      = 16
	sbIsolateDur   = 1 * time.Second // per-type isolation run
)

// BenchBase weights for mixed workload: Amalgamate, Balance, DepositChecking,
// SendPayment, TransactSavings, WriteCheck → 15,15,15,25,15,15
var sbWeights = []int{15, 15, 15, 25, 15, 15} // cumulative built below

// ---------------------------------------------------------------------------
// Key helpers  (identical to server/smallbank_transactions.go)
// ---------------------------------------------------------------------------

// Key format: "<id>:accounts_id" so all three keys for a given customer
// share the same routing prefix and land in the same DuckDB partition.
// This lets ReadBatch serve all reads for a transaction in one SQL query.
func sbKey(id int64, suffix string) []byte {
	// 20 bytes is enough for base-10 int64 (-9223372036854775808).
	b := make([]byte, 0, 20+1+len(suffix))
	b = strconv.AppendInt(b, id, 10)
	b = append(b, ':')
	b = append(b, suffix...)
	return b
}

func sbAccountKey(id int64) []byte {
	return sbKey(id, "accounts_id")
}
func sbSavingsKey(id int64) []byte {
	return sbKey(id, "savings_bal")
}
func sbCheckingKey(id int64) []byte {
	return sbKey(id, "checking_bal")
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

func sbEncode(v int64) []byte {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, v)
	return buf.Bytes()
}

func sbDecode(b []byte) int64 {
	var v int64
	_ = binary.Read(bytes.NewReader(b), binary.LittleEndian, &v)
	return v
}

func sbItemInt64(item *Item) int64 {
	if item == nil {
		return 0
	}
	var buf [8]byte
	_, _ = item.ValueCopy(buf[:])
	return int64(binary.LittleEndian.Uint64(buf[:]))
}

// ---------------------------------------------------------------------------
// Timestamp helper
// ---------------------------------------------------------------------------

var sbSeq atomic.Int64

func sbTs(oracle *divytime.Oracle) types.CustomTs {
	seq := sbSeq.Add(1)
	ts, _ := oracle.GetTimestamp(seq)
	return divyToTs(ts) // reuse helper from db_duckdb_bank_test.go
}

// ---------------------------------------------------------------------------
// Seeding
// ---------------------------------------------------------------------------

func sbSeed(tb testing.TB, db *DB, oracle *divytime.Oracle) {
	tb.Helper()
	tb.Log("[smallbank] seeding", sbNumCustomers, "accounts…")
	start := time.Now()

	for i := int64(0); i < sbNumCustomers; i++ {
		ts := sbTs(oracle)
		txn := db.NewTransactionAt(ts, true)
		_ = txn.Set(sbAccountKey(i), []byte(fmt.Sprintf("cust_%d", i)))
		_ = txn.Set(sbSavingsKey(i), sbEncode(sbInitBal))
		_ = txn.Set(sbCheckingKey(i), sbEncode(sbInitBal))
		if err := txn.CommitAt(ts, nil); err != nil {
			tb.Fatalf("seed commit i=%d: %v", i, err)
		}
	}
	tb.Logf("[smallbank] seeded in %v", time.Since(start).Round(time.Millisecond))
}

// ---------------------------------------------------------------------------
// SmallBank transaction implementations
// ---------------------------------------------------------------------------

// sbBalance reads savings + checking for one account (3 reads, 0 writes).
func sbBalance(db *DB, oracle *divytime.Oracle, rng *rand.Rand) (time.Duration, error) {
	id := rng.Int63n(sbNumCustomers)
	ts := sbTs(oracle)
	start := time.Now()

	txn := db.NewTransactionAt(ts, false)
	defer txn.Discard()

	_ = txn.PrefetchKeys([][]byte{sbAccountKey(id), sbSavingsKey(id), sbCheckingKey(id)})

	if _, err := txn.Get(sbAccountKey(id)); err != nil {
		return time.Since(start), err
	}
	savItem, err := txn.Get(sbSavingsKey(id))
	if err != nil {
		return time.Since(start), err
	}
	_ = sbItemInt64(savItem)
	chkItem, err := txn.Get(sbCheckingKey(id))
	if err != nil {
		return time.Since(start), err
	}
	_ = sbItemInt64(chkItem)

	err = txn.CommitAt(ts, nil)
	return time.Since(start), err
}

// sbDepositChecking reads checking balance, adds amount, writes back (2 reads, 1 write).
func sbDepositChecking(db *DB, oracle *divytime.Oracle, rng *rand.Rand) (time.Duration, error) {
	id := rng.Int63n(sbNumCustomers)
	ts := sbTs(oracle)
	start := time.Now()

	txn := db.NewTransactionAt(ts, true)
	defer txn.Discard()

	_ = txn.PrefetchKeys([][]byte{sbAccountKey(id), sbCheckingKey(id)})

	if _, err := txn.Get(sbAccountKey(id)); err != nil {
		return time.Since(start), err
	}
	chkItem, err := txn.Get(sbCheckingKey(id))
	if err != nil {
		return time.Since(start), err
	}
	newBal := sbItemInt64(chkItem) + sbTxAmount
	if err := txn.Set(sbCheckingKey(id), sbEncode(newBal)); err != nil {
		return time.Since(start), err
	}
	err = txn.CommitAt(ts, nil)
	return time.Since(start), err
}

// sbTransactSavings reads savings balance, subtracts amount, writes back (2 reads, 1 write).
func sbTransactSavings(db *DB, oracle *divytime.Oracle, rng *rand.Rand) (time.Duration, error) {
	id := rng.Int63n(sbNumCustomers)
	ts := sbTs(oracle)
	start := time.Now()

	txn := db.NewTransactionAt(ts, true)
	defer txn.Discard()

	_ = txn.PrefetchKeys([][]byte{sbAccountKey(id), sbSavingsKey(id)})

	if _, err := txn.Get(sbAccountKey(id)); err != nil {
		return time.Since(start), err
	}
	savItem, err := txn.Get(sbSavingsKey(id))
	if err != nil {
		return time.Since(start), err
	}
	bal := sbItemInt64(savItem)
	if bal < sbTxAmount {
		_ = txn.CommitAt(ts, nil)
		return time.Since(start), nil
	}
	if err := txn.Set(sbSavingsKey(id), sbEncode(bal-sbTxAmount)); err != nil {
		return time.Since(start), err
	}
	err = txn.CommitAt(ts, nil)
	return time.Since(start), err
}

// sbWriteCheck reads savings+checking, subtracts from checking (3 reads, 1 write).
func sbWriteCheck(db *DB, oracle *divytime.Oracle, rng *rand.Rand) (time.Duration, error) {
	id := rng.Int63n(sbNumCustomers)
	ts := sbTs(oracle)
	start := time.Now()

	txn := db.NewTransactionAt(ts, true)
	defer txn.Discard()

	_ = txn.PrefetchKeys([][]byte{sbAccountKey(id), sbSavingsKey(id), sbCheckingKey(id)})

	if _, err := txn.Get(sbAccountKey(id)); err != nil {
		return time.Since(start), err
	}
	savItem, err := txn.Get(sbSavingsKey(id))
	if err != nil {
		return time.Since(start), err
	}
	chkItem, err := txn.Get(sbCheckingKey(id))
	if err != nil {
		return time.Since(start), err
	}
	total := sbItemInt64(savItem) + sbItemInt64(chkItem)
	if total < sbTxAmount {
		total = 1
	} else {
		total -= sbTxAmount
	}
	if err := txn.Set(sbCheckingKey(id), sbEncode(total)); err != nil {
		return time.Since(start), err
	}
	err = txn.CommitAt(ts, nil)
	return time.Since(start), err
}

// sbSendPayment reads 2 checking balances, writes both (3 reads, 2 writes).
func sbSendPayment(db *DB, oracle *divytime.Oracle, rng *rand.Rand) (time.Duration, error) {
	src := rng.Int63n(sbNumCustomers)
	dst := rng.Int63n(sbNumCustomers)
	for dst == src {
		dst = rng.Int63n(sbNumCustomers)
	}
	ts := sbTs(oracle)
	start := time.Now()

	txn := db.NewTransactionAt(ts, true)
	defer txn.Discard()

	_ = txn.PrefetchKeys([][]byte{
		sbAccountKey(src), sbAccountKey(dst),
		sbCheckingKey(src), sbCheckingKey(dst),
	})

	if _, err := txn.Get(sbAccountKey(src)); err != nil {
		return time.Since(start), err
	}
	if _, err := txn.Get(sbAccountKey(dst)); err != nil {
		return time.Since(start), err
	}
	srcItem, err := txn.Get(sbCheckingKey(src))
	if err != nil {
		return time.Since(start), err
	}
	dstItem, err := txn.Get(sbCheckingKey(dst))
	if err != nil {
		return time.Since(start), err
	}
	srcBal := sbItemInt64(srcItem)
	dstBal := sbItemInt64(dstItem)
	if srcBal < sbTxAmount {
		_ = txn.CommitAt(ts, nil)
		return time.Since(start), nil
	}
	_ = txn.Set(sbCheckingKey(src), sbEncode(srcBal-sbTxAmount))
	_ = txn.Set(sbCheckingKey(dst), sbEncode(dstBal+sbTxAmount))
	err = txn.CommitAt(ts, nil)
	return time.Since(start), err
}

// sbAmalgamate moves checking[src] into savings[dst] and zeroes checking[src].
func sbAmalgamate(db *DB, oracle *divytime.Oracle, rng *rand.Rand) (time.Duration, error) {
	src := rng.Int63n(sbNumCustomers)
	dst := rng.Int63n(sbNumCustomers)
	for dst == src {
		dst = rng.Int63n(sbNumCustomers)
	}
	ts := sbTs(oracle)
	start := time.Now()

	txn := db.NewTransactionAt(ts, true)
	defer txn.Discard()

	_ = txn.PrefetchKeys([][]byte{
		sbAccountKey(src), sbAccountKey(dst),
		sbCheckingKey(src), sbSavingsKey(dst),
	})

	if _, err := txn.Get(sbAccountKey(src)); err != nil {
		return time.Since(start), err
	}
	if _, err := txn.Get(sbAccountKey(dst)); err != nil {
		return time.Since(start), err
	}
	chkItem, err := txn.Get(sbCheckingKey(src))
	if err != nil {
		return time.Since(start), err
	}
	savItem, err := txn.Get(sbSavingsKey(dst))
	if err != nil {
		return time.Since(start), err
	}
	newSav := sbItemInt64(savItem) + sbItemInt64(chkItem)
	_ = txn.Set(sbCheckingKey(src), sbEncode(0))
	_ = txn.Set(sbSavingsKey(dst), sbEncode(newSav))
	err = txn.CommitAt(ts, nil)
	return time.Since(start), err
}

// ---------------------------------------------------------------------------
// Stats helpers
// ---------------------------------------------------------------------------

type sbStats struct {
	mu      sync.Mutex
	samples []int64 // nanoseconds
	errors  int64
}

func (s *sbStats) record(d time.Duration, err error) {
	s.mu.Lock()
	if err != nil {
		s.errors++
	} else {
		s.samples = append(s.samples, int64(d))
	}
	s.mu.Unlock()
}

type sbResult struct {
	count  int64
	errors int64
	mean   time.Duration
	p50    time.Duration
	p90    time.Duration
	p99    time.Duration
	min    time.Duration
	max    time.Duration
	tps    float64
}

func (s *sbStats) result(elapsed time.Duration) sbResult {
	s.mu.Lock()
	raw := make([]int64, len(s.samples))
	copy(raw, s.samples)
	errs := s.errors
	s.mu.Unlock()

	if len(raw) == 0 {
		return sbResult{errors: errs}
	}
	sort.Slice(raw, func(i, j int) bool { return raw[i] < raw[j] })
	var total int64
	for _, v := range raw {
		total += v
	}
	pct := func(p float64) time.Duration {
		idx := int(float64(len(raw)) * p)
		if idx >= len(raw) {
			idx = len(raw) - 1
		}
		return time.Duration(raw[idx])
	}
	return sbResult{
		count:  int64(len(raw)),
		errors: errs,
		mean:   time.Duration(total / int64(len(raw))),
		p50:    pct(0.50),
		p90:    pct(0.90),
		p99:    pct(0.99),
		min:    time.Duration(raw[0]),
		max:    time.Duration(raw[len(raw)-1]),
		tps:    float64(len(raw)) / elapsed.Seconds(),
	}
}

// ---------------------------------------------------------------------------
// Isolation benchmark: one transaction type at a time, N workers
// ---------------------------------------------------------------------------

type sbTxFn func(db *DB, oracle *divytime.Oracle, rng *rand.Rand) (time.Duration, error)

func sbRunIsolated(t *testing.T, db *DB, oracle *divytime.Oracle, name string, fn sbTxFn) sbResult {
	t.Helper()
	stats := &sbStats{}
	var stop int32
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < sbWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			for atomic.LoadInt32(&stop) == 0 {
				d, err := fn(db, oracle, rng)
				stats.record(d, err)
			}
		}()
	}
	time.Sleep(sbIsolateDur)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
	elapsed := time.Since(start)

	r := stats.result(elapsed)
	t.Logf("  %-20s  mean=%-10v  p50=%-10v  p90=%-10v  p99=%-10v  tps=%-10.0f  errors=%d",
		name,
		r.mean.Round(time.Microsecond),
		r.p50.Round(time.Microsecond),
		r.p90.Round(time.Microsecond),
		r.p99.Round(time.Microsecond),
		r.tps,
		r.errors,
	)
	return r
}

// ---------------------------------------------------------------------------
// TestSmallBankDuckDB — per-type isolation benchmark
// ---------------------------------------------------------------------------

// TestSmallBankDuckDB runs each SmallBank transaction type in isolation with
// 16 concurrent workers and prints per-type latency and throughput.
// Output is directly comparable to Divy's regular-badger vs dd_exp table.
//
//	go test -v -tags duckdb -run TestSmallBankDuckDB -timeout 300s
func TestSmallBankDuckDB(t *testing.T) {
	oracle := divytime.NewOracle(1, 0)
	withDuckDB(t, true, func(db *DB) {
		sbSeed(t, db, oracle)

		type entry struct {
			name string
			fn   sbTxFn
			ops  string
		}
		txTypes := []entry{
			{"Balance", sbBalance, "3R 0W"},
			{"DepositChecking", sbDepositChecking, "2R 1W"},
			{"TransactSavings", sbTransactSavings, "2R 1W"},
			{"WriteCheck", sbWriteCheck, "3R 1W"},
			{"SendPayment", sbSendPayment, "4R 2W"},
			{"Amalgamate", sbAmalgamate, "4R 2W"},
		}

		t.Logf("\n=== DuckDB SmallBank — Per-Type Isolation (%d workers, %v each) ===", sbWorkers, sbIsolateDur)
		t.Logf("%-20s  %-6s  %-12s  %-12s  %-12s  %-12s  %s",
			"Transaction", "Ops", "Mean", "p50", "p90", "p99", "TPS")
		t.Logf("%s", "-------------------------------------------------------------------------------------")

		results := make(map[string]sbResult)
		for _, tx := range txTypes {
			r := sbRunIsolated(t, db, oracle, tx.name+" ("+tx.ops+")", tx.fn)
			results[tx.name] = r
		}

		// Print summary table matching Divy's format
		fmt.Println()
		fmt.Printf("%-22s  %12s  %15s\n", "Transaction", "Mean latency", "Throughput")
		fmt.Printf("%s\n", "---------------------------------------------------")
		for _, tx := range txTypes {
			r := results[tx.name]
			fmt.Printf("%-22s  %12v  %12.0f tx/s\n",
				tx.name, r.mean.Round(time.Microsecond), r.tps)
		}
		fmt.Println()
	})
}

// ---------------------------------------------------------------------------
// TestSmallBankDuckDBMixed — concurrent mixed workload (BenchBase weights)
// ---------------------------------------------------------------------------

// TestSmallBankDuckDBMixed runs all 6 transaction types concurrently with
// BenchBase weights (15,15,15,25,15,15) and 16 workers for sbBenchDur.
// This matches the actual production workload.
//
//	go test -v -tags duckdb -run TestSmallBankDuckDBMixed -timeout 120s
func TestSmallBankDuckDBMixed(t *testing.T) {
	oracle := divytime.NewOracle(1, 0)
	withDuckDB(t, true, func(db *DB) {
		sbSeed(t, db, oracle)

		// Build cumulative weight table for weighted random selection.
		// Order: Amalgamate, Balance, DepositChecking, SendPayment, TransactSavings, WriteCheck
		fns := []sbTxFn{sbAmalgamate, sbBalance, sbDepositChecking, sbSendPayment, sbTransactSavings, sbWriteCheck}
		names := []string{"Amalgamate", "Balance", "DepositChecking", "SendPayment", "TransactSavings", "WriteCheck"}
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

		for w := 0; w < sbWorkers; w++ {
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

		time.Sleep(sbBenchDur)
		atomic.StoreInt32(&stop, 1)
		wg.Wait()
		elapsed := time.Since(start)

		t.Logf("\n=== DuckDB SmallBank — Mixed Workload (%d workers, %v) ===", sbWorkers, elapsed.Round(time.Second))
		t.Logf("%-20s  %-12s  %-12s  %-12s  %-10s", "Transaction", "Mean", "p90", "p99", "TPS")
		t.Logf("%s", "---------------------------------------------------------------")

		var totalOps int64
		for i, name := range names {
			r := perType[i].result(elapsed)
			totalOps += r.count
			t.Logf("  %-20s  %-12v  %-12v  %-12v  %.0f",
				name,
				r.mean.Round(time.Microsecond),
				r.p90.Round(time.Microsecond),
				r.p99.Round(time.Microsecond),
				r.tps)
		}
		overallTPS := float64(totalOps) / elapsed.Seconds()
		t.Logf("")
		t.Logf("  TOTAL TPS: %.0f  (over %v)", overallTPS, elapsed.Round(time.Millisecond))
	})
}

// ---------------------------------------------------------------------------
// TestSmallBankDuckDBPhases — read vs write vs commit breakdown per type
// ---------------------------------------------------------------------------

// TestSmallBankDuckDBPhases times each phase of each transaction separately
// to pinpoint whether latency comes from reads, writes, or the commit itself.
//
//	go test -v -tags duckdb -run TestSmallBankDuckDBPhases -timeout 120s
func TestSmallBankDuckDBPhases(t *testing.T) {
	oracle := divytime.NewOracle(1, 0)
	withDuckDB(t, true, func(db *DB) {
		sbSeed(t, db, oracle)

		const runs = 10_000

		type phaseResult struct {
			name     string
			readNs   []int64
			writeNs  []int64
			commitNs []int64
		}

		measure := func(name string, fn func(oracle *divytime.Oracle, rng *rand.Rand) (readT, writeT, commitT time.Duration)) phaseResult {
			rng := rand.New(rand.NewSource(42))
			pr := phaseResult{name: name}
			for i := 0; i < runs; i++ {
				r, w, c := fn(oracle, rng)
				pr.readNs = append(pr.readNs, int64(r))
				pr.writeNs = append(pr.writeNs, int64(w))
				pr.commitNs = append(pr.commitNs, int64(c))
			}
			return pr
		}

		avgNs := func(samples []int64) time.Duration {
			if len(samples) == 0 {
				return 0
			}
			var sum int64
			for _, v := range samples {
				sum += v
			}
			return time.Duration(sum / int64(len(samples)))
		}

		// Balance: 3 reads, 0 writes
		balPhase := measure("Balance", func(oracle *divytime.Oracle, rng *rand.Rand) (time.Duration, time.Duration, time.Duration) {
			id := rng.Int63n(sbNumCustomers)
			ts := sbTs(oracle)

			txn := db.NewTransactionAt(ts, false)
			defer txn.Discard()

			t0 := time.Now()
			_ = txn.PrefetchKeys([][]byte{sbAccountKey(id), sbSavingsKey(id), sbCheckingKey(id)})
			_, _ = txn.Get(sbAccountKey(id))
			_, _ = txn.Get(sbSavingsKey(id))
			_, _ = txn.Get(sbCheckingKey(id))
			readT := time.Since(t0)

			t1 := time.Now()
			commitT := time.Since(t1)
			_ = txn.CommitAt(ts, nil)

			return readT, 0, commitT
		})

		// DepositChecking: 2 reads, 1 write
		depPhase := measure("DepositChecking", func(oracle *divytime.Oracle, rng *rand.Rand) (time.Duration, time.Duration, time.Duration) {
			id := rng.Int63n(sbNumCustomers)
			ts := sbTs(oracle)

			txn := db.NewTransactionAt(ts, true)
			defer txn.Discard()

			t0 := time.Now()
			_ = txn.PrefetchKeys([][]byte{sbAccountKey(id), sbCheckingKey(id)})
			_, _ = txn.Get(sbAccountKey(id))
			chkItem, _ := txn.Get(sbCheckingKey(id))
			bal := sbItemInt64(chkItem) + sbTxAmount
			readT := time.Since(t0)

			t1 := time.Now()
			_ = txn.Set(sbCheckingKey(id), sbEncode(bal))
			writeT := time.Since(t1)

			t2 := time.Now()
			_ = txn.CommitAt(ts, nil)
			commitT := time.Since(t2)

			return readT, writeT, commitT
		})

		// SendPayment: 4 reads, 2 writes
		sendPhase := measure("SendPayment", func(oracle *divytime.Oracle, rng *rand.Rand) (time.Duration, time.Duration, time.Duration) {
			src := rng.Int63n(sbNumCustomers)
			dst := rng.Int63n(sbNumCustomers)
			for dst == src {
				dst = rng.Int63n(sbNumCustomers)
			}
			ts := sbTs(oracle)

			txn := db.NewTransactionAt(ts, true)
			defer txn.Discard()

			t0 := time.Now()
			_ = txn.PrefetchKeys([][]byte{
				sbAccountKey(src), sbAccountKey(dst),
				sbCheckingKey(src), sbCheckingKey(dst),
			})
			_, _ = txn.Get(sbAccountKey(src))
			_, _ = txn.Get(sbAccountKey(dst))
			srcItem, _ := txn.Get(sbCheckingKey(src))
			dstItem, _ := txn.Get(sbCheckingKey(dst))
			srcBal := sbItemInt64(srcItem)
			dstBal := sbItemInt64(dstItem)
			readT := time.Since(t0)

			t1 := time.Now()
			_ = txn.Set(sbCheckingKey(src), sbEncode(srcBal-sbTxAmount))
			_ = txn.Set(sbCheckingKey(dst), sbEncode(dstBal+sbTxAmount))
			writeT := time.Since(t1)

			t2 := time.Now()
			_ = txn.CommitAt(ts, nil)
			commitT := time.Since(t2)

			return readT, writeT, commitT
		})

		t.Logf("\n=== DuckDB SmallBank — Phase Breakdown (%d runs, single-threaded) ===", runs)
		t.Logf("%-22s  %12s  %12s  %12s  %12s", "Transaction", "Read avg", "Write avg", "Commit avg", "Total avg")
		t.Logf("%s", "--------------------------------------------------------------------------")

		for _, pr := range []phaseResult{balPhase, depPhase, sendPhase} {
			r := avgNs(pr.readNs)
			w := avgNs(pr.writeNs)
			c := avgNs(pr.commitNs)
			total := r + w + c
			t.Logf("  %-20s  %12v  %12v  %12v  %12v",
				pr.name,
				r.Round(time.Microsecond),
				w.Round(time.Microsecond),
				c.Round(time.Microsecond),
				total.Round(time.Microsecond))
		}

		t.Logf("")
		t.Logf("NOTE: If read avg >> write avg, SQL query overhead or flush-on-demand is the bottleneck.")
		t.Logf("NOTE: If commit avg is high, CGo Appender.Flush() boundary cost dominates.")
		t.Logf("NOTE: If write avg ≈ 0 but commit avg is high, the DirectFlush path is the bottleneck.")
	})
}

// ---------------------------------------------------------------------------
// BenchmarkSmallBankBalance — pprof-friendly benchmark for the Balance txn
// ---------------------------------------------------------------------------

// BenchmarkSmallBankBalance runs the Balance transaction (3 reads, 0 writes)
// in a tight loop so that cpu/memory profiles capture where the time goes.
//
//	go test -tags duckdb -bench='^BenchmarkSmallBankBalance$' -benchtime=30s \
//	        -cpuprofile=cpu_duckdb.prof
//	go tool pprof -top cpu_duckdb.prof
func BenchmarkSmallBankBalance(b *testing.B) {
	oracle := divytime.NewOracle(1, 0)
	withDuckDB(b, true, func(db *DB) {
		sbSeed(b, db, oracle)
		rng := rand.New(rand.NewSource(42))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := rng.Int63n(sbNumCustomers)
			ts := sbTs(oracle)
			txn := db.NewTransactionAt(ts, false)
			_ = txn.PrefetchKeys([][]byte{sbAccountKey(id), sbSavingsKey(id), sbCheckingKey(id)})
			_, _ = txn.Get(sbAccountKey(id))
			_, _ = txn.Get(sbSavingsKey(id))
			_, _ = txn.Get(sbCheckingKey(id))
			_ = txn.CommitAt(ts, nil)
			txn.Discard()
		}
	})
}
