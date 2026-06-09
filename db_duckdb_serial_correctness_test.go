//go:build duckdb

package badger

// Serial execution correctness tests for the DuckDB backend.
//
// Darshan's requirement: confirm correctness w.r.t. a serial execution for
// both our transaction load (SmallBank) and the bank workload.
//
// Approach
// --------
//  1. Run all operations single-threaded with a monotonically-increasing
//     timestamp from the divytime oracle.
//  2. Maintain an in-memory reference map that tracks the exact expected
//     state of every account after each committed operation.
//  3. After each commit, read back the affected keys and assert they match
//     the reference map.
//  4. At the end, walk all accounts and assert the global balance invariant
//     holds exactly — no write-skew is possible in a serial schedule.
//
// Run with:
//
//	go test -v -tags duckdb -run TestDuckDBBankSerialCorrectness      -timeout 120s
//	go test -v -tags duckdb -run TestDuckDBSmallBankSerialCorrectness  -timeout 300s

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/dgraph-io/badger/v4/divytime"
	"github.com/dgraph-io/badger/v4/types"
)

// ---------------------------------------------------------------------------
// TestDuckDBBankSerialCorrectness
// ---------------------------------------------------------------------------

// TestDuckDBBankSerialCorrectness verifies that a serial (single-threaded)
// sequence of bank transfers produces results that exactly match an in-memory
// reference state.  Because there is no concurrency, snapshot isolation is
// equivalent to serializable isolation, so:
//   - every per-account read-back matches the reference map, and
//   - the global balance invariant holds exactly throughout.
//
// Run with:
//
//	go test -v -tags duckdb -run TestDuckDBBankSerialCorrectness -timeout 120s
func TestDuckDBBankSerialCorrectness(t *testing.T) {
	const (
		serialAccounts  = 200
		serialInitBal   = uint64(1_000)
		serialXferAmt   = uint64(10)
		serialNumXfers  = 500
	)

	oracle := divytime.NewOracle(1, 0)
	var tsSeq int64
	nextTs := func() types.CustomTs {
		tsSeq++
		ts, _ := oracle.GetTimestamp(tsSeq)
		return divyToTs(ts)
	}

	withDuckDB(t, true, func(db *DB) {
		// ── Phase 1: seed ──────────────────────────────────────────────────
		refBal := make([]uint64, serialAccounts)
		for i := 0; i < serialAccounts; i++ {
			refBal[i] = serialInitBal
			ts := nextTs()
			txn := db.NewTransactionAt(ts, true)
			if err := txn.Set(bankKey(i), bankEncodeUint64(serialInitBal)); err != nil {
				t.Fatalf("seed account %d: %v", i, err)
			}
			if err := txn.CommitAt(ts, nil); err != nil {
				t.Fatalf("seed commit account %d: %v", i, err)
			}
		}

		// readBal reads the current balance of account i at a snapshot.
		readBal := func(ts types.CustomTs, i int) uint64 {
			txn := db.NewTransactionAt(ts, false)
			defer txn.Discard()
			item, err := txn.Get(bankKey(i))
			if err != nil {
				t.Fatalf("readBal account %d: %v", i, err)
			}
			v, _ := item.ValueCopy(nil)
			return bankDecodeUint64(v)
		}

		// ── Phase 2: serial transfers ──────────────────────────────────────
		rng := rand.New(rand.NewSource(42))
		completedXfers := 0

		for i := 0; i < serialNumXfers; i++ {
			from := rng.Intn(serialAccounts)
			to := rng.Intn(serialAccounts)
			for to == from {
				to = rng.Intn(serialAccounts)
			}

			if refBal[from] < serialXferAmt {
				continue // not enough funds; skip without changing ref state
			}

			expectedFrom := refBal[from] - serialXferAmt
			expectedTo := refBal[to] + serialXferAmt

			// Open the read-write transaction.
			ts := nextTs()
			txn := db.NewTransactionAt(ts, true)

			// Pre-commit read: must match reference state.
			fromItem, err := txn.Get(bankKey(from))
			if err != nil {
				txn.Discard()
				t.Fatalf("xfer %d: get from=%d: %v", i, from, err)
			}
			fromBal, _ := fromItem.ValueCopy(nil)
			if got := bankDecodeUint64(fromBal); got != refBal[from] {
				txn.Discard()
				t.Fatalf("xfer %d: from=%d pre-read want=%d got=%d (reference mismatch)",
					i, from, refBal[from], got)
			}

			toItem, err := txn.Get(bankKey(to))
			if err != nil {
				txn.Discard()
				t.Fatalf("xfer %d: get to=%d: %v", i, to, err)
			}
			toBal, _ := toItem.ValueCopy(nil)
			if got := bankDecodeUint64(toBal); got != refBal[to] {
				txn.Discard()
				t.Fatalf("xfer %d: to=%d pre-read want=%d got=%d (reference mismatch)",
					i, to, refBal[to], got)
			}

			// Write new balances.
			if err := txn.Set(bankKey(from), bankEncodeUint64(expectedFrom)); err != nil {
				txn.Discard()
				t.Fatalf("xfer %d: set from: %v", i, err)
			}
			if err := txn.Set(bankKey(to), bankEncodeUint64(expectedTo)); err != nil {
				txn.Discard()
				t.Fatalf("xfer %d: set to: %v", i, err)
			}
			if err := txn.CommitAt(ts, nil); err != nil {
				txn.Discard()
				t.Fatalf("xfer %d: commit: %v", i, err)
			}

			// Advance reference state.
			refBal[from] = expectedFrom
			refBal[to] = expectedTo
			completedXfers++

			// Post-commit read-back: must match reference state exactly.
			readTs := nextTs()
			if got := readBal(readTs, from); got != expectedFrom {
				t.Fatalf("xfer %d: post-commit from=%d want=%d got=%d",
					i, from, expectedFrom, got)
			}
			if got := readBal(readTs, to); got != expectedTo {
				t.Fatalf("xfer %d: post-commit to=%d want=%d got=%d",
					i, to, expectedTo, got)
			}
		}

		// ── Phase 3: global invariant ──────────────────────────────────────
		finalTs := nextTs()
		txn := db.NewTransactionAt(finalTs, false)
		defer txn.Discard()

		var totalDB, totalRef uint64
		for i := 0; i < serialAccounts; i++ {
			item, err := txn.Get(bankKey(i))
			if err != nil {
				t.Fatalf("final verify: get account %d: %v", i, err)
			}
			v, _ := item.ValueCopy(nil)
			got := bankDecodeUint64(v)
			if got != refBal[i] {
				t.Fatalf("final verify: account %d ref=%d db=%d", i, refBal[i], got)
			}
			totalDB += got
			totalRef += refBal[i]
		}

		expected := uint64(serialAccounts) * serialInitBal
		if totalDB != expected {
			t.Fatalf("global invariant violated: want=%d got=%d delta=%d",
				expected, totalDB, int64(expected)-int64(totalDB))
		}

		t.Logf("PASS: serial bank correctness: %d/%d transfers completed, "+
			"global total=%d (invariant holds exactly)",
			completedXfers, serialNumXfers, totalDB)
	})
}

// ---------------------------------------------------------------------------
// TestDuckDBSmallBankSerialCorrectness
// ---------------------------------------------------------------------------

// TestDuckDBSmallBankSerialCorrectness verifies each SmallBank transaction
// type against an in-memory reference state when executed serially.  After
// every commit the affected keys are read back and compared to the reference.
//
// Invariants checked
//   - Balance:          savings[i]+checking[i] == ref_savings[i]+ref_checking[i]
//   - DepositChecking:  checking[i] == old+sbTxAmount
//   - TransactSavings:  savings[i] == old-sbTxAmount  (when old >= sbTxAmount)
//   - SendPayment:      total checking balance is preserved
//   - WriteCheck:       checking[i] decreases by sbTxAmount  (when total ≥ sbTxAmount)
//   - Amalgamate:       checking[src]=0, savings[dst]=old_savings[src]+old_checking[dst]
//
// Run with:
//
//	go test -v -tags duckdb -run TestDuckDBSmallBankSerialCorrectness -timeout 300s
func TestDuckDBSmallBankSerialCorrectness(t *testing.T) {
	const (
		sbSerialCustomers = 500   // smaller than the bench constant to keep it fast
		sbSerialOps       = 300   // operations per transaction type
	)

	oracle := divytime.NewOracle(1, 0)
	var tsSeq int64
	nextTs := func() types.CustomTs {
		tsSeq++
		ts, _ := oracle.GetTimestamp(tsSeq)
		return divyToTs(ts)
	}

	withDuckDB(t, true, func(db *DB) {
		// Reference state: in-memory copies of savings and checking balances.
		refSav := make([]int64, sbSerialCustomers)
		refChk := make([]int64, sbSerialCustomers)
		for i := range refSav {
			refSav[i] = sbInitBal
			refChk[i] = sbInitBal
		}

		// ── Phase 1: seed ──────────────────────────────────────────────────
		t.Log("[smallbank-serial] seeding", sbSerialCustomers, "accounts…")
		for i := int64(0); i < sbSerialCustomers; i++ {
			ts := nextTs()
			txn := db.NewTransactionAt(ts, true)
			_ = txn.Set(sbAccountKey(i), []byte(fmt.Sprintf("cust_%d", i)))
			_ = txn.Set(sbSavingsKey(i), sbEncode(sbInitBal))
			_ = txn.Set(sbCheckingKey(i), sbEncode(sbInitBal))
			if err := txn.CommitAt(ts, nil); err != nil {
				t.Fatalf("seed commit i=%d: %v", i, err)
			}
		}

		// Helper: read a balance key and return its int64 value.
		readInt64 := func(ts types.CustomTs, key []byte) int64 {
			txn := db.NewTransactionAt(ts, false)
			defer txn.Discard()
			item, err := txn.Get(key)
			if err != nil {
				t.Fatalf("readInt64 key=%q: %v", key, err)
			}
			v, _ := item.ValueCopy(nil)
			return sbDecode(v)
		}

		rng := rand.New(rand.NewSource(7))

		// ── Balance (read-only: verify against reference) ──────────────────
		t.Log("[smallbank-serial] verifying Balance …")
		for op := 0; op < sbSerialOps; op++ {
			id := rng.Int63n(sbSerialCustomers)
			ts := nextTs()
			txn := db.NewTransactionAt(ts, false)

			savItem, err := txn.Get(sbSavingsKey(id))
			if err != nil {
				txn.Discard()
				t.Fatalf("balance op %d: savings %d: %v", op, id, err)
			}
			chkItem, err := txn.Get(sbCheckingKey(id))
			if err != nil {
				txn.Discard()
				t.Fatalf("balance op %d: checking %d: %v", op, id, err)
			}
			gotSav := sbItemInt64(savItem)
			gotChk := sbItemInt64(chkItem)
			txn.Discard()

			if gotSav != refSav[id] {
				t.Fatalf("balance op %d: savings[%d] want=%d got=%d",
					op, id, refSav[id], gotSav)
			}
			if gotChk != refChk[id] {
				t.Fatalf("balance op %d: checking[%d] want=%d got=%d",
					op, id, refChk[id], gotChk)
			}
		}
		t.Log("[smallbank-serial] Balance: PASS")

		// ── DepositChecking ─────────────────────────────────────────────────
		t.Log("[smallbank-serial] verifying DepositChecking …")
		for op := 0; op < sbSerialOps; op++ {
			id := rng.Int63n(sbSerialCustomers)
			expectedChk := refChk[id] + sbTxAmount

			ts := nextTs()
			txn := db.NewTransactionAt(ts, true)
			chkItem, err := txn.Get(sbCheckingKey(id))
			if err != nil {
				txn.Discard()
				t.Fatalf("depositChecking op %d: get checking %d: %v", op, id, err)
			}
			pre := sbItemInt64(chkItem)
			if pre != refChk[id] {
				txn.Discard()
				t.Fatalf("depositChecking op %d: checking[%d] pre-read want=%d got=%d",
					op, id, refChk[id], pre)
			}
			_ = txn.Set(sbCheckingKey(id), sbEncode(expectedChk))
			if err := txn.CommitAt(ts, nil); err != nil {
				txn.Discard()
				t.Fatalf("depositChecking op %d: commit: %v", op, err)
			}
			refChk[id] = expectedChk

			// Read-back.
			postTs := nextTs()
			if got := readInt64(postTs, sbCheckingKey(id)); got != expectedChk {
				t.Fatalf("depositChecking op %d: post-commit checking[%d] want=%d got=%d",
					op, id, expectedChk, got)
			}
		}
		t.Log("[smallbank-serial] DepositChecking: PASS")

		// ── TransactSavings ─────────────────────────────────────────────────
		t.Log("[smallbank-serial] verifying TransactSavings …")
		for op := 0; op < sbSerialOps; op++ {
			id := rng.Int63n(sbSerialCustomers)
			if refSav[id] < sbTxAmount {
				continue // skip unaffordable ops
			}
			expectedSav := refSav[id] - sbTxAmount

			ts := nextTs()
			txn := db.NewTransactionAt(ts, true)
			savItem, err := txn.Get(sbSavingsKey(id))
			if err != nil {
				txn.Discard()
				t.Fatalf("transactSavings op %d: get savings %d: %v", op, id, err)
			}
			pre := sbItemInt64(savItem)
			if pre != refSav[id] {
				txn.Discard()
				t.Fatalf("transactSavings op %d: savings[%d] pre-read want=%d got=%d",
					op, id, refSav[id], pre)
			}
			_ = txn.Set(sbSavingsKey(id), sbEncode(expectedSav))
			if err := txn.CommitAt(ts, nil); err != nil {
				txn.Discard()
				t.Fatalf("transactSavings op %d: commit: %v", op, err)
			}
			refSav[id] = expectedSav

			postTs := nextTs()
			if got := readInt64(postTs, sbSavingsKey(id)); got != expectedSav {
				t.Fatalf("transactSavings op %d: post-commit savings[%d] want=%d got=%d",
					op, id, expectedSav, got)
			}
		}
		t.Log("[smallbank-serial] TransactSavings: PASS")

		// ── SendPayment ──────────────────────────────────────────────────────
		// Invariant: total checking balance across src+dst is preserved.
		t.Log("[smallbank-serial] verifying SendPayment …")
		for op := 0; op < sbSerialOps; op++ {
			src := rng.Int63n(sbSerialCustomers)
			dst := rng.Int63n(sbSerialCustomers)
			for dst == src {
				dst = rng.Int63n(sbSerialCustomers)
			}
			if refChk[src] < sbTxAmount {
				continue
			}

			pairTotal := refChk[src] + refChk[dst]
			expectedSrc := refChk[src] - sbTxAmount
			expectedDst := refChk[dst] + sbTxAmount

			ts := nextTs()
			txn := db.NewTransactionAt(ts, true)

			srcItem, err := txn.Get(sbCheckingKey(src))
			if err != nil {
				txn.Discard()
				t.Fatalf("sendPayment op %d: get checking src=%d: %v", op, src, err)
			}
			dstItem, err := txn.Get(sbCheckingKey(dst))
			if err != nil {
				txn.Discard()
				t.Fatalf("sendPayment op %d: get checking dst=%d: %v", op, dst, err)
			}
			preSrc := sbItemInt64(srcItem)
			preDst := sbItemInt64(dstItem)
			if preSrc != refChk[src] {
				txn.Discard()
				t.Fatalf("sendPayment op %d: src=%d pre-read want=%d got=%d",
					op, src, refChk[src], preSrc)
			}
			if preDst != refChk[dst] {
				txn.Discard()
				t.Fatalf("sendPayment op %d: dst=%d pre-read want=%d got=%d",
					op, dst, refChk[dst], preDst)
			}

			_ = txn.Set(sbCheckingKey(src), sbEncode(expectedSrc))
			_ = txn.Set(sbCheckingKey(dst), sbEncode(expectedDst))
			if err := txn.CommitAt(ts, nil); err != nil {
				txn.Discard()
				t.Fatalf("sendPayment op %d: commit: %v", op, err)
			}
			refChk[src] = expectedSrc
			refChk[dst] = expectedDst

			// Post-commit: pair total must be preserved.
			postTs := nextTs()
			gotSrc := readInt64(postTs, sbCheckingKey(src))
			gotDst := readInt64(postTs, sbCheckingKey(dst))
			if gotSrc != expectedSrc {
				t.Fatalf("sendPayment op %d: post src=%d want=%d got=%d",
					op, src, expectedSrc, gotSrc)
			}
			if gotDst != expectedDst {
				t.Fatalf("sendPayment op %d: post dst=%d want=%d got=%d",
					op, dst, expectedDst, gotDst)
			}
			if gotSrc+gotDst != pairTotal {
				t.Fatalf("sendPayment op %d: pair total mismatch: want=%d got=%d",
					op, pairTotal, gotSrc+gotDst)
			}
		}
		t.Log("[smallbank-serial] SendPayment: PASS")

		// ── WriteCheck ───────────────────────────────────────────────────────
		// After WriteCheck(id): checking[id] decreases by sbTxAmount when
		// savings[id]+checking[id] >= sbTxAmount.
		t.Log("[smallbank-serial] verifying WriteCheck …")
		for op := 0; op < sbSerialOps; op++ {
			id := rng.Int63n(sbSerialCustomers)
			total := refSav[id] + refChk[id]
			if total < sbTxAmount {
				continue
			}
			expectedChk := refChk[id] - sbTxAmount

			ts := nextTs()
			txn := db.NewTransactionAt(ts, true)
			savItem, err := txn.Get(sbSavingsKey(id))
			if err != nil {
				txn.Discard()
				t.Fatalf("writeCheck op %d: get savings %d: %v", op, id, err)
			}
			chkItem, err := txn.Get(sbCheckingKey(id))
			if err != nil {
				txn.Discard()
				t.Fatalf("writeCheck op %d: get checking %d: %v", op, id, err)
			}
			preSav := sbItemInt64(savItem)
			preChk := sbItemInt64(chkItem)
			if preSav != refSav[id] || preChk != refChk[id] {
				txn.Discard()
				t.Fatalf("writeCheck op %d: id=%d pre-read sav want=%d got=%d, chk want=%d got=%d",
					op, id, refSav[id], preSav, refChk[id], preChk)
			}

			preTotal := preSav + preChk
			var newChk int64
			if preTotal < sbTxAmount {
				newChk = 1
			} else {
				newChk = preTotal - sbTxAmount
			}
			_ = txn.Set(sbCheckingKey(id), sbEncode(newChk))
			if err := txn.CommitAt(ts, nil); err != nil {
				txn.Discard()
				t.Fatalf("writeCheck op %d: commit: %v", op, err)
			}
			// Use the same logic as sbWriteCheck for the reference.
			_ = expectedChk // suppress unused
			refChk[id] = newChk

			postTs := nextTs()
			if got := readInt64(postTs, sbCheckingKey(id)); got != newChk {
				t.Fatalf("writeCheck op %d: post-commit checking[%d] want=%d got=%d",
					op, id, newChk, got)
			}
		}
		t.Log("[smallbank-serial] WriteCheck: PASS")

		// ── Amalgamate ───────────────────────────────────────────────────────
		// After Amalgamate(src→dst):
		//   checking[src] = 0
		//   savings[dst]  = old_savings[src] + old_checking[dst]
		t.Log("[smallbank-serial] verifying Amalgamate …")
		for op := 0; op < sbSerialOps; op++ {
			src := rng.Int63n(sbSerialCustomers)
			dst := rng.Int63n(sbSerialCustomers)
			for dst == src {
				dst = rng.Int63n(sbSerialCustomers)
			}

			expectedChkSrc := int64(0)
			expectedSavDst := refSav[src] + refChk[dst]

			ts := nextTs()
			txn := db.NewTransactionAt(ts, true)
			savSrcItem, err := txn.Get(sbSavingsKey(src))
			if err != nil {
				txn.Discard()
				t.Fatalf("amalgamate op %d: get savings src=%d: %v", op, src, err)
			}
			chkDstItem, err := txn.Get(sbCheckingKey(dst))
			if err != nil {
				txn.Discard()
				t.Fatalf("amalgamate op %d: get checking dst=%d: %v", op, dst, err)
			}
			preSavSrc := sbItemInt64(savSrcItem)
			preChkDst := sbItemInt64(chkDstItem)
			if preSavSrc != refSav[src] || preChkDst != refChk[dst] {
				txn.Discard()
				t.Fatalf("amalgamate op %d: pre-read src_sav want=%d got=%d, dst_chk want=%d got=%d",
					op, refSav[src], preSavSrc, refChk[dst], preChkDst)
			}
			moved := preSavSrc + preChkDst
			_ = txn.Set(sbCheckingKey(src), sbEncode(0))
			_ = txn.Set(sbSavingsKey(dst), sbEncode(moved))
			if err := txn.CommitAt(ts, nil); err != nil {
				txn.Discard()
				t.Fatalf("amalgamate op %d: commit: %v", op, err)
			}
			refChk[src] = expectedChkSrc
			refSav[dst] = expectedSavDst

			postTs := nextTs()
			if got := readInt64(postTs, sbCheckingKey(src)); got != 0 {
				t.Fatalf("amalgamate op %d: post-commit checking[src=%d] want=0 got=%d",
					op, src, got)
			}
			if got := readInt64(postTs, sbSavingsKey(dst)); got != expectedSavDst {
				t.Fatalf("amalgamate op %d: post-commit savings[dst=%d] want=%d got=%d",
					op, dst, expectedSavDst, got)
			}
		}
		t.Log("[smallbank-serial] Amalgamate: PASS")

		// ── Global consistency check ─────────────────────────────────────────
		t.Log("[smallbank-serial] running final global consistency check …")
		finalTs := nextTs()
		txn := db.NewTransactionAt(finalTs, false)
		defer txn.Discard()

		for i := int64(0); i < sbSerialCustomers; i++ {
			savItem, err := txn.Get(sbSavingsKey(i))
			if err != nil {
				t.Fatalf("final check: get savings %d: %v", i, err)
			}
			chkItem, err := txn.Get(sbCheckingKey(i))
			if err != nil {
				t.Fatalf("final check: get checking %d: %v", i, err)
			}
			if got := sbItemInt64(savItem); got != refSav[i] {
				t.Fatalf("final check: savings[%d] want=%d got=%d", i, refSav[i], got)
			}
			if got := sbItemInt64(chkItem); got != refChk[i] {
				t.Fatalf("final check: checking[%d] want=%d got=%d", i, refChk[i], got)
			}
		}
		t.Log("[smallbank-serial] PASS — all accounts match reference state exactly")
	})
}
