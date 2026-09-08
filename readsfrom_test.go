/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Self-tests for the reads-from equivalence checker.
 *
 * A correctness checker that has only ever been run against passing executions
 * is worthless — it may be vacuously true. Every test below that asserts PASS
 * is paired with a test that injects a specific, realistic anomaly and asserts
 * the checker names it. These run with plain `go test` (no duckdb build tag);
 * they exercise the checker logic itself, not the storage engine.
 */

package badger

import (
	"testing"

	"github.com/dgraph-io/badger/v4/types"
)

func rfTs(n uint32) types.CustomTs {
	return types.CustomTs{EpochID: 0, BrokerID: 1, AssignedTs: n}
}

func rfBal(v byte) []byte { return []byte{v} }

// seedTxn is the initial-state transaction: two accounts at 100 each.
func rfSeed(rec *RFRecorder) {
	rec.Add(&RFTxn{
		ID:        rec.NextID(),
		Label:     "SEED",
		ReadTs:    rfTs(0),
		CommitTs:  rfTs(1),
		Committed: true,
		Writes: []RFWrite{
			{Key: []byte("A"), Value: rfBal(100)},
			{Key: []byte("B"), Value: rfBal(100)},
		},
	})
}

// TestReadsFromCleanSerialHistory: two transfers that do not overlap. Each
// reads the snapshot the previous one produced. Must pass all three read
// checks and the final-state check.
func TestReadsFromCleanSerialHistory(t *testing.T) {
	rec := NewRFRecorder(0)
	rfSeed(rec)

	// T2: A -= 10, B += 10. Reads the seed versions.
	rec.Add(&RFTxn{
		ID:        rec.NextID(),
		Label:     "TRANSFER",
		ReadTs:    rfTs(1),
		CommitTs:  rfTs(2),
		Committed: true,
		Reads: []RFRead{
			{Key: []byte("A"), Found: true, Value: rfBal(100), ObservedVersion: rfTs(1)},
			{Key: []byte("B"), Found: true, Value: rfBal(100), ObservedVersion: rfTs(1)},
		},
		Writes: []RFWrite{
			{Key: []byte("A"), Value: rfBal(90)},
			{Key: []byte("B"), Value: rfBal(110)},
		},
	})

	// T3: reads what T2 wrote, at version 2.
	rec.Add(&RFTxn{
		ID:        rec.NextID(),
		Label:     "TRANSFER",
		ReadTs:    rfTs(2),
		CommitTs:  rfTs(3),
		Committed: true,
		Reads: []RFRead{
			{Key: []byte("A"), Found: true, Value: rfBal(90), ObservedVersion: rfTs(2)},
			{Key: []byte("B"), Found: true, Value: rfBal(110), ObservedVersion: rfTs(2)},
		},
		Writes: []RFWrite{
			{Key: []byte("A"), Value: rfBal(80)},
			{Key: []byte("B"), Value: rfBal(120)},
		},
	})

	final := map[string][]byte{"A": rfBal(80), "B": rfBal(120)}
	res := rec.Verify(final)
	if !res.OK() {
		t.Fatalf("expected clean history to verify, got:\n%s", res.Report(10))
	}
	if res.ReadsChecked != 4 {
		t.Errorf("ReadsChecked = %d, want 4", res.ReadsChecked)
	}
	if res.CommittedTxns != 3 {
		t.Errorf("CommittedTxns = %d, want 3", res.CommittedTxns)
	}
}

// TestReadsFromLostUpdate is the anomaly that motivates the whole exercise and
// the one the balance-sum invariant cannot see.
//
// T2 and T3 both read A=100 at readTs=1. T2 commits at ts=2 writing A=90.
// T3 commits at ts=3 writing A=90 as well (it computed 100-10 from its stale
// read). One transfer is lost. The SUM of balances is still self-consistent
// with T3's own arithmetic, so a total-balance check can be fooled; the
// reads-from check cannot be, because at position ts=3 the replay says A must
// read as 90-at-version-2, not 100-at-version-1.
func TestReadsFromLostUpdate(t *testing.T) {
	rec := NewRFRecorder(0)
	rfSeed(rec)

	rec.Add(&RFTxn{
		ID:        rec.NextID(),
		Label:     "TRANSFER-T2",
		ReadTs:    rfTs(1),
		CommitTs:  rfTs(2),
		Committed: true,
		Reads: []RFRead{
			{Key: []byte("A"), Found: true, Value: rfBal(100), ObservedVersion: rfTs(1)},
		},
		Writes: []RFWrite{{Key: []byte("A"), Value: rfBal(90)}},
	})

	// The offender: readTs=1, so it never saw T2's commit at ts=2, yet it
	// commits at ts=3. hasConflict is supposed to abort exactly this.
	rec.Add(&RFTxn{
		ID:        rec.NextID(),
		Label:     "TRANSFER-T3-STALE",
		ReadTs:    rfTs(1),
		CommitTs:  rfTs(3),
		Committed: true,
		Reads: []RFRead{
			{Key: []byte("A"), Found: true, Value: rfBal(100), ObservedVersion: rfTs(1)},
		},
		Writes: []RFWrite{{Key: []byte("A"), Value: rfBal(90)}},
	})

	res := rec.Verify(map[string][]byte{"A": rfBal(90), "B": rfBal(100)})
	if res.OK() {
		t.Fatalf("checker missed a lost update:\n%s", res.Report(10))
	}

	var found bool
	for _, v := range res.Violations {
		if v.Kind == RFReadValueMismatch && v.TxnID == 3 && string(v.Key) == "A" {
			found = true
			if string(v.ExpectedValue) != string(rfBal(90)) {
				t.Errorf("ExpectedValue = %x, want %x", v.ExpectedValue, rfBal(90))
			}
			if !v.ExpectedVersion.Equal(rfTs(2)) {
				t.Errorf("ExpectedVersion = %s, want %s", v.ExpectedVersion, rfTs(2))
			}
		}
	}
	if !found {
		t.Fatalf("no read-value-mismatch attributed to the stale txn:\n%s", res.Report(10))
	}
}

// TestReadsFromVersionMismatchOnEqualBytes is the case a value-only comparison
// cannot catch: the read observed the correct bytes but from the wrong writer.
// Both T2 and T3 write the byte 90, so at position ts=4 the value comparison
// succeeds while the reads-from edge is wrong.
func TestReadsFromVersionMismatchOnEqualBytes(t *testing.T) {
	rec := NewRFRecorder(0)
	rfSeed(rec)

	rec.Add(&RFTxn{
		ID: rec.NextID(), Label: "W1",
		ReadTs: rfTs(1), CommitTs: rfTs(2), Committed: true,
		Writes: []RFWrite{{Key: []byte("A"), Value: rfBal(90)}},
	})
	rec.Add(&RFTxn{
		ID: rec.NextID(), Label: "W2",
		ReadTs: rfTs(2), CommitTs: rfTs(3), Committed: true,
		Writes: []RFWrite{{Key: []byte("A"), Value: rfBal(90)}},
	})
	// Reader at ts=4 must read from W2 (version 3) but reports version 2.
	rec.Add(&RFTxn{
		ID: rec.NextID(), Label: "R",
		ReadTs: rfTs(4), CommitTs: types.CustomTs{}, Committed: true,
		Reads: []RFRead{
			{Key: []byte("A"), Found: true, Value: rfBal(90), ObservedVersion: rfTs(2)},
		},
	})

	res := rec.Verify(nil)
	if res.OK() {
		t.Fatalf("checker missed a reads-from edge violation:\n%s", res.Report(10))
	}
	var kinds []RFViolationKind
	for _, v := range res.Violations {
		kinds = append(kinds, v.Kind)
	}
	if len(kinds) != 1 || kinds[0] != RFReadVersionMismatch {
		t.Fatalf("violations = %v, want exactly [read-version-mismatch]:\n%s", kinds, res.Report(10))
	}
}

// TestReadsFromReadOnlySeesEqualTimestampCommit pins the visibility rule the
// event ordering depends on: a snapshot at readTs X must see a commit at
// exactly X, because visibility here is version <= readTs (hasConflict skips
// committed txns with ts <= readTs, and duckDBTracker.waitUntil(readTs) waits
// for commits with ts <= readTs). If this test ever fails, the phase ordering
// in Verify is wrong, not the engine.
func TestReadsFromReadOnlySeesEqualTimestampCommit(t *testing.T) {
	rec := NewRFRecorder(0)
	rec.Add(&RFTxn{
		ID: rec.NextID(), Label: "W",
		ReadTs: rfTs(1), CommitTs: rfTs(5), Committed: true,
		Writes: []RFWrite{{Key: []byte("A"), Value: rfBal(42)}},
	})
	rec.Add(&RFTxn{
		ID: rec.NextID(), Label: "READ_ONLY",
		ReadTs: rfTs(5), Committed: true,
		Reads: []RFRead{
			{Key: []byte("A"), Found: true, Value: rfBal(42), ObservedVersion: rfTs(5)},
		},
	})
	if res := rec.Verify(nil); !res.OK() {
		t.Fatalf("snapshot at readTs must see a commit at the same ts:\n%s", res.Report(10))
	}
}

// TestReadsFromAbortedTxnsExcluded: an aborted attempt is allowed to have
// observed a stale snapshot — that is why it aborted. It must not be replayed
// and its reads must not be checked.
func TestReadsFromAbortedTxnsExcluded(t *testing.T) {
	rec := NewRFRecorder(0)
	rfSeed(rec)
	rec.Add(&RFTxn{
		ID: rec.NextID(), Label: "W",
		ReadTs: rfTs(1), CommitTs: rfTs(2), Committed: true,
		Writes: []RFWrite{{Key: []byte("A"), Value: rfBal(90)}},
	})
	// Aborted: read stale A=100 and would have written garbage.
	rec.Add(&RFTxn{
		ID: rec.NextID(), Label: "ABORTED",
		ReadTs: rfTs(1), CommitTs: rfTs(3), Committed: false,
		Reads:  []RFRead{{Key: []byte("A"), Found: true, Value: rfBal(100), ObservedVersion: rfTs(1)}},
		Writes: []RFWrite{{Key: []byte("A"), Value: rfBal(7)}},
	})

	res := rec.Verify(map[string][]byte{"A": rfBal(90), "B": rfBal(100)})
	if !res.OK() {
		t.Fatalf("aborted attempt must not produce violations:\n%s", res.Report(10))
	}
	if res.AbortedTxns != 1 {
		t.Errorf("AbortedTxns = %d, want 1", res.AbortedTxns)
	}
	if res.ReadsChecked != 0 {
		t.Errorf("ReadsChecked = %d, want 0 (only the aborted txn had reads)", res.ReadsChecked)
	}
}

// TestReadsFromDetectsMissingWriteInFinalState catches a commit that was
// acknowledged but never landed — the ReasonDuckDBFlushError shape, where
// doneCommit advanced the watermark past a flush that failed.
func TestReadsFromDetectsMissingWriteInFinalState(t *testing.T) {
	rec := NewRFRecorder(0)
	rfSeed(rec)
	rec.Add(&RFTxn{
		ID: rec.NextID(), Label: "W-LOST",
		ReadTs: rfTs(1), CommitTs: rfTs(2), Committed: true,
		Writes: []RFWrite{{Key: []byte("A"), Value: rfBal(90)}},
	})
	// Database still holds the seed value: the write never landed.
	res := rec.Verify(map[string][]byte{"A": rfBal(100), "B": rfBal(100)})
	if res.OK() {
		t.Fatalf("checker missed a dropped write:\n%s", res.Report(10))
	}
	if res.Violations[0].Kind != RFFinalStateMismatch {
		t.Fatalf("got %s, want final-state-mismatch", res.Violations[0].Kind)
	}
}

// TestReadsFromDuplicateCommitTs: two committed writers sharing a commit
// timestamp means no total order exists, so the reference sequential execution
// is ill-defined and must be reported rather than silently tie-broken.
func TestReadsFromDuplicateCommitTs(t *testing.T) {
	rec := NewRFRecorder(0)
	rec.Add(&RFTxn{
		ID: rec.NextID(), Label: "W1",
		ReadTs: rfTs(1), CommitTs: rfTs(2), Committed: true,
		Writes: []RFWrite{{Key: []byte("A"), Value: rfBal(1)}},
	})
	rec.Add(&RFTxn{
		ID: rec.NextID(), Label: "W2",
		ReadTs: rfTs(1), CommitTs: rfTs(2), Committed: true,
		Writes: []RFWrite{{Key: []byte("A"), Value: rfBal(2)}},
	})
	res := rec.Verify(nil)
	var got bool
	for _, v := range res.Violations {
		if v.Kind == RFDuplicateCommitTs {
			got = true
		}
	}
	if !got {
		t.Fatalf("duplicate commitTs not reported:\n%s", res.Report(10))
	}
}

// TestReadsFromTruncationIsInconclusive: a partial history must never report
// PASS. Dropping a committed write leaves the replay missing a version, so
// "no violations found" would be an artifact, not a result.
func TestReadsFromTruncationIsInconclusive(t *testing.T) {
	rec := NewRFRecorder(1)
	rec.Add(&RFTxn{ID: rec.NextID(), CommitTs: rfTs(2), Committed: true,
		Writes: []RFWrite{{Key: []byte("A"), Value: rfBal(1)}}})
	rec.Add(&RFTxn{ID: rec.NextID(), CommitTs: rfTs(3), Committed: true,
		Writes: []RFWrite{{Key: []byte("A"), Value: rfBal(2)}}})

	res := rec.Verify(nil)
	if res.OK() {
		t.Fatal("truncated history must not report OK")
	}
	if !res.Truncated || res.DroppedTxns != 1 {
		t.Fatalf("Truncated=%v DroppedTxns=%d, want true/1", res.Truncated, res.DroppedTxns)
	}
}

// TestAbortStatsReasonsAreDistinct guards the counter table against a
// copy-paste collision between reason names, which would silently merge two
// abort causes into one bucket and defeat the point of the breakdown.
func TestAbortStatsReasonsAreDistinct(t *testing.T) {
	seen := map[string]AbortReason{}
	for r := AbortReason(0); r < numAbortReasons; r++ {
		name := r.String()
		if name == "" || name == "unknown" {
			t.Errorf("reason %d has no name", int(r))
		}
		if prev, dup := seen[name]; dup {
			t.Errorf("reason %d and %d share the name %q", int(prev), int(r), name)
		}
		seen[name] = r
	}

	ResetAbortStats()
	st := SnapshotAbortStats()
	if st.TotalAborts != 0 {
		t.Errorf("TotalAborts after reset = %d, want 0", st.TotalAborts)
	}
	if len(st.ByReason) != int(numAbortReasons) {
		t.Errorf("ByReason has %d entries, want %d", len(st.ByReason), int(numAbortReasons))
	}

	// conflict-check-skipped and empty-write-set must never inflate the abort
	// total: one is a bypass count, the other returns nil to the caller.
	abortCounters[ReasonConflictCheckSkipped].Add(5)
	abortCounters[ReasonEmptyWriteSet].Add(3)
	abortCounters[ReasonConflictHasConflict].Add(2)
	st = SnapshotAbortStats()
	if st.TotalAborts != 2 {
		t.Errorf("TotalAborts = %d, want 2 (skipped/empty must be excluded)", st.TotalAborts)
	}
	ResetAbortStats()
}
