/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Reads-from equivalence checker.
 *
 * WHAT IS BEING PROVED
 * --------------------
 * For a concurrent execution E, we want: there exists a valid sequential
 * execution S such that (a) every read in E observes the value it would have
 * observed in S, and (b) the final state of E equals the final state of S.
 *
 * S is fixed, not searched for: it is the replay of all committed transactions
 * in commit-timestamp order. That total order is not an arbitrary choice — it
 * is already the order the storage layer uses for MVCC visibility
 * (NewTransactionAt(readTs) exposes exactly the versions with ts <= readTs).
 * Using it as the reference sequential execution is the substitute the
 * professor allowed in place of replaying a Divy-generated dependency DAG, and
 * it is the only ordering this repository actually has: there is no
 * dependency-graph structure here, only linear CustomTs ordering via
 * divytime.Oracle / types.CustomTs.
 *
 * WHY THIS IS A REAL TEST AND NOT A TAUTOLOGY
 * -------------------------------------------
 * A committed read-write transaction T reads at T.readTs but is placed in S at
 * position T.commitTs. Those differ. If some U committed with
 * T.readTs < U.commitTs < T.commitTs and U wrote a key T read, then T's
 * observed value is stale with respect to T's position in S, and check (a)
 * fails. Detecting and aborting exactly that case is the entire job of
 * oracle.hasConflict. So this checker is a direct test of the conflict
 * detector, not a restatement of it.
 *
 * Three independent things are checked per read, weakest to strongest:
 *   - found:   did the read see a value at all, where S says it should?
 *   - value:   are the bytes equal?
 *   - version: is the observed version the commit timestamp of the transaction
 *              that S says produced it? This is the reads-from EDGE itself. It
 *              can fail while value passes — two transactions writing the same
 *              bytes mask a value comparison but not a version comparison.
 *
 * This subsumes the sum-of-balances invariant already checked by
 * verifyBankTotal: a lost update that preserves the total (A-10, B+10 applied
 * against a stale read of A) keeps the sum correct but produces a read-version
 * mismatch here.
 */

package badger

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4/types"
)

// RFRead is one read observed during the concurrent execution.
type RFRead struct {
	Key []byte

	// Found is false when the read returned ErrKeyNotFound.
	Found bool

	// Value is a copy of the bytes observed. Must be a copy: badger item
	// values alias internal buffers that are reused after Discard.
	Value []byte

	// ObservedVersion is item.Version() — the commit timestamp of the
	// transaction whose write the read landed on. This is the reads-from edge,
	// recoverable directly because Get already returns the version it saw.
	// Zero when !Found.
	ObservedVersion types.CustomTs
}

// RFWrite is one write staged by a transaction that went on to commit.
type RFWrite struct {
	Key     []byte
	Value   []byte
	Deleted bool
}

// RFTxn is the recorded read set and write set of a single transaction.
type RFTxn struct {
	// ID is a recorder-assigned monotonic id, used only for tie-breaking and
	// for naming a transaction in a violation report.
	ID uint64

	ReadTs   types.CustomTs
	CommitTs types.CustomTs

	// Committed is true only if CommitAt/Commit returned nil. Aborted and
	// retried attempts must be recorded with Committed=false so they are
	// excluded from the reference sequential execution but still available for
	// diagnosis.
	Committed bool

	// Label is free-form, e.g. "TRANSFER" or "READ_ONLY", for report legibility.
	Label string

	Reads  []RFRead
	Writes []RFWrite
}

// isReadOnly reports whether the txn produced no versions. Read-only txns are
// placed in the reference execution by ReadTs rather than CommitTs.
func (t *RFTxn) isReadOnly() bool { return len(t.Writes) == 0 || !t.Committed }

// RFRecorder collects transaction histories from a concurrent workload.
//
// Recording is opt-in and bounded. An unbounded recorder would change the
// thing it measures (allocation pressure on the commit path) and would OOM at
// the 10M/40M/100M cardinalities this fork also benchmarks. When the cap is
// hit, recording stops and Verify reports the history as TRUNCATED — it must
// never silently validate a prefix and return "PASS".
type RFRecorder struct {
	mu      sync.Mutex
	txns    []*RFTxn
	nextID  atomic.Uint64
	maxTxns int

	dropped atomic.Int64
}

// NewRFRecorder returns a recorder that retains at most maxTxns transactions.
// maxTxns <= 0 means unbounded (only safe for small, fixed workloads).
func NewRFRecorder(maxTxns int) *RFRecorder {
	return &RFRecorder{maxTxns: maxTxns}
}

// NextID hands out a transaction id. Call once per attempt, not once per retry
// loop, so a retried transfer appears as several distinct attempts.
func (r *RFRecorder) NextID() uint64 { return r.nextID.Add(1) }

// Add appends a completed transaction record. Safe for concurrent use.
func (r *RFRecorder) Add(t *RFTxn) {
	if r == nil || t == nil {
		return
	}
	r.mu.Lock()
	if r.maxTxns > 0 && len(r.txns) >= r.maxTxns {
		r.mu.Unlock()
		r.dropped.Add(1)
		return
	}
	r.txns = append(r.txns, t)
	r.mu.Unlock()
}

// Dropped returns the number of transactions discarded because the cap was hit.
func (r *RFRecorder) Dropped() int64 { return r.dropped.Load() }

// Len returns the number of retained transactions.
func (r *RFRecorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.txns)
}

// RFViolationKind classifies a counterexample.
type RFViolationKind string

const (
	// A read observed different bytes than the sequential replay produces.
	RFReadValueMismatch RFViolationKind = "read-value-mismatch"

	// A read found a value where the replay says there is none, or vice versa.
	RFReadFoundMismatch RFViolationKind = "read-found-mismatch"

	// The read landed on the right bytes but the wrong version — it read from
	// a different transaction than the sequential order says it should. This
	// is the reads-from edge violation proper.
	RFReadVersionMismatch RFViolationKind = "read-version-mismatch"

	// Two committed transactions share a commit timestamp, so no total order
	// exists and the reference sequential execution is ill-defined.
	RFDuplicateCommitTs RFViolationKind = "duplicate-commit-ts"

	// A committed transaction's commitTs is not strictly greater than its
	// readTs — the transaction would be ordered before the snapshot it read.
	RFCommitTsNotAfterReadTs RFViolationKind = "commit-ts-not-after-read-ts"

	// The database's final state disagrees with the replay's final state.
	RFFinalStateMismatch RFViolationKind = "final-state-mismatch"
)

// RFViolation is one concrete, actionable counterexample.
type RFViolation struct {
	Kind  RFViolationKind
	TxnID uint64
	Label string
	Key   []byte

	ReadTs   types.CustomTs
	CommitTs types.CustomTs

	// Observed* is what the concurrent execution actually saw.
	ObservedFound   bool
	ObservedValue   []byte
	ObservedVersion types.CustomTs

	// Expected* is what the sequential replay says it should have seen.
	ExpectedFound   bool
	ExpectedValue   []byte
	ExpectedVersion types.CustomTs

	// ExpectedFromTxn is the id of the transaction the replay says produced
	// ExpectedValue, when known.
	ExpectedFromTxn uint64

	Detail string
}

func (v RFViolation) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "[%s] txn=%d", v.Kind, v.TxnID)
	if v.Label != "" {
		fmt.Fprintf(&sb, " (%s)", v.Label)
	}
	if len(v.Key) > 0 {
		fmt.Fprintf(&sb, " key=%q", string(v.Key))
	}
	fmt.Fprintf(&sb, " readTs=%s commitTs=%s", v.ReadTs.String(), v.CommitTs.String())

	switch v.Kind {
	case RFReadFoundMismatch:
		fmt.Fprintf(&sb, " observedFound=%v expectedFound=%v", v.ObservedFound, v.ExpectedFound)
	case RFReadValueMismatch:
		fmt.Fprintf(&sb, " observed=%x expected=%x (expected written by txn=%d at %s)",
			v.ObservedValue, v.ExpectedValue, v.ExpectedFromTxn, v.ExpectedVersion.String())
	case RFReadVersionMismatch:
		fmt.Fprintf(&sb, " observedVersion=%s expectedVersion=%s (expected written by txn=%d)",
			v.ObservedVersion.String(), v.ExpectedVersion.String(), v.ExpectedFromTxn)
	case RFFinalStateMismatch:
		fmt.Fprintf(&sb, " dbFound=%v db=%x replayFound=%v replay=%x",
			v.ObservedFound, v.ObservedValue, v.ExpectedFound, v.ExpectedValue)
	}
	if v.Detail != "" {
		fmt.Fprintf(&sb, " detail=%s", v.Detail)
	}
	return sb.String()
}

// RFResult is the outcome of a verification pass.
type RFResult struct {
	Violations []RFViolation

	TotalTxns     int
	CommittedTxns int
	ReadOnlyTxns  int
	AbortedTxns   int
	ReadsChecked  int
	KeysInReplay  int

	// Truncated is true when the recorder dropped transactions. A truncated
	// history cannot be verified: an unrecorded committed write means the
	// replay is missing a version, and every subsequent read of that key would
	// be reported as a mismatch (or, worse, a real violation would be masked).
	Truncated      bool
	DroppedTxns    int64
	FinalStateSeen bool
}

// OK reports whether the execution was verified equivalent to the sequential
// replay. A truncated history is never OK.
func (r RFResult) OK() bool { return len(r.Violations) == 0 && !r.Truncated }

// Report renders a human-readable summary, capping the violation list so a
// systematic failure does not produce megabytes of log.
func (r RFResult) Report(maxViolations int) string {
	var sb strings.Builder
	sb.WriteString("reads-from equivalence check\n")
	fmt.Fprintf(&sb, "  transactions recorded : %d (committed=%d read-only=%d aborted=%d)\n",
		r.TotalTxns, r.CommittedTxns, r.ReadOnlyTxns, r.AbortedTxns)
	fmt.Fprintf(&sb, "  reads checked         : %d\n", r.ReadsChecked)
	fmt.Fprintf(&sb, "  keys in replay        : %d\n", r.KeysInReplay)
	fmt.Fprintf(&sb, "  final state compared  : %v\n", r.FinalStateSeen)

	if r.Truncated {
		fmt.Fprintf(&sb, "  RESULT                : INCONCLUSIVE — history truncated, %d txns dropped\n", r.DroppedTxns)
		sb.WriteString("  (raise the recorder cap or shorten the run; a partial history cannot be verified)\n")
		return sb.String()
	}

	if len(r.Violations) == 0 {
		sb.WriteString("  RESULT                : PASS — every read and the final state match the\n")
		sb.WriteString("                          commit-timestamp-ordered sequential replay\n")
		return sb.String()
	}

	fmt.Fprintf(&sb, "  RESULT                : FAIL — %d violation(s)\n", len(r.Violations))
	counts := map[RFViolationKind]int{}
	for _, v := range r.Violations {
		counts[v.Kind]++
	}
	kinds := make([]string, 0, len(counts))
	for k := range counts {
		kinds = append(kinds, string(k))
	}
	sort.Strings(kinds)
	for _, k := range kinds {
		fmt.Fprintf(&sb, "    %-28s %d\n", k, counts[RFViolationKind(k)])
	}

	n := len(r.Violations)
	if maxViolations > 0 && n > maxViolations {
		n = maxViolations
	}
	sb.WriteString("  counterexamples:\n")
	for _, v := range r.Violations[:n] {
		fmt.Fprintf(&sb, "    %s\n", v.String())
	}
	if n < len(r.Violations) {
		fmt.Fprintf(&sb, "    ... and %d more\n", len(r.Violations)-n)
	}
	return sb.String()
}

// modelVal is a cell in the sequential replay's state.
type modelVal struct {
	value   []byte
	version types.CustomTs
	fromTxn uint64
	present bool
}

// rfEvent is a position in the reference sequential execution.
type rfEvent struct {
	ts types.CustomTs

	// phase orders events that share a timestamp. Read-write transactions
	// (phase 0) are applied before read-only transactions (phase 1) observe
	// that timestamp, because MVCC visibility here is version <= readTs: a
	// snapshot at readTs X does see a commit at exactly X.
	phase int

	txn *RFTxn
}

// Verify replays the recorded committed transactions in commit-timestamp order
// and checks every recorded read, plus the final state.
//
// finalState may be nil to skip the final-state comparison. When supplied it
// maps key -> value for every key present in the database at the end of the
// run; keys absent from the map are treated as not present.
func (r *RFRecorder) Verify(finalState map[string][]byte) RFResult {
	r.mu.Lock()
	txns := make([]*RFTxn, len(r.txns))
	copy(txns, r.txns)
	r.mu.Unlock()

	res := RFResult{
		TotalTxns:      len(txns),
		DroppedTxns:    r.Dropped(),
		FinalStateSeen: finalState != nil,
	}
	res.Truncated = res.DroppedTxns > 0

	// ---- build the event list -------------------------------------------
	events := make([]rfEvent, 0, len(txns))
	for _, t := range txns {
		switch {
		case !t.Committed:
			res.AbortedTxns++
			// Aborted attempts are excluded from the reference execution.
			// Their reads are also not checked: an aborted transaction is
			// permitted to have observed an inconsistent snapshot — that is
			// precisely why it aborted.
			continue
		case t.isReadOnly():
			res.ReadOnlyTxns++
			events = append(events, rfEvent{ts: t.ReadTs, phase: 1, txn: t})
		default:
			res.CommittedTxns++
			// Only meaningful for transactions that actually read something.
			// A blind write (e.g. the account seeding loop, which reuses one
			// timestamp for both readTs and commitTs) has no snapshot it needs
			// to be ordered after.
			if len(t.Reads) > 0 && !t.CommitTs.Greater(t.ReadTs) {
				res.Violations = append(res.Violations, RFViolation{
					Kind:     RFCommitTsNotAfterReadTs,
					TxnID:    t.ID,
					Label:    t.Label,
					ReadTs:   t.ReadTs,
					CommitTs: t.CommitTs,
					Detail:   "commit would be ordered at or before the snapshot it read",
				})
			}
			events = append(events, rfEvent{ts: t.CommitTs, phase: 0, txn: t})
		}
	}

	sort.SliceStable(events, func(i, j int) bool {
		a, b := events[i], events[j]
		if a.ts.Less(b.ts) {
			return true
		}
		if b.ts.Less(a.ts) {
			return false
		}
		if a.phase != b.phase {
			return a.phase < b.phase
		}
		return a.txn.ID < b.txn.ID
	})

	// ---- replay ----------------------------------------------------------
	model := make(map[string]modelVal)
	seenCommitTs := make(map[string]uint64)

	for _, ev := range events {
		t := ev.txn

		if ev.phase == 0 {
			key := t.CommitTs.String()
			if prev, dup := seenCommitTs[key]; dup {
				res.Violations = append(res.Violations, RFViolation{
					Kind:     RFDuplicateCommitTs,
					TxnID:    t.ID,
					Label:    t.Label,
					ReadTs:   t.ReadTs,
					CommitTs: t.CommitTs,
					Detail:   fmt.Sprintf("also used by txn=%d; no total order exists", prev),
				})
			} else {
				seenCommitTs[key] = t.ID
			}
		}

		// Check this transaction's reads against the model state at its
		// position in the sequential order — i.e. before applying its own
		// writes.
		for _, rd := range t.Reads {
			res.ReadsChecked++
			want := model[string(rd.Key)]

			if rd.Found != want.present {
				res.Violations = append(res.Violations, RFViolation{
					Kind:            RFReadFoundMismatch,
					TxnID:           t.ID,
					Label:           t.Label,
					Key:             rd.Key,
					ReadTs:          t.ReadTs,
					CommitTs:        t.CommitTs,
					ObservedFound:   rd.Found,
					ObservedValue:   rd.Value,
					ObservedVersion: rd.ObservedVersion,
					ExpectedFound:   want.present,
					ExpectedValue:   want.value,
					ExpectedVersion: want.version,
					ExpectedFromTxn: want.fromTxn,
				})
				continue
			}
			if !rd.Found {
				continue
			}
			if !bytes.Equal(rd.Value, want.value) {
				res.Violations = append(res.Violations, RFViolation{
					Kind:            RFReadValueMismatch,
					TxnID:           t.ID,
					Label:           t.Label,
					Key:             rd.Key,
					ReadTs:          t.ReadTs,
					CommitTs:        t.CommitTs,
					ObservedFound:   true,
					ObservedValue:   rd.Value,
					ObservedVersion: rd.ObservedVersion,
					ExpectedFound:   true,
					ExpectedValue:   want.value,
					ExpectedVersion: want.version,
					ExpectedFromTxn: want.fromTxn,
				})
				continue
			}
			// Value matched. The version check is strictly stronger: equal
			// bytes written by two different transactions still means the read
			// took its value from the wrong place in the order.
			if !rd.ObservedVersion.Equal(want.version) {
				res.Violations = append(res.Violations, RFViolation{
					Kind:            RFReadVersionMismatch,
					TxnID:           t.ID,
					Label:           t.Label,
					Key:             rd.Key,
					ReadTs:          t.ReadTs,
					CommitTs:        t.CommitTs,
					ObservedFound:   true,
					ObservedValue:   rd.Value,
					ObservedVersion: rd.ObservedVersion,
					ExpectedFound:   true,
					ExpectedValue:   want.value,
					ExpectedVersion: want.version,
					ExpectedFromTxn: want.fromTxn,
				})
			}
		}

		if ev.phase != 0 {
			continue
		}
		for _, w := range t.Writes {
			if w.Deleted {
				model[string(w.Key)] = modelVal{present: false, version: t.CommitTs, fromTxn: t.ID}
				continue
			}
			buf := make([]byte, len(w.Value))
			copy(buf, w.Value)
			model[string(w.Key)] = modelVal{
				value:   buf,
				version: t.CommitTs,
				fromTxn: t.ID,
				present: true,
			}
		}
	}

	res.KeysInReplay = len(model)

	// ---- final state -----------------------------------------------------
	if finalState != nil {
		checked := make(map[string]struct{}, len(model))
		for k, want := range model {
			checked[k] = struct{}{}
			got, ok := finalState[k]
			if ok != want.present || (want.present && !bytes.Equal(got, want.value)) {
				res.Violations = append(res.Violations, RFViolation{
					Kind:            RFFinalStateMismatch,
					Key:             []byte(k),
					ObservedFound:   ok,
					ObservedValue:   got,
					ExpectedFound:   want.present,
					ExpectedValue:   want.value,
					ExpectedVersion: want.version,
					ExpectedFromTxn: want.fromTxn,
				})
			}
		}
		for k, got := range finalState {
			if _, done := checked[k]; done {
				continue
			}
			res.Violations = append(res.Violations, RFViolation{
				Kind:          RFFinalStateMismatch,
				Key:           []byte(k),
				ObservedFound: true,
				ObservedValue: got,
				ExpectedFound: false,
				Detail:        "key present in database but never written by any recorded committed txn",
			})
		}
	}

	return res
}
