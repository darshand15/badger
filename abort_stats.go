/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Transaction abort instrumentation.
 *
 * WHY THIS FILE EXISTS
 * --------------------
 * Before this instrumentation, a benchmark run could report "N aborts" but not
 * *why*. Worse, the two configurations this fork runs under disagree about
 * which abort paths are even reachable:
 *
 *   1. The bank/smallbank tests here open with DefaultOptions, so
 *      DetectConflicts == true. oracle.newCommitTs takes the slow path,
 *      hasConflict runs, and ErrConflict is reachable.
 *
 *   2. The deployed server (lock-free-machine services/server/main.go) opens
 *      with OpenManaged + WithDetectConflicts(false). newCommitTs then takes
 *      the fast path at the top of the function and returns before
 *      hasConflict is ever called. In that configuration a badger-level
 *      conflict abort is *structurally impossible* — the transaction failures
 *      observed there come from somewhere else (typically a read that finds no
 *      visible version, surfacing as ErrKeyNotFound out of txn.Get).
 *
 * A single scalar "abort count" cannot distinguish those. The counters below
 * are therefore keyed by reason, and deliberately include
 * ReasonConflictCheckSkipped — a count of commits that bypassed conflict
 * detection entirely. If that counter is large while ReasonConflictHasConflict
 * is zero, the run had no conflict detection to speak of, and any serializability
 * claim about that run must rest on the external timestamp order rather than on
 * badger's own conflict check.
 */

package badger

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4/types"
)

// AbortReason identifies a distinct reason a transaction failed to commit.
type AbortReason int

const (
	// ReasonConflictHasConflict: oracle.hasConflict found a key in this txn's
	// read set that was written by a transaction which committed in the window
	// (txn.readTs, now]. This is the classic read-write conflict abort.
	ReasonConflictHasConflict AbortReason = iota

	// ReasonPrecheckFailure: commitPrecheck rejected the txn (already
	// discarded, or managed mode with a zero commitTs).
	ReasonPrecheckFailure

	// ReasonEmptyWriteSet: Commit() was called with no pending writes. Not an
	// error for the caller, but it is a commit that produced no version, so it
	// must be excluded from a reads-from replay.
	ReasonEmptyWriteSet

	// ReasonDuckDBFlushError: the commit timestamp was issued and the conflict
	// check passed, but DirectFlush into the DuckDB appender failed. Unlike a
	// conflict this is NOT safe to retry blindly — doneCommit already ran, so
	// the watermark advanced past a commit that never landed.
	ReasonDuckDBFlushError

	// ReasonWriteChError: sendToWriteCh failed (blocked writes / closed DB).
	ReasonWriteChError

	// ReasonCommitWaitError: the write request was accepted but req.Wait()
	// returned an error, so the commit did not durably land.
	ReasonCommitWaitError

	// ReasonConflictCheckSkipped is NOT an abort. It counts commits that took
	// the managed && !detectConflicts fast path in newCommitTs and therefore
	// never ran hasConflict. See the file comment: this is the counter that
	// tells you whether a zero conflict-abort count means "no conflicts" or
	// "never looked".
	ReasonConflictCheckSkipped

	numAbortReasons
)

var abortReasonNames = [numAbortReasons]string{
	ReasonConflictHasConflict:  "conflict-hasConflict",
	ReasonPrecheckFailure:      "precheck-failure",
	ReasonEmptyWriteSet:        "empty-write-set",
	ReasonDuckDBFlushError:     "duckdb-flush-error",
	ReasonWriteChError:         "write-ch-error",
	ReasonCommitWaitError:      "commit-wait-error",
	ReasonConflictCheckSkipped: "conflict-check-skipped",
}

// String returns the stable log/report name for the reason.
func (r AbortReason) String() string {
	if r < 0 || r >= numAbortReasons {
		return "unknown"
	}
	return abortReasonNames[r]
}

// abortCounters is package-level rather than per-DB on purpose. It mirrors the
// existing precedent in y/metrics.go (numCASSuccesses et al.), keeps the DB
// struct untouched, and costs one relaxed atomic add on the abort path — which
// is already the slow path. Tests that need isolation call ResetAbortStats.
var abortCounters [numAbortReasons]atomic.Int64

// abortTraceEnabled gates the per-abort structured log line. Counters are
// always on (a single atomic add); the log line is opt-in because at high abort
// rates it dominates the measurement it is supposed to explain.
//
// Enable with BADGER_ABORT_TRACE=1.
var abortTraceEnabled = os.Getenv("BADGER_ABORT_TRACE") == "1"

// abortEvent carries the identifying detail for one abort. Fields not
// applicable to a given reason are left zero.
type abortEvent struct {
	reason   AbortReason
	readTs   types.CustomTs
	commitTs types.CustomTs

	// conflictFP is the fingerprint (Z.MemHash of the key) of the read that
	// collided. txn.reads stores fingerprints, not keys, so the original key
	// bytes are genuinely not recoverable here — reporting the fingerprint is
	// the most specific attribution the existing data structure allows.
	conflictFP uint64

	// conflictWithTs is the commit timestamp of the already-committed
	// transaction whose write set contained conflictFP.
	conflictWithTs types.CustomTs

	// err is the underlying error for the I/O-shaped reasons.
	err error
}

// recordAbort bumps the counter for reason and, when tracing is enabled, emits
// one structured line. logger may be nil (Options.Logger is nil in the bench
// harness), in which case the line goes to stderr so it is still capturable.
func recordAbort(logger Logger, ev abortEvent) {
	if ev.reason < 0 || ev.reason >= numAbortReasons {
		return
	}
	abortCounters[ev.reason].Add(1)

	if !abortTraceEnabled {
		return
	}

	var sb strings.Builder
	sb.WriteString("badger.abort reason=")
	sb.WriteString(ev.reason.String())
	sb.WriteString(" readTs=")
	sb.WriteString(ev.readTs.String())
	sb.WriteString(" commitTs=")
	sb.WriteString(ev.commitTs.String())
	if ev.reason == ReasonConflictHasConflict {
		fmt.Fprintf(&sb, " conflictKeyFP=%d conflictWithCommitTs=%s",
			ev.conflictFP, ev.conflictWithTs.String())
	}
	if ev.err != nil {
		fmt.Fprintf(&sb, " err=%q", ev.err.Error())
	}

	line := sb.String()
	if logger != nil {
		logger.Warningf("%s", line)
		return
	}
	fmt.Fprintln(os.Stderr, line)
}

// AbortStats is a point-in-time snapshot of the abort counters.
type AbortStats struct {
	// ByReason maps reason name -> count. Reasons with a zero count are
	// included, because "this abort path was never taken" is itself the
	// finding in several of the configurations this fork runs under.
	ByReason map[string]int64

	// TotalAborts is the sum over genuine abort reasons. It deliberately
	// EXCLUDES ReasonConflictCheckSkipped (not an abort) and
	// ReasonEmptyWriteSet (not a failure — the caller's Commit returned nil).
	TotalAborts int64
}

// isRealAbort reports whether a reason should be summed into TotalAborts.
func isRealAbort(r AbortReason) bool {
	switch r {
	case ReasonConflictCheckSkipped, ReasonEmptyWriteSet:
		return false
	default:
		return true
	}
}

// SnapshotAbortStats returns the current counters. Safe to call concurrently
// with a running workload; the counts are individually atomic but not a
// consistent cut across reasons.
func SnapshotAbortStats() AbortStats {
	st := AbortStats{ByReason: make(map[string]int64, numAbortReasons)}
	for r := AbortReason(0); r < numAbortReasons; r++ {
		n := abortCounters[r].Load()
		st.ByReason[r.String()] = n
		if isRealAbort(r) {
			st.TotalAborts += n
		}
	}
	return st
}

// ResetAbortStats zeroes every counter. Call at the start of a test or
// benchmark iteration so counts attribute to that run only.
func ResetAbortStats() {
	for r := AbortReason(0); r < numAbortReasons; r++ {
		abortCounters[r].Store(0)
	}
}

// Report renders the breakdown as a stable, sorted, multi-line table suitable
// for t.Logf / b.Log next to a TPS number.
func (s AbortStats) Report() string {
	names := make([]string, 0, len(s.ByReason))
	for name := range s.ByReason {
		names = append(names, name)
	}
	sort.Strings(names)

	width := 0
	for _, name := range names {
		if len(name) > width {
			width = len(name)
		}
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "abort breakdown (total real aborts=%d):\n", s.TotalAborts)
	for _, name := range names {
		fmt.Fprintf(&sb, "  %-*s  %d", width, name, s.ByReason[name])
		switch name {
		case abortReasonNames[ReasonConflictCheckSkipped]:
			sb.WriteString("   (not an abort: commits that never ran hasConflict)")
		case abortReasonNames[ReasonEmptyWriteSet]:
			sb.WriteString("   (not an abort: Commit returned nil, no version produced)")
		}
		sb.WriteString("\n")
	}
	return sb.String()
}
