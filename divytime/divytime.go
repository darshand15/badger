// Package divytime provides a simulated 3-tuple timestamp oracle for testing
// and benchmarking DuckDB-backed Badger workloads.
//
// In production the ordering service ("Divy") issues (EpochID, BrokerID,
// AssignedTs) tuples with a network round-trip; this package replaces it with
// a pure in-process monotonic counter so unit tests and benchmarks are fully
// self-contained.
package divytime

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Timestamp is a 3-tuple ordering token issued by the oracle.
type Timestamp struct {
	EpochID    int64
	BrokerID   int64
	AssignedTs int64
}

// TimestampOracle is the interface every timestamp source (simulated or real)
// must satisfy. Extracting this interface lets callers depend on "a source of
// ordered timestamps" rather than on the concrete in-process Oracle type, so
// a future real Divy broker client can be swapped in without touching call
// sites.
//
// Both methods are satisfied by *Oracle (aliased below as *LocalOracle)
// without modification, and are intended to be satisfied by a future
// *RemoteOracle once it is wired up to a real broker connection.
type TimestampOracle interface {
	// GetTimestamp returns a unique Timestamp and the call's wall-clock
	// latency. The epochIDHint parameter is accepted for interface
	// compatibility with existing call sites but may be ignored by the
	// implementation (LocalOracle ignores it; see GetTimestamp below).
	GetTimestamp(epochIDHint int64) (Timestamp, time.Duration)

	// GetCommitTimestamp is like GetTimestamp but additionally invokes
	// register(ts) atomically with timestamp issuance -- see the
	// GetCommitTimestamp doc comment on Oracle for why this matters.
	GetCommitTimestamp(register func(Timestamp)) (Timestamp, time.Duration)
}

// Oracle issues monotonically-increasing Timestamp values.
// A configurable SimulatedDelay adds artificial latency so benchmarks can
// model the cost of a remote ordering service.
type Oracle struct {
	brokerID       int64
	simulatedDelay time.Duration
	counter        int64 // atomic; incremented on every GetTimestamp call

	// issueMu serialises GetTimestamp calls when simulatedDelay > 0.
	//
	// Rationale: a real distributed oracle (e.g. Divy) is backed by a single
	// service whose replies arrive in the order the requests were processed.
	// When multiple goroutines concurrently sleep for simulatedDelay and then
	// race to increment the counter, the counter-assignment order is random with
	// respect to the call-site order.  This lets a transaction C obtain a
	// commitTs that is numerically less than B's readTs even though C called the
	// oracle after B did — a situation that is impossible with a real serialised
	// oracle but that breaks MVCC snapshot isolation here (conflict detection
	// only fires for commitTs > readTs, so C's out-of-order write goes
	// undetected and produces an inconsistent snapshot).
	//
	// By holding issueMu for the full sleep+increment window we restore the
	// real-oracle invariant: call order == counter-assignment order.  Calls
	// still take simulatedDelay each (modelling latency), they simply do not
	// overlap.
	issueMu sync.Mutex

	mu      sync.Mutex
	samples []int64 // nanosecond latencies, appended under mu
}

// NewOracle creates an Oracle that issues timestamps with the given brokerID.
// If simulatedDelay > 0 each call to GetTimestamp sleeps for that duration to
// model a remote oracle round-trip.
func NewOracle(brokerID int64, simulatedDelay time.Duration) *Oracle {
	return &Oracle{
		brokerID:       brokerID,
		simulatedDelay: simulatedDelay,
	}
}

// GetTimestamp returns a unique Timestamp and the wall-clock latency of the
// call (useful for percentile reporting).  The caller-supplied epochID hint is
// ignored; the actual EpochID is captured inside issueMu so its order always
// matches the AssignedTs counter order.
//
// Why epochID must be captured inside the lock
// --------------------------------------------
// A real distributed oracle (e.g. Divy) is a single service: requests are
// processed in arrival order, so EpochID (wall-clock time at the service) is
// monotonically non-decreasing in the same order as AssignedTs.
//
// Without the lock, two goroutines can read time.Now() in one order but then
// race on atomic.Add and receive counter values in the opposite order, yielding
// an inverted 3-tuple (B.EpochID < A.EpochID yet B.AssignedTs > A.AssignedTs).
// This breaks MVCC snapshot isolation: conflict detection only fires for
// commitTs > readTs, so an inverted commit is silently accepted and money is
// created or destroyed in the bank invariant test.
//
// By always holding issueMu and reading the clock after acquiring it we
// guarantee call N gets (EpochID_N, AssignedTs_N) with both fields
// monotonically non-decreasing relative to call N+1.
func (o *Oracle) GetTimestamp(_ int64) (Timestamp, time.Duration) {
	start := time.Now()

	o.issueMu.Lock()
	if o.simulatedDelay > 0 {
		time.Sleep(o.simulatedDelay)
	}
	// Capture epoch AFTER acquiring the lock so EpochID order matches
	// lock-acquisition (and therefore counter-assignment) order.
	epochID := time.Now().UnixNano()
	assigned := atomic.AddInt64(&o.counter, 1)
	o.issueMu.Unlock()

	elapsed := time.Since(start)

	o.mu.Lock()
	o.samples = append(o.samples, int64(elapsed))
	o.mu.Unlock()

	return Timestamp{
		EpochID:    epochID,
		BrokerID:   o.brokerID,
		AssignedTs: assigned,
	}, elapsed
}

// GetCommitTimestamp is like GetTimestamp but additionally invokes register(ts)
// while still holding issueMu, i.e. atomically with timestamp issuance.
//
// Why this exists
// ---------------
// A commit timestamp is useless to conflict detection until the rest of the
// system knows it is in flight.  If issuance and registration are two separate
// steps, there is a window in which the timestamp exists but is invisible:
//
//	C: GetTimestamp() -> commitTs=100        (issued, NOT yet registered)
//	B: GetTimestamp() -> readTs=105
//	B: NewTransactionAt(105)                 sees no pending commit <= 105, reads stale data
//	C: CommitAt(100)                         registers + flushes; hasConflict skips
//	                                         C for B because 100 <= B.readTs
//	B: CommitAt(110)                         stale read-modify-write commits -> lost update
//
// By running register(ts) before issueMu is released, every timestamp issued
// later (in particular any readTs) is guaranteed to observe the pending commit,
// closing the window completely.
//
// Contract: the caller MUST eventually deregister ts (in Badger this happens
// via doneCommit / the abort paths of CommitAt), otherwise readers at
// readTs >= ts block forever.
func (o *Oracle) GetCommitTimestamp(register func(Timestamp)) (Timestamp, time.Duration) {
	start := time.Now()

	o.issueMu.Lock()
	if o.simulatedDelay > 0 {
		time.Sleep(o.simulatedDelay)
	}
	epochID := time.Now().UnixNano()
	assigned := atomic.AddInt64(&o.counter, 1)
	ts := Timestamp{
		EpochID:    epochID,
		BrokerID:   o.brokerID,
		AssignedTs: assigned,
	}
	if register != nil {
		register(ts)
	}
	o.issueMu.Unlock()

	elapsed := time.Since(start)

	o.mu.Lock()
	o.samples = append(o.samples, int64(elapsed))
	o.mu.Unlock()

	return ts, elapsed
}

// Stats summarises oracle call latency.
type Stats struct {
	Count   int64
	AvgNs   int64
	P90Ns   int64
	MinNs   int64
	MaxNs   int64
	TotalNs int64
}

// Snapshot returns a point-in-time latency summary for all GetTimestamp calls
// made so far.  The internal sample slice is copied so the oracle can continue
// operating concurrently.
func (o *Oracle) Snapshot() Stats {
	o.mu.Lock()
	raw := make([]int64, len(o.samples))
	copy(raw, o.samples)
	o.mu.Unlock()

	if len(raw) == 0 {
		return Stats{}
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

	return Stats{
		Count:   int64(len(raw)),
		AvgNs:   total / int64(len(raw)),
		P90Ns:   raw[p90idx],
		MinNs:   raw[0],
		MaxNs:   raw[len(raw)-1],
		TotalNs: total,
	}
}

// LocalOracle is an alias for Oracle, naming it explicitly as the in-process
// ("local") implementation of TimestampOracle -- as opposed to RemoteOracle,
// which will talk to a real out-of-process Divy broker once one exists. It is
// a plain type alias (not a wrapper), so every existing NewOracle/*Oracle
// call site continues to compile unchanged; new code can spell either
// divytime.Oracle or divytime.LocalOracle interchangeably.
type LocalOracle = Oracle

// NewLocalOracle is an alias for NewOracle, provided for symmetry with
// NewRemoteOracle below.
func NewLocalOracle(brokerID int64, simulatedDelay time.Duration) *LocalOracle {
	return NewOracle(brokerID, simulatedDelay)
}

// RemoteOracle is a placeholder for a real Divy broker client. It implements
// TimestampOracle so production code can already be written against the
// interface, but it is not yet wired up to an actual network client --
// GetTimestamp/GetCommitTimestamp panic today rather than silently returning
// meaningless timestamps. Replacing the panics with a real gRPC/HTTP client
// to a running Divy broker is tracked as follow-up work; see the DuckDB
// project open-items list ("production Divy oracle integration").
type RemoteOracle struct {
	// Addr is the address of the remote Divy broker (e.g. "divy-broker:9090").
	// Stored for when a real client is implemented; unused today.
	Addr string
}

// NewRemoteOracle constructs a RemoteOracle pointed at addr. The returned
// value satisfies TimestampOracle but every call currently panics -- see the
// RemoteOracle doc comment.
func NewRemoteOracle(addr string) *RemoteOracle {
	return &RemoteOracle{Addr: addr}
}

// GetTimestamp is not yet implemented; see the RemoteOracle doc comment.
func (r *RemoteOracle) GetTimestamp(_ int64) (Timestamp, time.Duration) {
	panic("divytime: RemoteOracle is not yet connected to a real Divy broker (addr=" + r.Addr + ")")
}

// GetCommitTimestamp is not yet implemented; see the RemoteOracle doc comment.
func (r *RemoteOracle) GetCommitTimestamp(_ func(Timestamp)) (Timestamp, time.Duration) {
	panic("divytime: RemoteOracle is not yet connected to a real Divy broker (addr=" + r.Addr + ")")
}

// Compile-time assertions that both implementations satisfy TimestampOracle.
var (
	_ TimestampOracle = (*LocalOracle)(nil)
	_ TimestampOracle = (*RemoteOracle)(nil)
)
