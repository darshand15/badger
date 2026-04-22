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

// Oracle issues monotonically-increasing Timestamp values.
// A configurable SimulatedDelay adds artificial latency so benchmarks can
// model the cost of a remote ordering service.
type Oracle struct {
	brokerID       int64
	simulatedDelay time.Duration
	counter        int64 // atomic; incremented on every GetTimestamp call

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

// GetTimestamp returns a unique Timestamp for the given epochID and the
// wall-clock latency of the call (useful for percentile reporting).
func (o *Oracle) GetTimestamp(epochID int64) (Timestamp, time.Duration) {
	start := time.Now()

	if o.simulatedDelay > 0 {
		time.Sleep(o.simulatedDelay)
	}

	assigned := atomic.AddInt64(&o.counter, 1)
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
