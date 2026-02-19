package storage

import (
	"github.com/dgraph-io/ristretto/v2/z"
)

// PartitionCalculator handles dynamic partition ID calculation
// using the same hashing logic as Darshan's Badger implementation
type PartitionCalculator struct {
	partitionCount int
}

// NewPartitionCalculator creates a partition calculator with the given count
func NewPartitionCalculator(partitionCount int) *PartitionCalculator {
	if partitionCount <= 0 {
		partitionCount = 1 // Default fallback
	}
	return &PartitionCalculator{
		partitionCount: partitionCount,
	}
}

// GetPartitionID calculates which partition a key belongs to
// This MUST match Darshan's logic in levels.go
func (pc *PartitionCalculator) GetPartitionID(key []byte) int {
	// Don't strip bytes here — callers must pass the logical key (no ts suffix).
	// Hash the provided key directly.
	hashValue := z.MemHash(key)

	pid := int(hashValue % uint64(pc.partitionCount))

	return pid
}

// GetPartitionCount returns the configured number of partitions
func (pc *PartitionCalculator) GetPartitionCount() int {
	return pc.partitionCount
}
