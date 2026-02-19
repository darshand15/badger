package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4/duckdb-lsm/pkg/types"
	"github.com/dgraph-io/badger/v4/y"
)

type DarshanEntry struct {
	Key     []byte
	Value   []byte
	Version uint64
}

// ConvertDarshanEntry converts Darshan's Entry to our BadgerEntry format
func ConvertDarshanEntry(entry *DarshanEntry) types.BadgerEntry {
	// Store logical key (strip timestamp) so reads by logical key succeed.
	logicalKey := y.ParseKey(entry.Key)
	return types.BadgerEntry{
		Key:       logicalKey,
		Value:     entry.Value,
		Timestamp: DecodeVersion(entry.Version),
	}
}

// DecodeVersion converts Darshan's uint64 version to our 3-part timestamp
func DecodeVersion(version uint64) types.CustomTs {
	// Current: Simple mapping
	return types.CustomTs{
		EpochID:    int64(version),
		BrokerID:   0,
		AssignedTs: 0,
	}
}

// EncodeVersion converts our 3-part timestamp to Darshan's uint64 format
func EncodeVersion(ts types.CustomTs) uint64 {
	return uint64(ts.EpochID)
}

// FlushDarshanEntries receives entries from Darshan's memtable flush
func (s *DuckDBStorage) FlushDarshanEntries(entries []*DarshanEntry) error {
	if len(entries) == 0 {
		return nil
	}

	myEntries := make([]types.BadgerEntry, 0, len(entries))
	for _, entry := range entries {
		if entry == nil || len(entry.Key) == 0 {
			continue
		}
		myEntries = append(myEntries, ConvertDarshanEntry(entry))
	}

	if len(myEntries) == 0 {
		return nil
	}

	return s.FlushBatch(myEntries)
}

// FlushBatch partitions entries and flushes them
func (s *DuckDBStorage) FlushBatch(entries []types.BadgerEntry) error {
	partitions := make(map[int][]types.BadgerEntry)

	for _, entry := range entries {
		partition := s.getPartition(entry.Key)
		partitions[partition] = append(partitions[partition], entry)
	}

	for partID, partEntries := range partitions {
		if err := s.flushPartitionBatch(partID, partEntries); err != nil {
			return fmt.Errorf("failed to flush partition %d: %w", partID, err)
		}
	}

	return nil
}

// flushPartitionBatch flushes entries to a specific partition
func (s *DuckDBStorage) flushPartitionBatch(partID int, entries []types.BadgerEntry) error {
	s.mu.RLock()
	conn := s.connections[partID]
	s.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("partition %d not initialized", partID)
	}

	tx, err := conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	tableName := fmt.Sprintf("partition_%d", partID)
	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT INTO %s (key, epoch_id, broker_id, assigned_ts, value)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (key, epoch_id, broker_id, assigned_ts) DO NOTHING
	`, tableName))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, entry := range entries {
		_, err = stmt.Exec(
			entry.Key,
			entry.Timestamp.EpochID,
			entry.Timestamp.BrokerID,
			entry.Timestamp.AssignedTs,
			entry.Value,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetPartitionStats returns statistics about a partition
func (s *DuckDBStorage) GetPartitionStats(partID int) (int64, error) {
	s.mu.RLock()
	conn := s.connections[partID]
	s.mu.RUnlock()

	if conn == nil {
		return 0, fmt.Errorf("partition %d not initialized", partID)
	}

	tableName := fmt.Sprintf("partition_%d", partID)
	var count int64

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	err := conn.QueryRow(query).Scan(&count)

	return count, err
}

// GetAllStats returns statistics for all partitions
func (s *DuckDBStorage) GetAllStats() (map[int]int64, error) {
	stats := make(map[int]int64)

	for i := 0; i < s.numPartitions; i++ {
		count, err := s.GetPartitionStats(i)
		if err != nil {
			return nil, err
		}
		stats[i] = count
	}

	return stats, nil
}
