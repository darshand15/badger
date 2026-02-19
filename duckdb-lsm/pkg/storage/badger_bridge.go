package storage

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4/duckdb-lsm/pkg/types"
	"github.com/dgraph-io/ristretto/v2/z"
	duckdb "github.com/marcboeker/go-duckdb"
)

// ConvertDarshanEntry converts a Badger entry to our storage Entry type
func ConvertDarshanEntry(key []byte, value []byte, version uint64) Entry {
	return Entry{
		Key:   key,
		Value: value,
		Timestamp: types.CustomTs{
			EpochID:    int64(version),
			BrokerID:   0,
			AssignedTs: 0,
		},
	}
}

// FlushDarshanEntries is the main entry point called from Badger's flush path
func (s *DuckDBStorage) FlushDarshanEntries(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}

	// Group entries by partition
	partitionMap := make(map[int][]Entry)
	for _, entry := range entries {
		partition := s.partCalc.GetPartitionForKey(entry.Key)
		partitionMap[partition] = append(partitionMap[partition], entry)
	}

	// Flush each partition in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, len(partitionMap))

	for partition, partEntries := range partitionMap {
		wg.Add(1)
		go func(p int, entries []Entry) {
			defer wg.Done()
			if err := s.flushPartitionWithAppender(p, entries); err != nil {
				errChan <- fmt.Errorf("partition %d flush failed: %w", p, err)
			}
		}(partition, partEntries)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		return err
	}

	return nil
}

// flushPartitionWithAppender uses DuckDB's Appender API for bulk inserts (3-10x faster)
func (s *DuckDBStorage) flushPartitionWithAppender(partition int, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}

	tableName := fmt.Sprintf("partition_%d", partition)

	// Get a connection from the pool
	conn, err := s.db.Conn(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	// Extract the underlying DuckDB connection for Appender API
	var duckdbConn *duckdb.Conn
	err = conn.Raw(func(driverConn interface{}) error {
		var ok bool
		duckdbConn, ok = driverConn.(*duckdb.Conn)
		if !ok {
			return fmt.Errorf("connection is not *duckdb.Conn, got %T", driverConn)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to extract DuckDB connection: %w", err)
	}

	// Create Appender for bulk insert
	appender, err := duckdb.NewAppenderFromConn(duckdbConn, "", tableName)
	if err != nil {
		return fmt.Errorf("failed to create appender for %s: %w", tableName, err)
	}
	defer appender.Close()

	// Bulk append all entries
	for _, entry := range entries {
		err := appender.AppendRow(
			entry.Key,                    // key BLOB
			entry.Timestamp.EpochID,      // epoch_id BIGINT
			entry.Timestamp.BrokerID,     // broker_id BIGINT
			entry.Timestamp.AssignedTs,   // assigned_ts BIGINT
			entry.Value,                  // value BLOB
		)
		if err != nil {
			return fmt.Errorf("failed to append row to %s: %w", tableName, err)
		}
	}

	// Single flush at the end (this is where the magic happens)
	if err := appender.Flush(); err != nil {
		return fmt.Errorf("failed to flush appender for %s: %w", tableName, err)
	}

	return nil
}

// flushPartitionBatch - OLD SLOW VERSION (kept for reference, not used)
// This is the prepared statement version that was 654% slower
func (s *DuckDBStorage) flushPartitionBatch_DEPRECATED(conn *sql.Conn, tableName string, entries []Entry) error {
	// Begin transaction
	tx, err := conn.BeginTx(s.ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Prepare statement (SLOW: prepared once but executed many times)
	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT INTO %s (key, epoch_id, broker_id, assigned_ts, value)
		VALUES (?, ?, ?, ?, ?)
	`, tableName))
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Execute for each entry (SLOW: individual SQL calls)
	for _, entry := range entries {
		_, err := stmt.Exec(
			entry.Key,
			entry.Timestamp.EpochID,
			entry.Timestamp.BrokerID,
			entry.Timestamp.AssignedTs,
			entry.Value,
		)
		if err != nil {
			return fmt.Errorf("failed to execute insert: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// PartitionCalculator handles hash-based key partitioning (matches Darshan's Badger implementation)
type PartitionCalculator struct {
	numPartitions int
}

func NewPartitionCalculator(numPartitions int) *PartitionCalculator {
	return &PartitionCalculator{
		numPartitions: numPartitions,
	}
}

func (pc *PartitionCalculator) GetPartitionForKey(key []byte) int {
	if pc.numPartitions <= 1 {
		return 0
	}
	// Use ristretto's MemHash (same as Darshan's Badger implementation)
	hash := z.MemHash(key)
	return int(hash % uint64(pc.numPartitions))
}

// Entry represents a key-value pair with timestamp
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp types.CustomTs
}