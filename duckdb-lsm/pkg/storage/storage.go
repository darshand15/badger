package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4/duckdb-lsm/pkg/types"
	"github.com/dgraph-io/ristretto/v2/z"
	_ "github.com/marcboeker/go-duckdb"
)

// DuckDBStorage unified implementation
type DuckDBStorage struct {
	db       *sql.DB
	ctx      context.Context
	partCalc *PartitionCalculator
	mu       sync.RWMutex
	numParts int
}

// NewDuckDBStorage creates a new DuckDB storage instance
func NewDuckDBStorage(dbPath string, numPartitions int) (*DuckDBStorage, error) {
	if numPartitions <= 0 {
		numPartitions = 8 // Default to 8 partitions
	}

	// Open DuckDB connection
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	// Configure connection pool for better concurrency
	db.SetMaxOpenConns(numPartitions * 2) // 2 connections per partition
	db.SetMaxIdleConns(numPartitions)

	storage := &DuckDBStorage{
		db:       db,
		ctx:      context.Background(),
		partCalc: NewPartitionCalculator(numPartitions),
		numParts: numPartitions,
	}

	// Initialize all partition tables
	if err := storage.initializeTables(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize tables: %w", err)
	}

	return storage, nil
}

// initializeTables creates all partition tables with proper schema
func (s *DuckDBStorage) initializeTables() error {
	for i := 0; i < s.numParts; i++ {
		tableName := fmt.Sprintf("partition_%d", i)
		createTableSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				key BLOB NOT NULL,
				epoch_id BIGINT NOT NULL,
				broker_id BIGINT NOT NULL,
				assigned_ts BIGINT NOT NULL,
				value BLOB,
				PRIMARY KEY (key, epoch_id, broker_id, assigned_ts)
			)
		`, tableName)

		if _, err := s.db.ExecContext(s.ctx, createTableSQL); err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
	}
	return nil
}

// Write inserts a single entry
func (s *DuckDBStorage) Write(key []byte, value []byte, ts types.CustomTs) error {
	partition := s.partCalc.GetPartitionForKey(key)
	tableName := fmt.Sprintf("partition_%d", partition)

	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (key, epoch_id, broker_id, assigned_ts, value)
		VALUES (?, ?, ?, ?, ?)
	`, tableName)

	_, err := s.db.ExecContext(s.ctx, insertSQL, key, ts.EpochID, ts.BrokerID, ts.AssignedTs, value)
	if err != nil {
		return fmt.Errorf("failed to write to %s: %w", tableName, err)
	}

	return nil
}

// Read retrieves the latest value for a key with timestamp <= readTs
func (s *DuckDBStorage) Read(key []byte, readTs types.CustomTs) (*Entry, error) {
	partition := s.partCalc.GetPartitionForKey(key)
	tableName := fmt.Sprintf("partition_%d", partition)

	// Query for the latest entry with timestamp <= readTs
	querySQL := fmt.Sprintf(`
		SELECT key, epoch_id, broker_id, assigned_ts, value
		FROM %s
		WHERE key = ?
		  AND (epoch_id < ? OR 
		       (epoch_id = ? AND broker_id < ?) OR
		       (epoch_id = ? AND broker_id = ? AND assigned_ts <= ?))
		ORDER BY epoch_id DESC, broker_id DESC, assigned_ts DESC
		LIMIT 1
	`, tableName)

	var entry Entry
	var epochID, brokerID, assignedTs int64

	err := s.db.QueryRowContext(
		s.ctx,
		querySQL,
		key,
		readTs.EpochID,
		readTs.EpochID, readTs.BrokerID,
		readTs.EpochID, readTs.BrokerID, readTs.AssignedTs,
	).Scan(&entry.Key, &epochID, &brokerID, &assignedTs, &entry.Value)

	if err == sql.ErrNoRows {
		return nil, nil // Key not found
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read from %s: %w", tableName, err)
	}

	entry.Timestamp = types.CustomTs{
		EpochID:    epochID,
		BrokerID:   brokerID,
		AssignedTs: assignedTs,
	}

	return &entry, nil
}

// ReadAll retrieves all entries for a key
func (s *DuckDBStorage) ReadAll(key []byte) ([]Entry, error) {
	partition := s.partCalc.GetPartitionForKey(key)
	tableName := fmt.Sprintf("partition_%d", partition)

	querySQL := fmt.Sprintf(`
		SELECT key, epoch_id, broker_id, assigned_ts, value
		FROM %s
		WHERE key = ?
		ORDER BY epoch_id DESC, broker_id DESC, assigned_ts DESC
	`, tableName)

	rows, err := s.db.QueryContext(s.ctx, querySQL, key)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s: %w", tableName, err)
	}
	defer rows.Close()

	var entries []Entry
	for rows.Next() {
		var entry Entry
		var epochID, brokerID, assignedTs int64

		if err := rows.Scan(&entry.Key, &epochID, &brokerID, &assignedTs, &entry.Value); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		entry.Timestamp = types.CustomTs{
			EpochID:    epochID,
			BrokerID:   brokerID,
			AssignedTs: assignedTs,
		}

		entries = append(entries, entry)
	}

	return entries, rows.Err()
}

// FlushDarshanEntries bulk inserts entries using transactions
func (s *DuckDBStorage) FlushDarshanEntries(entries []*DarshanEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Group by partition
	partitions := make(map[int][]*DarshanEntry)
	for _, entry := range entries {
		pid := s.partCalc.GetPartitionForKey(entry.Key)
		partitions[pid] = append(partitions[pid], entry)
	}

	// Insert per partition in a transaction
	for pid, pEntries := range partitions {
		tableName := fmt.Sprintf("partition_%d", pid)
		tx, err := s.db.BeginTx(s.ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		for _, entry := range pEntries {
			insertSQL := fmt.Sprintf(`
				INSERT INTO %s (key, epoch_id, broker_id, assigned_ts, value)
				VALUES (?, ?, ?, ?, ?)
			`, tableName)

			if _, err := tx.ExecContext(
				s.ctx,
				insertSQL,
				entry.Key,
				entry.Timestamp.EpochID,
				entry.Timestamp.BrokerID,
				entry.Timestamp.AssignedTs,
				entry.Value,
			); err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to insert into %s: %w", tableName, err)
			}
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction for %s: %w", tableName, err)
		}
	}

	return nil
}

// FlushDarshanEntriesBuffered flushes with periodic batching
func (s *DuckDBStorage) FlushDarshanEntriesBuffered(entries []*DarshanEntry, batchSize int) error {
	if len(entries) == 0 {
		return nil
	}

	// Group by partition
	partitions := make(map[int][]*DarshanEntry)
	for _, entry := range entries {
		pid := s.partCalc.GetPartitionForKey(entry.Key)
		partitions[pid] = append(partitions[pid], entry)
	}

	// Insert per partition with periodic flushing
	for pid, pEntries := range partitions {
		tableName := fmt.Sprintf("partition_%d", pid)
		
		for i := 0; i < len(pEntries); i += batchSize {
			end := i + batchSize
			if end > len(pEntries) {
				end = len(pEntries)
			}

			batch := pEntries[i:end]
			tx, err := s.db.BeginTx(s.ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}

			for _, entry := range batch {
				insertSQL := fmt.Sprintf(`
					INSERT INTO %s (key, epoch_id, broker_id, assigned_ts, value)
					VALUES (?, ?, ?, ?, ?)
				`, tableName)

				if _, err := tx.ExecContext(
					s.ctx,
					insertSQL,
					entry.Key,
					entry.Timestamp.EpochID,
					entry.Timestamp.BrokerID,
					entry.Timestamp.AssignedTs,
					entry.Value,
				); err != nil {
					tx.Rollback()
					return fmt.Errorf("failed to insert into %s: %w", tableName, err)
				}
			}

			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit batch for %s: %w", tableName, err)
			}
		}
	}

	return nil
}

// GetPartitionID returns which partition a key belongs to
func (s *DuckDBStorage) GetPartitionID(key []byte) int {
	return s.partCalc.GetPartitionForKey(key)
}

// getPartition helper (used elsewhere in code)
func (s *DuckDBStorage) getPartition(key []byte) int {
	return s.GetPartitionID(key)
}

// Close closes the DuckDB connection
func (s *DuckDBStorage) Close() error {
	return s.db.Close()
}

// GetStats returns storage statistics
func (s *DuckDBStorage) GetStats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})
	
	for i := 0; i < s.numParts; i++ {
		tableName := fmt.Sprintf("partition_%d", i)
		var count int64
		
		err := s.db.QueryRowContext(
			s.ctx,
			fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName),
		).Scan(&count)
		
		if err != nil {
			return nil, fmt.Errorf("failed to get stats for %s: %w", tableName, err)
		}
		
		stats[tableName] = count
	}
	
	return stats, nil
}

// Entry represents a key-value pair with timestamp
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp types.CustomTs
}

// DarshanEntry represents a Badger entry ready for DuckDB
type DarshanEntry struct {
	Key       []byte
	Value     []byte
	Timestamp types.CustomTs
	Version   uint64
}

// PartitionCalculator handles hash-based key partitioning
type PartitionCalculator struct {
	numPartitions int
}

// NewPartitionCalculator creates a partition calculator
func NewPartitionCalculator(numPartitions int) *PartitionCalculator {
	if numPartitions <= 0 {
		numPartitions = 1
	}
	return &PartitionCalculator{
		numPartitions: numPartitions,
	}
}

// GetPartitionForKey returns which partition a key belongs to
func (pc *PartitionCalculator) GetPartitionForKey(key []byte) int {
	if pc.numPartitions <= 1 {
		return 0
	}
	hash := z.MemHash(key)
	return int(hash % uint64(pc.numPartitions))
}