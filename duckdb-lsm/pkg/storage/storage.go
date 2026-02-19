package storage

import (
	"database/sql"
	"fmt"
	"os"
	"sync"

	"github.com/dgraph-io/badger/v4/duckdb-lsm/pkg/types"

	_ "github.com/marcboeker/go-duckdb"
)

// DuckDBStorage manages multiple DuckDB partition databases
type DuckDBStorage struct {
	basePath      string
	db            *sql.DB // Main connection (set to connections[0])
	connections   []*sql.DB
	preparedStmts map[int]*sql.Stmt
	partitionCalc *PartitionCalculator
	numPartitions int
	mu            sync.RWMutex
}

// NewDuckDBStorage creates storage with DYNAMIC partition count
func NewDuckDBStorage(basePath string, partitionCount int) (*DuckDBStorage, error) {
	storage := &DuckDBStorage{
		basePath:      basePath,
		connections:   make([]*sql.DB, partitionCount),
		preparedStmts: make(map[int]*sql.Stmt),
		partitionCalc: NewPartitionCalculator(partitionCount),
		numPartitions: partitionCount,
	}

	// Initialize each partition database
	for pid := 0; pid < partitionCount; pid++ {
		if err := storage.initPartition(pid); err != nil {
			storage.Close()
			return nil, fmt.Errorf("failed to init partition %d: %w", pid, err)
		}
	}

	// Set main db connection
	storage.db = storage.connections[0]

	return storage, nil
}

// initPartition creates database connection and table for one partition
func (s *DuckDBStorage) initPartition(pid int) error {
	var dbPath string
	if s.basePath == ":memory:" {
		// In-memory mode
		dbPath = ":memory:"
	} else {
		// File-based mode
		dbPath = fmt.Sprintf("%s/partition_%d.db", s.basePath, pid)
		// Ensure base directory exists.
		if err := os.MkdirAll(s.basePath, 0o700); err != nil {
			return fmt.Errorf("failed to create duckdb base dir %q: %w", s.basePath, err)
		}
	}

	conn, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open partition %d: %w", pid, err)
	}

	s.connections[pid] = conn

	// Create table
	tableName := fmt.Sprintf("partition_%d", pid)
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

	if _, err := conn.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create table in partition %d: %w", pid, err)
	}

	// Create prepared statement
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (key, epoch_id, broker_id, assigned_ts, value)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (key, epoch_id, broker_id, assigned_ts) DO NOTHING
	`, tableName)

	stmt, err := conn.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare statement for partition %d: %w", pid, err)
	}

	s.preparedStmts[pid] = stmt

	return nil
}

// getPartition calculates partition ID from key
func (s *DuckDBStorage) getPartition(key []byte) int {
	return s.partitionCalc.GetPartitionID(key)
}

// Write writes a single entry
func (s *DuckDBStorage) Write(entry *types.BadgerEntry) error {
	pid := s.getPartition(entry.Key)

	s.mu.RLock()
	stmt := s.preparedStmts[pid]
	s.mu.RUnlock()

	if stmt == nil {
		return fmt.Errorf("partition %d not initialized", pid)
	}

	_, err := stmt.Exec(
		entry.Key,
		entry.Timestamp.EpochID,
		entry.Timestamp.BrokerID,
		entry.Timestamp.AssignedTs,
		entry.Value,
	)

	return err
}

// Read reads the latest version <= given timestamp
func (s *DuckDBStorage) Read(key []byte, readTs types.CustomTs) (*types.BadgerEntry, error) {
	pid := s.getPartition(key)

	s.mu.RLock()
	conn := s.connections[pid]
	s.mu.RUnlock()

	if conn == nil {
		return nil, fmt.Errorf("partition %d not initialized", pid)
	}

	tableName := fmt.Sprintf("partition_%d", pid)
	// Fetch the latest row for the key (no read-timestamp filtering).
	query := fmt.Sprintf(`
		SELECT epoch_id, broker_id, assigned_ts, value
		FROM %s
		WHERE key = ?
		ORDER BY epoch_id DESC, broker_id DESC, assigned_ts DESC
		LIMIT 1
	`, tableName)

	var entry types.BadgerEntry
	err := conn.QueryRow(query, key).Scan(
		&entry.Timestamp.EpochID,
		&entry.Timestamp.BrokerID,
		&entry.Timestamp.AssignedTs,
		&entry.Value,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// Keep the stored key as the logical key.
	entry.Key = key
	return &entry, nil
}

// FlushPartition flushes a batch of entries to a specific partition
func (s *DuckDBStorage) FlushPartition(pid int, entries []*types.BadgerEntry) error {
	s.mu.RLock()
	conn := s.connections[pid]
	s.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("partition %d not initialized", pid)
	}

	tx, err := conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt := tx.Stmt(s.preparedStmts[pid])

	for _, entry := range entries {
		_, err := stmt.Exec(
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

// Close closes all connections
func (s *DuckDBStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var lastErr error

	for pid, stmt := range s.preparedStmts {
		if stmt != nil {
			if err := stmt.Close(); err != nil {
				lastErr = err
			}
			delete(s.preparedStmts, pid)
		}
	}

	for i, conn := range s.connections {
		if conn != nil {
			if err := conn.Close(); err != nil {
				lastErr = err
			}
			s.connections[i] = nil
		}
	}

	return lastErr
}

func (s *DuckDBStorage) GetPartitionCount() int {
	return s.numPartitions
}
