//go:build duckdb

// Package duckdb implements the DuckDB-backed storage layer for Badger.
// Only compiled when the "duckdb" build tag is set.
package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/dgraph-io/ristretto/v2/z"
	duckdbdriver "github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"
)

// ---------------------------------------------------------------------------
// Timestamp types
// ---------------------------------------------------------------------------

// CustomTs represents a custom timestamp with three components.
type CustomTs struct {
	EpochID    int64
	BrokerID   int64
	AssignedTs int64
}

// Compare returns -1, 0, or +1 comparing t to other.
func (t CustomTs) Compare(other CustomTs) int {
	if t.EpochID != other.EpochID {
		if t.EpochID < other.EpochID {
			return -1
		}
		return 1
	}
	if t.BrokerID != other.BrokerID {
		if t.BrokerID < other.BrokerID {
			return -1
		}
		return 1
	}
	if t.AssignedTs != other.AssignedTs {
		if t.AssignedTs < other.AssignedTs {
			return -1
		}
		return 1
	}
	return 0
}

// LessOrEqual returns true if t <= other.
func (t CustomTs) LessOrEqual(other CustomTs) bool { return t.Compare(other) <= 0 }

// Less returns true if t < other.
func (t CustomTs) Less(other CustomTs) bool { return t.Compare(other) < 0 }

// ---------------------------------------------------------------------------
// Storage types
// ---------------------------------------------------------------------------

// Entry represents a key-value pair with timestamp.
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp CustomTs
}

// DarshanEntry is a Badger entry ready for DuckDB.
type DarshanEntry struct {
	Key       []byte
	Value     []byte
	Timestamp CustomTs
	Version   uint64
}

// ---------------------------------------------------------------------------
// DuckDBStorage
// ---------------------------------------------------------------------------

// DuckDBStorage is the unified DuckDB storage implementation.
type DuckDBStorage struct {
	db       *sql.DB
	ctx      context.Context
	partCalc *partitionCalculator
	mu       sync.RWMutex
	numParts int
}

// NewDuckDBStorage creates a new DuckDB storage instance.
func NewDuckDBStorage(dbPath string, numPartitions int) (*DuckDBStorage, error) {
	if numPartitions <= 0 {
		numPartitions = 8
	}

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	db.SetMaxOpenConns(numPartitions * 10)
	db.SetMaxIdleConns(numPartitions * 5)

	s := &DuckDBStorage{
		db:       db,
		ctx:      context.Background(),
		partCalc: newPartitionCalculator(numPartitions),
		numParts: numPartitions,
	}

	if err := s.initializeTables(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize tables: %w", err)
	}

	return s, nil
}

func (s *DuckDBStorage) initializeTables() error {
	for i := 0; i < s.numParts; i++ {
		tableName := fmt.Sprintf("partition_%d", i)
		createSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				key BLOB NOT NULL,
				epoch_id BIGINT NOT NULL,
				broker_id BIGINT NOT NULL,
				assigned_ts BIGINT NOT NULL,
				value BLOB,
				PRIMARY KEY (key, epoch_id, broker_id, assigned_ts)
			)`, tableName)
		if _, err := s.db.ExecContext(s.ctx, createSQL); err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
	}
	return nil
}

// Read retrieves the latest value for a key with timestamp <= readTs.
func (s *DuckDBStorage) Read(key []byte, readTs CustomTs) (*Entry, error) {
	partition := s.partCalc.getPartition(key)
	tableName := fmt.Sprintf("partition_%d", partition)

	querySQL := fmt.Sprintf(`
		SELECT key, epoch_id, broker_id, assigned_ts, value
		FROM %s
		WHERE key = ?
		  AND (epoch_id < ? OR
		       (epoch_id = ? AND broker_id < ?) OR
		       (epoch_id = ? AND broker_id = ? AND assigned_ts <= ?))
		ORDER BY epoch_id DESC, broker_id DESC, assigned_ts DESC
		LIMIT 1`, tableName)

	var entry Entry
	var epochID, brokerID, assignedTs int64

	err := s.db.QueryRowContext(
		s.ctx, querySQL,
		key,
		readTs.EpochID,
		readTs.EpochID, readTs.BrokerID,
		readTs.EpochID, readTs.BrokerID, readTs.AssignedTs,
	).Scan(&entry.Key, &epochID, &brokerID, &assignedTs, &entry.Value)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	entry.Timestamp = CustomTs{EpochID: epochID, BrokerID: brokerID, AssignedTs: assignedTs}
	return &entry, nil
}

// FlushDarshanEntries writes a batch of entries to DuckDB.
func (s *DuckDBStorage) FlushDarshanEntries(entries []*DarshanEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Group by partition.
	partitions := make(map[int][]*DarshanEntry)
	for _, e := range entries {
		pid := s.partCalc.getPartition(e.Key)
		partitions[pid] = append(partitions[pid], e)
	}

	// Fast path: single partition — no goroutine/WaitGroup overhead.
	if len(partitions) == 1 {
		for pid, pEntries := range partitions {
			return s.flushPartitionWithAppender(pid, pEntries)
		}
	}

	// Multi-partition: flush in parallel.
	var wg sync.WaitGroup
	errChan := make(chan error, len(partitions))
	for pid, pEntries := range partitions {
		wg.Add(1)
		go func(partition int, batch []*DarshanEntry) {
			defer wg.Done()
			if err := s.flushPartitionWithAppender(partition, batch); err != nil {
				errChan <- fmt.Errorf("partition %d flush failed: %w", partition, err)
			}
		}(pid, pEntries)
	}
	wg.Wait()
	close(errChan)
	for err := range errChan {
		return err
	}
	return nil
}

func (s *DuckDBStorage) flushPartitionWithAppender(partition int, entries []*DarshanEntry) error {
	if len(entries) == 0 {
		return nil
	}

	tableName := fmt.Sprintf("partition_%d", partition)

	conn, err := s.db.Conn(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	var duckConn *duckdbdriver.Conn
	if err := conn.Raw(func(dc interface{}) error {
		var ok bool
		duckConn, ok = dc.(*duckdbdriver.Conn)
		if !ok {
			return fmt.Errorf("connection is not *duckdb.Conn, got %T", dc)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to extract DuckDB connection: %w", err)
	}

	appender, err := duckdbdriver.NewAppenderFromConn(duckConn, "", tableName)
	if err != nil {
		return fmt.Errorf("failed to create appender for %s: %w", tableName, err)
	}
	defer appender.Close()

	for _, e := range entries {
		if err := appender.AppendRow(
			e.Key,
			e.Timestamp.EpochID,
			e.Timestamp.BrokerID,
			e.Timestamp.AssignedTs,
			e.Value,
		); err != nil {
			return fmt.Errorf("failed to append row: %w", err)
		}
	}

	if err := appender.Flush(); err != nil {
		return fmt.Errorf("failed to flush appender: %w", err)
	}
	return nil
}

// CompactPartitions removes superseded versions and runs VACUUM.
func (s *DuckDBStorage) CompactPartitions() error {
	for i := 0; i < s.numParts; i++ {
		tableName := fmt.Sprintf("partition_%d", i)
		deleteSQL := fmt.Sprintf(`
			DELETE FROM %s
			WHERE (key, epoch_id, broker_id, assigned_ts) IN (
				SELECT key, epoch_id, broker_id, assigned_ts
				FROM (
					SELECT key, epoch_id, broker_id, assigned_ts,
						ROW_NUMBER() OVER (
							PARTITION BY key
							ORDER BY epoch_id DESC, broker_id DESC, assigned_ts DESC
						) AS rn
					FROM %s
				) sub WHERE rn > 1
			)`, tableName, tableName)
		if _, err := s.db.ExecContext(s.ctx, deleteSQL); err != nil {
			return fmt.Errorf("compact %s: %w", tableName, err)
		}
	}
	_, err := s.db.ExecContext(s.ctx, "VACUUM")
	return err
}

// Close releases all DuckDB resources.
func (s *DuckDBStorage) Close() error {
	return s.db.Close()
}

// ---------------------------------------------------------------------------
// partitionCalculator (unexported)
// ---------------------------------------------------------------------------

type partitionCalculator struct {
	numPartitions int
}

func newPartitionCalculator(numPartitions int) *partitionCalculator {
	if numPartitions <= 0 {
		numPartitions = 1
	}
	return &partitionCalculator{numPartitions: numPartitions}
}

func (pc *partitionCalculator) getPartition(key []byte) int {
	if pc.numPartitions <= 1 {
		return 0
	}
	return int(z.MemHash(key) % uint64(pc.numPartitions))
}
