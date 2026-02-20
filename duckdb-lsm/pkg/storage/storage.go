package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4/duckdb-lsm/pkg/types"
	"github.com/dgraph-io/ristretto/v2/z"
	duckdb "github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"
)

// DuckDBStorage unified implementation
type DuckDBStorage struct {
    db       *sql.DB
    ctx      context.Context
    partCalc *PartitionCalculator
    mu       sync.RWMutex
    numParts int
    
    connCache map[int]*sql.Conn
    connMu    sync.Mutex
}

// NewDuckDBStorage creates a new DuckDB storage instance
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
	fmt.Printf("DuckDB connection pool: max=%d, idle=%d\n", numPartitions * 2, numPartitions)

	storage := &DuckDBStorage{
		db:       db,
		ctx:      context.Background(),
		partCalc: NewPartitionCalculator(numPartitions),
		numParts: numPartitions,
	}

	if err := storage.initializeTables(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize tables: %w", err)
	}

	return storage, nil
}

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

// Write inserts a single entry (for testing)
func (s *DuckDBStorage) Write(key []byte, value []byte, ts types.CustomTs) error {
	partition := s.partCalc.GetPartitionForKey(key)
	tableName := fmt.Sprintf("partition_%d", partition)

	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (key, epoch_id, broker_id, assigned_ts, value)
		VALUES (?, ?, ?, ?, ?)
	`, tableName)

	_, err := s.db.ExecContext(s.ctx, insertSQL, key, ts.EpochID, ts.BrokerID, ts.AssignedTs, value)
	return err
}

// Read retrieves the latest value for a key with timestamp <= readTs
func (s *DuckDBStorage) Read(key []byte, readTs types.CustomTs) (*Entry, error) {
	partition := s.partCalc.GetPartitionForKey(key)
	tableName := fmt.Sprintf("partition_%d", partition)

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
		return nil, nil
	}
	if err != nil {
		return nil, err
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

// FlushDarshanEntries - FAST VERSION WITH APPENDER API
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

	// Flush each partition in parallel using Appender API
	var wg sync.WaitGroup
	errChan := make(chan error, len(partitions))

	for pid, pEntries := range partitions {
		wg.Add(1)
		go func(partition int, entries []*DarshanEntry) {
			defer wg.Done()
			if err := s.flushPartitionWithAppender(partition, entries); err != nil {
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

	// Get connection from pool
	conn, err := s.db.Conn(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	// Extract DuckDB connection for Appender API
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
			entry.Key,
			entry.Timestamp.EpochID,
			entry.Timestamp.BrokerID,
			entry.Timestamp.AssignedTs,
			entry.Value,
		)
		if err != nil {
			return fmt.Errorf("failed to append row: %w", err)
		}
	}

	// Single flush (3-10x faster than individual inserts)
	if err := appender.Flush(); err != nil {
		return fmt.Errorf("failed to flush appender: %w", err)
	}

	return nil
}

// FlushDarshanEntriesBuffered - DEPRECATED (kept for compatibility)
func (s *DuckDBStorage) FlushDarshanEntriesBuffered(entries []*DarshanEntry, batchSize int) error {
	// Just call the Appender version - it's always faster
	return s.FlushDarshanEntries(entries)
}

func (s *DuckDBStorage) GetPartitionID(key []byte) int {
	return s.partCalc.GetPartitionForKey(key)
}

func (s *DuckDBStorage) getPartition(key []byte) int {
	return s.GetPartitionID(key)
}

func (s *DuckDBStorage) Close() error {
	return s.db.Close()
}

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

func NewPartitionCalculator(numPartitions int) *PartitionCalculator {
	if numPartitions <= 0 {
		numPartitions = 1
	}
	return &PartitionCalculator{
		numPartitions: numPartitions,
	}
}

func (pc *PartitionCalculator) GetPartitionForKey(key []byte) int {
	if pc.numPartitions <= 1 {
		return 0
	}
	hash := z.MemHash(key)
	return int(hash % uint64(pc.numPartitions))
}