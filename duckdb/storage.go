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
	_ "github.com/marcboeker/go-duckdb"
	duckdbdriver "github.com/marcboeker/go-duckdb"
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
	Deleted   bool // true when this entry is a delete tombstone
}

// ---------------------------------------------------------------------------
// DuckDBStorage
// ---------------------------------------------------------------------------

// defaultFlushBatchSize is the row threshold for the memtable-flush path
// (FlushDarshanEntries).  Setting this to 1 means every call flushes
// immediately, which is correct for the infrequent, large-batch memtable path.
const defaultFlushBatchSize int64 = 1

// directFlushBatchSize is the per-partition row threshold for the
// per-commit DirectAppendEntries path.  Each transaction commit appends
// its rows to the Appender buffer; a CGo Appender.Flush() is issued only
// once this many rows have accumulated, amortising the fixed CGo cost across
// many commits.  Reads flush pending rows before querying so correctness
// is not affected by this value.
const directFlushBatchSize int64 = 512

// partitionAppender owns a persistent DuckDB connection and Appender for a
// single partition.  Keeping the Appender alive across memtable flushes
// eliminates the per-flush cost of:
//   - sql.DB.Conn() (connection acquisition / pool lookup)
//   - conn.Raw()    (CGo extraction)
//   - duckdbdriver.NewAppenderFromConn()  (allocates the C-side Appender)
//
// pendingKeys is a presence-only set of logical keys that have rows sitting
// in the DuckDB Appender buffer that have not yet been Flush'd to the table.
// Only the key STRING is stored — no value bytes — so this structure has
// negligible memory footprint regardless of value size and incurs no GC
// pressure from large BLOBs.
//
// Read() consults pendingKeys under a read-lock:
//   - If the key is NOT in the set, the data is already in DuckDB → query SQL
//     directly with no flush.
//   - If the key IS in the set, unflushed rows exist → acquire write lock,
//     flush the partition, then query SQL.
//
// The set is cleared (under write-lock) whenever flush() succeeds.
type partitionAppender struct {
	mu          sync.RWMutex
	sqlConn     *sql.Conn
	appender    *duckdbdriver.Appender
	pendingRows int64
	pendingKeys map[string]struct{} // keys with unflushed AppendRow'd rows
}

// flush pushes buffered rows to DuckDB and clears pendingKeys.
// Must be called with mu held for writing.
func (pa *partitionAppender) flush() error {
	if pa.pendingRows == 0 {
		return nil
	}
	if err := pa.appender.Flush(); err != nil {
		return err
	}
	pa.pendingRows = 0
	pa.pendingKeys = make(map[string]struct{})
	return nil
}

// markPending records that a key has an unflushed row in the Appender buffer.
// Must be called with mu held for writing, immediately after a successful AppendRow.
func (pa *partitionAppender) markPending(key []byte) {
	pa.pendingKeys[string(key)] = struct{}{}
}

// hasPending reports whether key has any unflushed rows in the Appender buffer.
// Must be called with mu held (read or write).
func (pa *partitionAppender) hasPending(key []byte) bool {
	_, ok := pa.pendingKeys[string(key)]
	return ok
}

// DuckDBStorage is the unified DuckDB storage implementation.
type DuckDBStorage struct {
	db             *sql.DB
	ctx            context.Context
	partCalc       *partitionCalculator
	mu             sync.RWMutex
	numParts       int
	// partAppenders holds one persistent Appender per partition.
	// Indexed by partition ID; created in initPersistentAppenders.
	partAppenders  []*partitionAppender
	// flushBatchSize is the per-partition row count that triggers an automatic
	// Appender flush.  Configurable; defaults to defaultFlushBatchSize.
	flushBatchSize int64
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
		db:             db,
		ctx:            context.Background(),
		partCalc:       newPartitionCalculator(numPartitions),
		numParts:       numPartitions,
		flushBatchSize: defaultFlushBatchSize,
	}

	if err := s.initializeTables(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize tables: %w", err)
	}

	if err := s.initPersistentAppenders(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to init persistent appenders: %w", err)
	}

	return s, nil
}

// initPersistentAppenders opens one SQL connection and one duckdb Appender per
// partition and stores them in s.partAppenders.  These are kept alive for the
// lifetime of the storage instance; re-using them across flushes eliminates
// repeated CGo boundary crossings for connection acquisition and Appender
// construction.
func (s *DuckDBStorage) initPersistentAppenders() error {
	s.partAppenders = make([]*partitionAppender, s.numParts)
	for i := 0; i < s.numParts; i++ {
		tableName := fmt.Sprintf("partition_%d", i)

		sqlConn, err := s.db.Conn(s.ctx)
		if err != nil {
			return fmt.Errorf("partition %d: get conn: %w", i, err)
		}

		var duckConn *duckdbdriver.Conn
		if err := sqlConn.Raw(func(dc interface{}) error {
			var ok bool
			duckConn, ok = dc.(*duckdbdriver.Conn)
			if !ok {
				return fmt.Errorf("not a *duckdb.Conn, got %T", dc)
			}
			return nil
		}); err != nil {
			sqlConn.Close()
			return fmt.Errorf("partition %d: extract duckdb conn: %w", i, err)
		}

		appender, err := duckdbdriver.NewAppenderFromConn(duckConn, "", tableName)
		if err != nil {
			sqlConn.Close()
			return fmt.Errorf("partition %d: create appender: %w", i, err)
		}

		s.partAppenders[i] = &partitionAppender{
			sqlConn:     sqlConn,
			appender:    appender,
			pendingKeys: make(map[string]struct{}),
		}
	}
	return nil
}

// FlushAllPending flushes pending rows in every partition's Appender.
func (s *DuckDBStorage) FlushAllPending() error {
	for i, pa := range s.partAppenders {
		pa.mu.Lock()
		err := pa.flush()
		pa.mu.Unlock()
		if err != nil {
			return fmt.Errorf("partition %d: flush pending: %w", i, err)
		}
	}
	return nil
}

// SetFlushBatchSize overrides the per-partition row threshold that triggers an
// automatic Appender.Flush() on the memtable-flush path.  The default is 1.
// Raise this for bulk-ingest workloads where each memtable flush contains
// hundreds of rows.  The value must be >= 1.
func (s *DuckDBStorage) SetFlushBatchSize(n int64) {
	if n < 1 {
		n = 1
	}
	s.flushBatchSize = n
}

// DirectAppendEntries is the hot per-commit write path.  It appends entries
// directly into the persistent Appender buffers, bypassing the WAL and
// memtable entirely.  A CGo Appender.Flush() fires automatically once
// directFlushBatchSize rows have accumulated in a partition.
func (s *DuckDBStorage) DirectAppendEntries(entries []*DarshanEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Group by partition.
	partitions := make(map[int][]*DarshanEntry)
	for _, e := range entries {
		pid := s.partCalc.getPartition(e.Key)
		partitions[pid] = append(partitions[pid], e)
	}

	// Fast path: single partition — no goroutine overhead.
	if len(partitions) == 1 {
		for pid, pEntries := range partitions {
			return s.appendPartitionDirect(pid, pEntries)
		}
	}

	// Multi-partition: flush in parallel.
	var wg sync.WaitGroup
	errChan := make(chan error, len(partitions))
	for pid, pEntries := range partitions {
		wg.Add(1)
		go func(partition int, batch []*DarshanEntry) {
			defer wg.Done()
			if err := s.appendPartitionDirect(partition, batch); err != nil {
				errChan <- err
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

// appendPartitionDirect appends rows to the persistent Appender for the given
// partition without forcing a CGo flush on every call.  A flush is triggered
// once directFlushBatchSize rows have accumulated, amortising the cost.
func (s *DuckDBStorage) appendPartitionDirect(partition int, entries []*DarshanEntry) error {
	pa := s.partAppenders[partition]
	pa.mu.Lock()
	defer pa.mu.Unlock()

	for _, e := range entries {
		if err := pa.appender.AppendRow(
			e.Key,
			e.Timestamp.EpochID,
			e.Timestamp.BrokerID,
			e.Timestamp.AssignedTs,
			e.Value,
			e.Deleted,
		); err != nil {
			return fmt.Errorf("partition %d: direct append row: %w", partition, err)
		}
		pa.markPending(e.Key)
		pa.pendingRows++
	}

	if pa.pendingRows >= directFlushBatchSize {
		if err := pa.flush(); err != nil {
			return fmt.Errorf("partition %d: direct flush: %w", partition, err)
		}
	}
	return nil
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
				value BLOB,				deleted BOOLEAN NOT NULL DEFAULT FALSE,				PRIMARY KEY (key, epoch_id, broker_id, assigned_ts)
			)`, tableName)
		if _, err := s.db.ExecContext(s.ctx, createSQL); err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
	}
	return nil
}

// Read retrieves the latest value for a key with timestamp <= readTs.
//
// Flush-on-demand: checks the pendingKeys set under a cheap RLock.
//   - Key not in set → data already committed to DuckDB, query SQL directly.
//   - Key in set → acquire write lock and flush (one CGo Flush() covering all
//     buffered rows for this partition), then query SQL.
//
// For write-heavy workloads the vast majority of reads are on keys that were
// committed in a prior flush cycle and are not in pendingKeys, so the flush
// is skipped entirely.  Reads on freshly-written keys (hot keys in the
// current 512-row window) pay one flush, but only once per window, not once
// per read.
func (s *DuckDBStorage) Read(key []byte, readTs CustomTs) (*Entry, error) {
	partition := s.partCalc.getPartition(key)
	pa := s.partAppenders[partition]

	// Check whether this key has any unflushed rows — RLock is enough.
	pa.mu.RLock()
	needFlush := pa.hasPending(key)
	pa.mu.RUnlock()

	if needFlush {
		// Upgrade to write-lock and flush so the SQL query below sees all rows.
		pa.mu.Lock()
		if err := pa.flush(); err != nil {
			pa.mu.Unlock()
			return nil, fmt.Errorf("flush pending before read: %w", err)
		}
		pa.mu.Unlock()
	}

	tableName := fmt.Sprintf("partition_%d", partition)

	querySQL := fmt.Sprintf(`
		SELECT key, epoch_id, broker_id, assigned_ts, value, deleted
		FROM %s
		WHERE key = ?
		  AND (epoch_id < ? OR
		       (epoch_id = ? AND broker_id < ?) OR
		       (epoch_id = ? AND broker_id = ? AND assigned_ts <= ?))
		ORDER BY epoch_id DESC, broker_id DESC, assigned_ts DESC
		LIMIT 1`, tableName)

	var entry Entry
	var epochID, brokerID, assignedTs int64
	var deleted bool

	err := s.db.QueryRowContext(
		s.ctx, querySQL,
		key,
		readTs.EpochID,
		readTs.EpochID, readTs.BrokerID,
		readTs.EpochID, readTs.BrokerID, readTs.AssignedTs,
	).Scan(&entry.Key, &epochID, &brokerID, &assignedTs, &entry.Value, &deleted)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if deleted {
		// Tombstone row: key was explicitly deleted at this timestamp.
		return nil, nil
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

// flushPartitionWithAppender appends entries to the persistent Appender for
// the given partition.  It avoids the per-call overhead of connection
// acquisition and Appender construction that existed in the previous
// implementation by reusing the Appender held in s.partAppenders[partition].
//
// A CGo-level Appender.Flush() is issued automatically once the number of
// buffered rows reaches s.flushBatchSize, amortizing the fixed CGo boundary
// cost across many entries.  Read() no longer calls Flush() — it uses the
// Go-side pendingByKey mirror instead.
func (s *DuckDBStorage) flushPartitionWithAppender(partition int, entries []*DarshanEntry) error {
	if len(entries) == 0 {
		return nil
	}

	pa := s.partAppenders[partition]
	pa.mu.Lock()
	defer pa.mu.Unlock()

	for _, e := range entries {
		if err := pa.appender.AppendRow(
			e.Key,
			e.Timestamp.EpochID,
			e.Timestamp.BrokerID,
			e.Timestamp.AssignedTs,
			e.Value,
			e.Deleted,
		); err != nil {
			return fmt.Errorf("failed to append row to partition %d: %w", partition, err)
		}
		pa.markPending(e.Key)
		pa.pendingRows++
	}

	// Trigger a coarse flush once the per-partition buffer is large enough.
	// This amortizes the fixed CGo cost of Appender.Flush() across many rows
	// rather than paying it once per memtable flush.
	if pa.pendingRows >= s.flushBatchSize {
		if err := pa.flush(); err != nil {
			return fmt.Errorf("failed to flush appender for partition %d: %w", partition, err)
		}
	}

	return nil
}

// CompactPartitions removes superseded versions and runs VACUUM.
func (s *DuckDBStorage) CompactPartitions() error {
	// Ensure all buffered rows are visible to the DELETE queries below.
	if err := s.FlushAllPending(); err != nil {
		return fmt.Errorf("compact: flush pending: %w", err)
	}

	for i := 0; i < s.numParts; i++ {
		tableName := fmt.Sprintf("partition_%d", i)
		// Delete all rows that are NOT the latest version per key.
		// Uses rowid to identify rows — avoids tuple-IN syntax that some
		// DuckDB versions reject for multi-column subqueries.
		deleteSQL := fmt.Sprintf(`
			DELETE FROM %s
			WHERE rowid NOT IN (
				SELECT rowid FROM (
					SELECT rowid,
						ROW_NUMBER() OVER (
							PARTITION BY key
							ORDER BY epoch_id DESC, broker_id DESC, assigned_ts DESC
						) AS rn
					FROM %s
				) sub WHERE rn = 1
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
	// Flush + close persistent appenders before closing the underlying DB;
	// appender.Close() flushes any remaining buffered rows.
	for i, pa := range s.partAppenders {
		pa.mu.Lock()
		if pa.appender != nil {
			if err := pa.appender.Close(); err != nil {
				// Log but continue — we still want to close everything.
				_ = fmt.Errorf("close appender for partition %d: %w", i, err)
			}
			pa.appender = nil
		}
		if pa.sqlConn != nil {
			_ = pa.sqlConn.Close()
			pa.sqlConn = nil
		}
		pa.pendingRows = 0
		pa.pendingKeys = nil
		pa.mu.Unlock()
	}
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
