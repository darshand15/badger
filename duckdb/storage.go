//go:build duckdb

// Package duckdb implements the DuckDB-backed storage layer for Badger.
// Only compiled when the "duckdb" build tag is set.
package duckdb

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
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

// defaultDirectFlushBatchSize is the per-partition row threshold for the
// DirectAppendEntries hot path. Unlike the memtable path, direct appends can
// run for long periods (e.g. 100M seed) without point reads touching most
// keys, so buffering unbounded rows in one Appender can make the eventual
// FlushAllPending call extremely expensive.
//
// We keep the threshold large enough to amortize CGo flush overhead, but
// bounded so long ingest phases periodically drain appender state.
const defaultDirectFlushBatchSize int64 = 50_000

// defaultReadPoolSize is the default number of dedicated read connections kept per
// partition (see partitionAppender.readConns). A single DuckDB connection
// handles one statement at a time, so this bounds how many readers can be
// concurrently active against one partition without serializing on either
// a single shared connection or database/sql's global pool lock.
// Tuned on Apple silicon using the Ashley sweep harness.
const defaultReadPoolSize = 2

// SmallBank transactions normally prefetch two or three keys. Preparing the
// corresponding batch statements once per read connection avoids reparsing
// the same ROW_NUMBER query for every transaction. Larger request sizes keep
// using the dynamic fallback so memory use stays bounded.
const maxPreparedReadBatchKeys = 8

// defaultEnvFlushBatchSize is the fallback flush threshold for the memtable
// flush path when BADGER_DUCKDB_FLUSH_BATCH_SIZE is unset/invalid.
//
// Ashley track profiling showed 1..4 as the best range for mixed transfer and
// ingest workloads; pin fallback to 4 so production-like profiles are stable
// unless explicitly overridden by env.
const defaultEnvFlushBatchSize int64 = 4

// readPoolSizeFromEnv reads BADGER_DUCKDB_READ_POOL_SIZE and clamps invalid
// values. Keeping this as an env var avoids changing public DB option structs
// while allowing fast local tuning runs.
func readPoolSizeFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("BADGER_DUCKDB_READ_POOL_SIZE"))
	if raw == "" {
		return defaultReadPoolSize
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return defaultReadPoolSize
	}
	if n < 1 {
		return 1
	}
	if n > 64 {
		return 64
	}
	return n
}

// flushBatchSizeFromEnv reads BADGER_DUCKDB_FLUSH_BATCH_SIZE and clamps
// invalid values. This tunes memtable flush batching without changing public
// option structs.
func flushBatchSizeFromEnv() int64 {
	raw := strings.TrimSpace(os.Getenv("BADGER_DUCKDB_FLUSH_BATCH_SIZE"))
	if raw == "" {
		return defaultEnvFlushBatchSize
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return defaultEnvFlushBatchSize
	}
	if n < 1 {
		return 1
	}
	if n > 1_000_000 {
		return 1_000_000
	}
	return n
}

// directFlushBatchSizeFromEnv reads BADGER_DUCKDB_DIRECT_FLUSH_BATCH_SIZE and
// clamps invalid values. This controls periodic flushing on the direct-append
// commit path.
func directFlushBatchSizeFromEnv() int64 {
	raw := strings.TrimSpace(os.Getenv("BADGER_DUCKDB_DIRECT_FLUSH_BATCH_SIZE"))
	if raw == "" {
		return defaultDirectFlushBatchSize
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return defaultDirectFlushBatchSize
	}
	if n < 1 {
		return 1
	}
	if n > 10_000_000 {
		return 10_000_000
	}
	return n
}

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
	// pendingKeyHash is a presence-only hash set for hot point lookups
	// (Read/ReadBatch). It avoids []byte->string allocations on every read.
	// Hash collisions are safe: false positives only cause an extra flush.
	pendingKeyHash map[uint64]struct{}
	// pendingValues overlays rows still buffered in the Appender. Reads can
	// merge this overlay with SQL instead of flushing the whole partition when
	// a read-modify-write transaction touches a recently appended key.
	pendingValues map[string][]pendingValue
	// pendingRows tracks how many rows are buffered in the Appender and not yet
	// visible to SQL reads.

	// readConns/readStmts are a small dedicated pool of connections used only
	// for reads (Read, ReadBatch, ScanPrefix). A single DuckDB connection can
	// only run one statement at a time — issuing QueryContext on a connection
	// while a previous Rows from that same connection is still open panics
	// with "misuse of duckdb driver: ... with active Rows". Routing reads
	// through database/sql's normal pool avoids that by handing out whichever
	// connection happens to be free, but the pool checkout serializes on
	// sql.DB's internal mutex, which shows up under load as CPU spent on Go
	// lock contention rather than real DuckDB work.
	//
	// This per-partition pool splits the difference: readPoolSize dedicated
	// connections, each with its own prepared LIMIT-1 statement, handed out
	// via a small buffered channel scoped to this partition. Checkout is a
	// channel receive local to the partition — no cross-partition contention,
	// and no interaction with sql.DB's global pool lock — while still
	// allowing up to readPoolSize concurrent readers per partition instead of
	// serializing every reader onto one connection.
	readConns []*sql.Conn
	readStmts []*sql.Stmt
	// batchStmts is indexed by request count and then read-connection index.
	// Each connection has its own prepared statements because DuckDB statements
	// cannot be used concurrently on a connection.
	batchStmts map[int][]*sql.Stmt
	readFree   chan int // free-list of indices into readConns/readStmts
}

type pendingValue struct {
	timestamp CustomTs
	value     []byte
	deleted   bool
}

// acquireRead blocks until a read connection/statement pair is free and
// returns its index. Must be paired with a releaseRead(idx).
func (pa *partitionAppender) acquireRead() int {
	return <-pa.readFree
}

// releaseRead returns a read connection/statement pair to the free pool.
func (pa *partitionAppender) releaseRead(idx int) {
	pa.readFree <- idx
}

// flush pushes buffered rows to DuckDB and clears pending-key tracking.
// Must be called with mu held for writing.
func (pa *partitionAppender) flush() error {
	if pa.pendingRows == 0 {
		return nil
	}
	if err := pa.appender.Flush(); err != nil {
		return err
	}
	pa.pendingRows = 0
	pa.pendingKeyHash = make(map[uint64]struct{})
	pa.pendingValues = make(map[string][]pendingValue)
	return nil
}

// markPending records that a key has an unflushed row in the Appender buffer.
// Must be called with mu held for writing, immediately after a successful AppendRow.
func (pa *partitionAppender) markPending(key []byte, value []byte, timestamp CustomTs, deleted bool) {
	pa.pendingKeyHash[z.MemHash(key)] = struct{}{}
	pa.pendingValues[string(key)] = append(pa.pendingValues[string(key)], pendingValue{
		timestamp: timestamp,
		value:     append([]byte(nil), value...),
		deleted:   deleted,
	})
}

// hasPending reports whether key has any unflushed rows in the Appender buffer.
// Must be called with mu held (read or write).
func (pa *partitionAppender) hasPending(key []byte) bool {
	_, ok := pa.pendingKeyHash[z.MemHash(key)]
	return ok
}

func (pa *partitionAppender) latestPending(key []byte, readTs CustomTs) (pendingValue, bool) {
	values := pa.pendingValues[string(key)]
	var latest pendingValue
	found := false
	for _, value := range values {
		if !value.timestamp.LessOrEqual(readTs) {
			continue
		}
		if !found || latest.timestamp.Less(value.timestamp) {
			latest = value
			found = true
		}
	}
	return latest, found
}

// DuckDBStorage is the unified DuckDB storage implementation.
type DuckDBStorage struct {
	db       *sql.DB
	ctx      context.Context
	partCalc *partitionCalculator
	mu       sync.RWMutex
	numParts int
	// partAppenders holds one persistent Appender per partition, along with
	// each partition's dedicated read-connection pool (partitionAppender.
	// readConns/readStmts). Indexed by partition ID; created in
	// initPersistentAppenders.
	partAppenders []*partitionAppender
	// flushBatchSize is the per-partition row count that triggers an automatic
	// Appender flush.  Configurable; defaults to defaultFlushBatchSize.
	flushBatchSize int64
	// directFlushBatchSize is the per-partition row threshold for periodic
	// flushing on the direct append path.
	directFlushBatchSize int64
	// readPoolSize controls how many dedicated read connections are created per
	// partition. Tunable via BADGER_DUCKDB_READ_POOL_SIZE.
	readPoolSize int
	// numVersionsToKeep mirrors badger's Options.NumVersionsToKeep: how many
	// versions of a key CompactPartitions retains per partition. Defaults to 1
	// (keep only the latest version) if not set to a positive value.
	numVersionsToKeep int
}

// NewDuckDBStorage creates a new DuckDB storage instance with the default
// numVersionsToKeep of 1 (keep only the latest version per key on compaction).
// Prefer NewDuckDBStorageWithOptions when the caller's NumVersionsToKeep
// option is available.
func NewDuckDBStorage(dbPath string, numPartitions int) (*DuckDBStorage, error) {
	return NewDuckDBStorageWithOptions(dbPath, numPartitions, 1)
}

// NewDuckDBStorageWithOptions creates a new DuckDB storage instance.
//
// numVersionsToKeep controls how many versions per key CompactPartitions
// retains (values <= 0 are treated as 1, matching badger's own default).
//
// It also records the partition fan-out (numPartitions) in a small metadata
// table on first creation, and errors out on subsequent opens if the
// requested fan-out doesn't match what's on disk. Without this check,
// reopening an existing on-disk DuckDB DB with a different PartitionFanOut
// would silently hash keys to different partitions than the ones they were
// originally written to, making previously-written data unreadable without
// any error -- this guard converts that into a loud failure at Open() time.
func NewDuckDBStorageWithOptions(dbPath string, numPartitions int, numVersionsToKeep int) (*DuckDBStorage, error) {
	if numPartitions <= 0 {
		numPartitions = 8
	}
	if numVersionsToKeep <= 0 {
		numVersionsToKeep = 1
	}
	readPoolSize := readPoolSizeFromEnv()
	flushBatchSize := flushBatchSizeFromEnv()
	directFlushBatchSize := directFlushBatchSizeFromEnv()

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	if err := applyDuckDBRuntimePragmas(db, context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to apply duckdb runtime pragmas: %w", err)
	}

	// Keep enough room for one write conn + read pool per partition plus some
	// slack for setup/compaction operations.
	db.SetMaxOpenConns(numPartitions * (readPoolSize + 4))
	db.SetMaxIdleConns(numPartitions * (readPoolSize + 1))

	s := &DuckDBStorage{
		db:                   db,
		ctx:                  context.Background(),
		partCalc:             newPartitionCalculator(numPartitions),
		numParts:             numPartitions,
		flushBatchSize:       flushBatchSize,
		directFlushBatchSize: directFlushBatchSize,
		readPoolSize:         readPoolSize,
		numVersionsToKeep:    numVersionsToKeep,
	}

	if err := s.verifyOrRecordFanOut(numPartitions); err != nil {
		db.Close()
		return nil, err
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

// applyDuckDBRuntimePragmas applies optional runtime tuning knobs controlled by
// environment variables.
//
// Supported env vars:
//   - BADGER_DUCKDB_MEMORY_LIMIT      e.g. 14GB, 12000MB
//   - BADGER_DUCKDB_TEMP_DIRECTORY    absolute/relative path for spill files
func applyDuckDBRuntimePragmas(db *sql.DB, ctx context.Context) error {
	if db == nil {
		return nil
	}
	if memLimit := strings.TrimSpace(os.Getenv("BADGER_DUCKDB_MEMORY_LIMIT")); memLimit != "" {
		q := fmt.Sprintf("PRAGMA memory_limit='%s'", strings.ReplaceAll(memLimit, "'", "''"))
		if _, err := db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("set memory_limit=%q: %w", memLimit, err)
		}
	}
	if tempDir := strings.TrimSpace(os.Getenv("BADGER_DUCKDB_TEMP_DIRECTORY")); tempDir != "" {
		q := fmt.Sprintf("PRAGMA temp_directory='%s'", strings.ReplaceAll(tempDir, "'", "''"))
		if _, err := db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("set temp_directory=%q: %w", tempDir, err)
		}
	}
	return nil
}

// verifyOrRecordFanOut records numPartitions in a one-row metadata table the
// first time a DuckDB DB is created at this path, and on every subsequent
// open verifies the requested numPartitions still matches. Partition
// assignment (see partitionCalculator) is a pure function of key and
// numPartitions, so any mismatch means keys would hash to different
// partition tables than the ones they were written under -- silently
// corrupting reads rather than erroring. For ":memory:" databases this table
// is simply recreated empty every process start (there is no persisted state
// to disagree with), so the check is a harmless no-op in that case.
func (s *DuckDBStorage) verifyOrRecordFanOut(numPartitions int) error {
	if s.db == nil {
		return nil
	}

	const createMetaSQL = `
		CREATE TABLE IF NOT EXISTS _badger_duckdb_meta (
			key   VARCHAR NOT NULL,
			value VARCHAR NOT NULL,
			PRIMARY KEY (key)
		)`
	if _, err := s.db.ExecContext(s.ctx, createMetaSQL); err != nil {
		return fmt.Errorf("verifyOrRecordFanOut: create meta table: %w", err)
	}

	row := s.db.QueryRowContext(s.ctx,
		`SELECT value FROM _badger_duckdb_meta WHERE key = 'partition_fan_out'`)
	var stored string
	switch err := row.Scan(&stored); err {
	case sql.ErrNoRows:
		// First time this DB path has been opened -- record the fan-out.
		if _, err := s.db.ExecContext(s.ctx,
			`INSERT INTO _badger_duckdb_meta (key, value) VALUES ('partition_fan_out', ?)`,
			strconv.Itoa(numPartitions)); err != nil {
			return fmt.Errorf("verifyOrRecordFanOut: record fan-out: %w", err)
		}
		return nil
	case nil:
		storedN, convErr := strconv.Atoi(stored)
		if convErr != nil {
			return fmt.Errorf("verifyOrRecordFanOut: corrupt stored fan-out value %q: %w",
				stored, convErr)
		}
		if storedN != numPartitions {
			return fmt.Errorf(
				"partition fan-out mismatch: this DuckDB DB was created with "+
					"PartitionFanOut=%d, but Open() was called with PartitionFanOut=%d. "+
					"Reopening with a different fan-out would hash existing keys to the "+
					"wrong partition tables. Use PartitionFanOut=%d to reopen this DB, "+
					"or start a fresh DB directory to change fan-out",
				storedN, numPartitions, storedN)
		}
		return nil
	default:
		return fmt.Errorf("verifyOrRecordFanOut: read stored fan-out: %w", err)
	}
}

func readBatchSQL(tableName string, keyCount int) string {
	placeholders := make([]string, keyCount)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return fmt.Sprintf(`
		SELECT key,
		       arg_max(epoch_id, struct_pack(epoch_id := epoch_id, broker_id := broker_id, assigned_ts := assigned_ts)) AS epoch_id,
		       arg_max(broker_id, struct_pack(epoch_id := epoch_id, broker_id := broker_id, assigned_ts := assigned_ts)) AS broker_id,
		       arg_max(assigned_ts, struct_pack(epoch_id := epoch_id, broker_id := broker_id, assigned_ts := assigned_ts)) AS assigned_ts,
		       arg_max(value, struct_pack(epoch_id := epoch_id, broker_id := broker_id, assigned_ts := assigned_ts)) AS value,
		       arg_max(deleted, struct_pack(epoch_id := epoch_id, broker_id := broker_id, assigned_ts := assigned_ts)) AS deleted
		FROM %s
		WHERE key IN (%s)
		  AND (epoch_id < ? OR
		       (epoch_id = ? AND broker_id < ?) OR
		       (epoch_id = ? AND broker_id = ? AND assigned_ts <= ?))
		GROUP BY key`, tableName, strings.Join(placeholders, ", "))
}

// initPersistentAppenders opens one SQL connection and one duckdb Appender per
// partition and stores them in s.partAppenders.  These are kept alive for the
// lifetime of the storage instance; re-using them across flushes eliminates
// repeated CGo boundary crossings for connection acquisition and Appender
// construction.  It also opens a small dedicated read-connection pool per
// partition (see partitionAppender.readConns) so Read()/ReadBatch()/
// ScanPrefix() avoid both SQL re-parsing and database/sql's global pool lock.
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
			sqlConn:        sqlConn,
			pendingKeyHash: make(map[uint64]struct{}),
			pendingValues:  make(map[string][]pendingValue),
			appender:       appender,
		}

		// Open a small dedicated pool of read connections for this partition,
		// each with its own prepared LIMIT-1 statement, so every Read() /
		// ReadBatch() / ScanPrefix() call avoids both SQL re-parsing and
		// database/sql's pool-checkout lock.
		//
		// Why not just prepare on s.db (the old approach)? A statement
		// prepared via s.db.PrepareContext can run on any pooled connection,
		// but every QueryRowContext call still has to check a connection out
		// of database/sql's pool under sql.DB's internal mutex
		// (freeConn/connRequests). CPU profiling under load showed this
		// pool-checkout lock accounted for ~37% of total CPU — pure Go-side
		// contention, not DuckDB work.
		//
		// Why not a single pinned connection per partition (the first attempt
		// here)? A DuckDB connection can only run one statement at a time —
		// concurrent QueryContext calls sharing one connection panic with
		// "misuse of duckdb driver: ... with active Rows". A small
		// per-partition pool avoids the global pool lock while still allowing
		// s.readPoolSize concurrent readers per partition.
		readSQL := fmt.Sprintf(`
			SELECT key, epoch_id, broker_id, assigned_ts, value, deleted
			FROM %s
			WHERE key = ?
			  AND (epoch_id < ? OR
			       (epoch_id = ? AND broker_id < ?) OR
			       (epoch_id = ? AND broker_id = ? AND assigned_ts <= ?))
			ORDER BY epoch_id DESC, broker_id DESC, assigned_ts DESC
			LIMIT 1`, tableName)

		pa := s.partAppenders[i]
		pa.readConns = make([]*sql.Conn, s.readPoolSize)
		pa.readStmts = make([]*sql.Stmt, s.readPoolSize)
		pa.batchStmts = make(map[int][]*sql.Stmt, maxPreparedReadBatchKeys-1)
		pa.readFree = make(chan int, s.readPoolSize)
		for j := 0; j < s.readPoolSize; j++ {
			rc, err := s.db.Conn(s.ctx)
			if err != nil {
				return fmt.Errorf("partition %d: get read conn %d: %w", i, j, err)
			}
			rstmt, err := rc.PrepareContext(s.ctx, readSQL)
			if err != nil {
				rc.Close()
				return fmt.Errorf("partition %d: prepare read stmt %d: %w", i, j, err)
			}
			pa.readConns[j] = rc
			pa.readStmts[j] = rstmt
			pa.readFree <- j
			for keyCount := 2; keyCount <= maxPreparedReadBatchKeys; keyCount++ {
				stmt, err := rc.PrepareContext(s.ctx, readBatchSQL(tableName, keyCount))
				if err != nil {
					return fmt.Errorf("partition %d: prepare batch stmt %d on conn %d: %w", i, keyCount, j, err)
				}
				pa.batchStmts[keyCount] = append(pa.batchStmts[keyCount], stmt)
			}
		}
	}
	return nil
}

// FlushAllPending flushes pending rows in every partition's Appender.
func (s *DuckDBStorage) FlushAllPending() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushAllPendingLocked()
}

func (s *DuckDBStorage) flushAllPendingLocked() error {
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

// PoolStats returns the underlying database/sql connection pool's stats for
// the shared write/setup pool (s.db). It does not include the per-partition
// dedicated read-connection pools (partitionAppender.readConns), which are
// managed outside of database/sql and don't expose sql.DBStats.
func (s *DuckDBStorage) PoolStats() sql.DBStats {
	return s.db.Stats()
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
	// Appenders are protected independently by partitionAppender.mu. Keep the
	// storage-wide read lock only to exclude Flush/Compact/Close; taking the
	// exclusive lock here serialized unrelated partitions and made every
	// transaction wait behind the slowest append.
	s.mu.RLock()
	defer s.mu.RUnlock()

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
// partition and performs periodic flushes once the partition-local pending row
// count reaches s.directFlushBatchSize.
//
// Snapshot correctness is preserved by timestamp filtering + commit tracker:
// rows are written with their commit timestamp, and NewTransactionAt waits for
// all commits with ts <= readTs to complete before reads start. So making rows
// physically visible earlier than FlushAllPending does not allow a snapshot to
// observe future versions.
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
		pa.markPending(e.Key, e.Value, e.Timestamp, e.Deleted)
		pa.pendingRows++
		if pa.pendingRows >= s.directFlushBatchSize {
			if err := pa.flush(); err != nil {
				return fmt.Errorf("partition %d: periodic direct flush: %w", partition, err)
			}
		}
	}
	return nil
}

// initializeTables creates the per-partition tables.
//
// NOTE on PRIMARY KEY removal: earlier versions declared
// `PRIMARY KEY (key, epoch_id, broker_id, assigned_ts)` on this table. No
// write path in this file relies on that uniqueness constraint for upsert
// semantics -- every write goes through the Appender API as a plain append,
// never an INSERT ... ON CONFLICT. The constraint's only effect was forcing
// DuckDB to run a full-table dedup scan on every appended row, which made
// BenchmarkDbGrowth (repeated write/delete/compact cycles) quadratic in the
// number of accumulated rows. Removing the constraint and replacing it with
// a plain (non-unique) index on `key` keeps point-lookup performance
// (Read/ReadBatch/ScanPrefix all filter or join on `key` first) while
// dropping the per-insert uniqueness check. Duplicate (key, epoch_id,
// broker_id, assigned_ts) rows can now accumulate between compactions; this
// is safe because every read path (Read, ReadBatch, ScanPrefix) already
// selects via `ORDER BY epoch_id DESC, broker_id DESC, assigned_ts DESC
// LIMIT 1` / `ROW_NUMBER() ... WHERE rn = 1` rather than assuming at most one
// row can match, and CompactPartitions' ROW_NUMBER()-based dedup already
// tolerates duplicates by construction.
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
				deleted BOOLEAN NOT NULL DEFAULT FALSE
			)`, tableName)
		if _, err := s.db.ExecContext(s.ctx, createSQL); err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}

		indexSQL := fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_%s_key ON %s (key)`, tableName, tableName)
		if _, err := s.db.ExecContext(s.ctx, indexSQL); err != nil {
			return fmt.Errorf("failed to create index on %s: %w", tableName, err)
		}
	}
	return nil
}

// Read retrieves the latest value for a key with timestamp <= readTs.
//
// The partition read lock covers the SQL lookup and pending-write overlay.
// This preserves snapshot visibility without flushing unrelated buffered rows
// whenever a read-modify-write transaction touches a recent key.
func (s *DuckDBStorage) Read(key []byte, readTs CustomTs) (*Entry, error) {
	partition := s.partCalc.getPartition(key)
	pa := s.partAppenders[partition]
	// Check out the connection before taking pa.mu. Waiting for a free read
	// connection while holding pa.mu.RLock can deadlock a writer that needs
	// pa.mu.Lock to flush pending appender rows.
	ridx := pa.acquireRead()
	defer pa.releaseRead(ridx)

	pa.mu.RLock()
	defer pa.mu.RUnlock()

	var entry Entry
	var epochID, brokerID, assignedTs int64
	var deleted bool

	// Check out one of this partition's dedicated read connections/statements
	// (pre-compiled, to avoid SQL re-parsing overhead) instead of going
	// through database/sql's global pool.
	err := pa.readStmts[ridx].QueryRowContext(
		s.ctx,
		key,
		readTs.EpochID,
		readTs.EpochID, readTs.BrokerID,
		readTs.EpochID, readTs.BrokerID, readTs.AssignedTs,
	).Scan(&entry.Key, &epochID, &brokerID, &assignedTs, &entry.Value, &deleted)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	var sqlEntry *Entry
	var sqlTimestamp CustomTs
	sqlFound := err == nil
	if sqlFound {
		sqlTimestamp = CustomTs{EpochID: epochID, BrokerID: brokerID, AssignedTs: assignedTs}
	}
	if sqlFound && !deleted {
		sqlEntry = &entry
		sqlEntry.Timestamp = CustomTs{EpochID: epochID, BrokerID: brokerID, AssignedTs: assignedTs}
	}
	if pending, ok := pa.latestPending(key, readTs); ok && (!sqlFound || sqlTimestamp.Less(pending.timestamp)) {
		if pending.deleted {
			return nil, nil
		}
		return &Entry{Key: key, Value: pending.value, Timestamp: pending.timestamp}, nil
	}
	return sqlEntry, nil
}

// ReadBatchRequest specifies a single key lookup within a ReadBatch call.
type ReadBatchRequest struct {
	Key    []byte
	ReadTs CustomTs
}

// ReadBatchResult is the result for one key in a ReadBatch call.
// Value is nil when the key is not found or was deleted.
type ReadBatchResult struct {
	Key       []byte
	Value     []byte
	Timestamp CustomTs
	Found     bool
}

// ReadBatch retrieves the latest value for multiple keys in as few SQL queries
// as possible. Keys are grouped by partition; a partition with only one
// requested key uses the pre-compiled LIMIT 1 prepared statement. A partition
// with multiple keys issues a single IN-clause query with ROW_NUMBER() OVER
// (PARTITION BY key) so DuckDB fetches the latest visible row for every key
// in one round-trip instead of N separate queries.
//
// The returned slice is in the same order as the input requests.
// If a key is not found its ReadBatchResult has Found=false and Value=nil.
func (s *DuckDBStorage) ReadBatch(requests []ReadBatchRequest) ([]ReadBatchResult, error) {
	if len(requests) == 0 {
		return nil, nil
	}

	results := make([]ReadBatchResult, len(requests))
	seenSQL := make([]bool, len(requests))
	for i, req := range requests {
		results[i].Key = req.Key
	}

	// key string → slice of result indices (a key may appear more than once).
	keyToIndices := make(map[string][]int, len(requests))
	for i, req := range requests {
		k := string(req.Key)
		keyToIndices[k] = append(keyToIndices[k], i)
	}

	// Group requests by partition.
	type partReq struct {
		key    []byte
		readTs CustomTs
		idx    int
	}
	partGroups := make(map[int][]partReq)
	for i, req := range requests {
		pid := s.partCalc.getPartition(req.Key)
		partGroups[pid] = append(partGroups[pid], partReq{req.Key, req.ReadTs, i})
	}

	for pid, reqs := range partGroups {
		pa := s.partAppenders[pid]
		// Acquire before pa.mu for the same reason as Read. A full read pool
		// must never strand a reader lock needed by the writer/flush path.
		ridx := pa.acquireRead()

		// Hold the partition read lock for the SQL query and pending-write
		// overlay. Appender rows remain buffered; no partition-wide flush is
		// needed just because this transaction reads a recent key.
		pa.mu.RLock()

		// All requests in a transaction share the same readTs.
		readTs := reqs[0].readTs

		if len(reqs) == 1 {
			// Single key — fast path: use the pre-compiled LIMIT 1 statement.
			r := reqs[0]
			var (
				key                   []byte
				epochID, brokerID, ts int64
				value                 []byte
				deleted               bool
			)
			err := pa.readStmts[ridx].QueryRowContext(
				s.ctx,
				r.key,
				readTs.EpochID,
				readTs.EpochID, readTs.BrokerID,
				readTs.EpochID, readTs.BrokerID, readTs.AssignedTs,
			).Scan(&key, &epochID, &brokerID, &ts, &value, &deleted)
			if err == nil && !deleted {
				seenSQL[r.idx] = true
				results[r.idx].Found = true
				results[r.idx].Value = value
				results[r.idx].Timestamp = CustomTs{EpochID: epochID, BrokerID: brokerID, AssignedTs: ts}
			} else if err == nil {
				seenSQL[r.idx] = true
				results[r.idx].Timestamp = CustomTs{EpochID: epochID, BrokerID: brokerID, AssignedTs: ts}
			}
			if pending, ok := pa.latestPending(r.key, readTs); ok &&
				(!seenSQL[r.idx] || results[r.idx].Timestamp.Less(pending.timestamp)) {
				results[r.idx].Found = !pending.deleted
				results[r.idx].Value = pending.value
				results[r.idx].Timestamp = pending.timestamp
			}
			pa.mu.RUnlock()
			// Return the connection on every single-key path, including the
			// successful and sql.ErrNoRows cases. Leaking it here eventually
			// exhausts the per-partition pool and blocks every later transaction
			// in acquireRead().
			pa.releaseRead(ridx)
			if err == sql.ErrNoRows {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("ReadBatch scan partition %d key %q: %w", pid, r.key, err)
			}
			continue
		}

		// Multiple keys in the same partition — single IN-clause query with
		// ROW_NUMBER() OVER (PARTITION BY key) fetches the latest visible row
		// for every key in one SQL round-trip.
		tableName := fmt.Sprintf("partition_%d", pid)
		args := make([]interface{}, 0, len(reqs)+6)
		for _, r := range reqs {
			args = append(args, r.key)
		}
		args = append(args,
			readTs.EpochID,
			readTs.EpochID, readTs.BrokerID,
			readTs.EpochID, readTs.BrokerID, readTs.AssignedTs,
		)
		var prepared *sql.Stmt
		if stmts, ok := pa.batchStmts[len(reqs)]; ok && ridx < len(stmts) {
			prepared = stmts[ridx]
		}

		// Run the prepared common-size query when available. Larger requests use
		// the dynamic fallback, still on one of this partition's dedicated
		// read connections rather than
		// s.db, to avoid database/sql's global pool-checkout lock. The
		// connection stays checked out until rows.Close() below — a single
		// DuckDB connection can only serve one open Rows at a time.
		var rows *sql.Rows
		var err error
		if prepared != nil {
			rows, err = prepared.QueryContext(s.ctx, args...)
		} else {
			rows, err = pa.readConns[ridx].QueryContext(s.ctx, readBatchSQL(tableName, len(reqs)), args...)
		}
		if err != nil {
			pa.mu.RUnlock()
			pa.releaseRead(ridx)
			return nil, fmt.Errorf("ReadBatch query partition %d: %w", pid, err)
		}
		for rows.Next() {
			var (
				key                   []byte
				epochID, brokerID, ts int64
				value                 []byte
				deleted               bool
			)
			if err := rows.Scan(&key, &epochID, &brokerID, &ts, &value, &deleted); err != nil {
				_ = rows.Close()
				pa.mu.RUnlock()
				pa.releaseRead(ridx)
				return nil, fmt.Errorf("ReadBatch scan partition %d: %w", pid, err)
			}
			for _, idx := range keyToIndices[string(key)] {
				seenSQL[idx] = true
				results[idx].Timestamp = CustomTs{EpochID: epochID, BrokerID: brokerID, AssignedTs: ts}
			}
			if deleted {
				continue
			}
			for _, idx := range keyToIndices[string(key)] {
				results[idx].Found = true
				results[idx].Value = value
				results[idx].Timestamp = CustomTs{EpochID: epochID, BrokerID: brokerID, AssignedTs: ts}
			}
		}
		closeErr := rows.Close()
		for _, r := range reqs {
			pending, ok := pa.latestPending(r.key, readTs)
			if !ok {
				continue
			}
			current := results[r.idx]
			if !seenSQL[r.idx] || current.Timestamp.Less(pending.timestamp) {
				current.Found = !pending.deleted
				current.Value = pending.value
				current.Timestamp = pending.timestamp
				results[r.idx] = current
			}
		}
		pa.mu.RUnlock()
		pa.releaseRead(ridx)
		if closeErr != nil {
			return nil, fmt.Errorf("ReadBatch close rows partition %d: %w", pid, closeErr)
		}
	}

	return results, nil
}

// prefixUpperBound returns the smallest byte slice that is greater than every
// key beginning with prefix, or nil when no finite bound exists (prefix is
// empty or all 0xff).
func prefixUpperBound(prefix []byte) []byte {
	ub := append([]byte(nil), prefix...)
	for i := len(ub) - 1; i >= 0; i-- {
		if ub[i] < 0xff {
			ub[i]++
			return ub[:i+1]
		}
	}
	return nil
}

// ScanPrefix returns the latest visible (version <= readTs, non-deleted) value
// for every key that starts with prefix, across all partitions.
//
// Motivation: aggregate checks (e.g. the bank SUM_CHECK) previously issued one
// point SELECT per key — 1,000 keys = 1,000 CGo round-trips through
// database/sql, which CPU profiling showed was ~78% of all CPU in the stress
// test. ScanPrefix replaces that with ONE query per partition.
//
// Locking matches Read/ReadBatch: per partition, hold RLock through the query;
// if there are any pending (unflushed) rows in that partition, upgrade to the
// write lock, flush, and query under the write lock. This intentionally trades
// a slightly coarser flush decision for lower write-path overhead by avoiding
// per-write key-string bookkeeping solely for prefix matching.
//
// Callers needing snapshot consistency across partitions must first pass the
// NewTransactionAt read barrier (duckDBTracker.waitUntil) with the same readTs,
// exactly as for Read.
func (s *DuckDBStorage) ScanPrefix(prefix []byte, readTs CustomTs) ([]ReadBatchResult, error) {
	ub := prefixUpperBound(prefix)
	var out []ReadBatchResult

	for pid := 0; pid < s.numParts; pid++ {
		pa := s.partAppenders[pid]
		// Acquire before pa.mu so read-pool pressure cannot hold a lock that
		// the writer must acquire in order to flush pending rows.
		ridx := pa.acquireRead()

		pa.mu.RLock()
		needFlush := pa.pendingRows > 0

		if needFlush {
			// Upgrade to write lock (no atomic upgrade in Go).
			pa.mu.RUnlock()
			pa.mu.Lock()
			still := pa.pendingRows > 0
			if still {
				if err := pa.flush(); err != nil {
					pa.mu.Unlock()
					pa.releaseRead(ridx)
					return nil, fmt.Errorf("ScanPrefix flush partition %d: %w", pid, err)
				}
			}
		}

		tableName := fmt.Sprintf("partition_%d", pid)
		keyCond := "key >= ?"
		args := []interface{}{prefix}
		if ub != nil {
			keyCond += " AND key < ?"
			args = append(args, ub)
		}
		args = append(args,
			readTs.EpochID,
			readTs.EpochID, readTs.BrokerID,
			readTs.EpochID, readTs.BrokerID, readTs.AssignedTs,
		)
		querySQL := fmt.Sprintf(`
			SELECT key, epoch_id, broker_id, assigned_ts, value, deleted
			FROM (
				SELECT key, epoch_id, broker_id, assigned_ts, value, deleted,
				       ROW_NUMBER() OVER (
				           PARTITION BY key
				           ORDER BY epoch_id DESC, broker_id DESC, assigned_ts DESC
				       ) AS rn
				FROM %s
				WHERE %s
				  AND (epoch_id < ? OR
				       (epoch_id = ? AND broker_id < ?) OR
				       (epoch_id = ? AND broker_id = ? AND assigned_ts <= ?))
			) sub
			WHERE rn = 1`, tableName, keyCond)

		// Same pool-bypass as ReadBatch: check out one of this partition's
		// dedicated read connections instead of s.db, to avoid database/sql's
		// pool-checkout lock on the hot scan path. Held until rows.Close().
		rows, err := pa.readConns[ridx].QueryContext(s.ctx, querySQL, args...)
		if needFlush {
			pa.mu.Unlock()
		} else {
			pa.mu.RUnlock()
		}
		if err != nil {
			pa.releaseRead(ridx)
			return nil, fmt.Errorf("ScanPrefix query partition %d: %w", pid, err)
		}
		for rows.Next() {
			var (
				key                   []byte
				epochID, brokerID, ts int64
				value                 []byte
				deleted               bool
			)
			if err := rows.Scan(&key, &epochID, &brokerID, &ts, &value, &deleted); err != nil {
				_ = rows.Close()
				pa.releaseRead(ridx)
				return nil, fmt.Errorf("ScanPrefix scan partition %d: %w", pid, err)
			}
			if deleted {
				continue
			}
			out = append(out, ReadBatchResult{
				Key:       key,
				Value:     value,
				Timestamp: CustomTs{EpochID: epochID, BrokerID: brokerID, AssignedTs: ts},
				Found:     true,
			})
		}
		closeErr := rows.Close()
		pa.releaseRead(ridx)
		if closeErr != nil {
			return nil, fmt.Errorf("ScanPrefix close rows partition %d: %w", pid, closeErr)
		}
	}
	return out, nil
}

// FlushDarshanEntries writes a batch of entries to DuckDB.
func (s *DuckDBStorage) FlushDarshanEntries(entries []*DarshanEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

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
		pa.markPending(e.Key, e.Value, e.Timestamp, e.Deleted)
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

// CompactPartitions removes superseded versions beyond numVersionsToKeep and
// runs VACUUM.
//
// Retention semantics mirror badger's own Options.NumVersionsToKeep: with the
// default of 1, only the single latest version per key survives (as before);
// with NumVersionsToKeep > 1, the N most recent versions per key are kept and
// only versions beyond that window are deleted. Snapshot reads at older
// timestamps within the retained window continue to work; reads older than
// the retained window still return "not found" post-compaction, same as
// before this change for the NumVersionsToKeep=1 case.
func (s *DuckDBStorage) CompactPartitions() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ensure all buffered rows are visible to the DELETE queries below.
	if err := s.flushAllPendingLocked(); err != nil {
		return fmt.Errorf("compact: flush pending: %w", err)
	}

	keep := s.numVersionsToKeep
	if keep <= 0 {
		keep = 1
	}

	for i := 0; i < s.numParts; i++ {
		tableName := fmt.Sprintf("partition_%d", i)
		// Delete all rows ranked beyond `keep` per key.
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
				) sub WHERE rn <= %d
			)`, tableName, tableName, keep)
		if _, err := s.db.ExecContext(s.ctx, deleteSQL); err != nil {
			return fmt.Errorf("compact %s: %w", tableName, err)
		}
	}
	_, err := s.db.ExecContext(s.ctx, "VACUUM")
	return err
}

// Close releases all DuckDB resources.
func (s *DuckDBStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	setErr := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Flush + close persistent appenders before closing the underlying DB;
	// appender.Close() flushes any remaining buffered rows.
	for i, pa := range s.partAppenders {
		pa.mu.Lock()
		if err := pa.flush(); err != nil {
			setErr(fmt.Errorf("close flush partition %d: %w", i, err))
		}
		if pa.appender != nil {
			if err := pa.appender.Close(); err != nil {
				setErr(fmt.Errorf("close appender for partition %d: %w", i, err))
			}
			pa.appender = nil
		}
		if pa.sqlConn != nil {
			if err := pa.sqlConn.Close(); err != nil {
				setErr(fmt.Errorf("close sql conn partition %d: %w", i, err))
			}
			pa.sqlConn = nil
		}
		// Close this partition's dedicated read-connection pool.
		for j, stmt := range pa.readStmts {
			if stmt != nil {
				if err := stmt.Close(); err != nil {
					setErr(fmt.Errorf("close read stmt partition %d idx %d: %w", i, j, err))
				}
			}
			for keyCount, stmts := range pa.batchStmts {
				if j < len(stmts) && stmts[j] != nil {
					if err := stmts[j].Close(); err != nil {
						setErr(fmt.Errorf("close batch stmt partition %d keys %d idx %d: %w", i, keyCount, j, err))
					}
				}
			}
			if pa.readConns[j] != nil {
				if err := pa.readConns[j].Close(); err != nil {
					setErr(fmt.Errorf("close read conn partition %d idx %d: %w", i, j, err))
				}
			}
		}
		pa.readStmts = nil
		pa.readConns = nil
		pa.pendingRows = 0
		pa.pendingKeyHash = nil
		pa.mu.Unlock()
	}
	if err := s.db.Close(); err != nil {
		setErr(fmt.Errorf("close duckdb sql.DB: %w", err))
	}
	return firstErr
}

// ---------------------------------------------------------------------------
// partitionCalculator (unexported)
// ---------------------------------------------------------------------------

type partitionCalculator struct {
	numPartitions int
}

// stableHash64 computes a deterministic 64-bit FNV-1a hash. Unlike runtime
// memhash-based helpers, this is stable across processes and restarts, which
// is required for persistent on-disk partition routing.
func stableHash64(b []byte) uint64 {
	const (
		offset64 = 1469598103934665603
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime64
	}
	return h
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
	// Co-locate related keys: if the key contains ':', hash only the prefix
	// (bytes before the first ':') so that e.g. "42:accounts_id",
	// "42:savings_bal", "42:checking_bal" all land in the same partition.
	// Keys without ':' are hashed in full (backward-compatible).
	partKey := key
	if idx := bytes.IndexByte(key, ':'); idx >= 0 {
		partKey = key[:idx]
	}
	return int(stableHash64(partKey) % uint64(pc.numPartitions))
}
