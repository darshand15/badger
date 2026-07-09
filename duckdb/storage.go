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

// directFlushBatchSize is the per-partition row threshold for the
// per-commit DirectAppendEntries path.  Each transaction commit appends
// its rows to the Appender buffer; a CGo Appender.Flush() is issued only
// once this many rows have accumulated, amortising the fixed CGo cost across
// many commits.
//
// Correctness note: the race between logical commit registration and physical
// DuckDB write is handled at the DB level by holding writeChLock for the
// full oracle→DirectFlush window, and by having NewTransactionAt (DuckDB
// mode) acquire+release writeChLock as a read barrier before any reads.
// The pendingKeys mechanism then ensures keys buffered in the Appender are
// flushed before the SQL query that needs them.
const directFlushBatchSize int64 = 512

// defaultReadPoolSize is the default number of dedicated read connections kept per
// partition (see partitionAppender.readConns). A single DuckDB connection
// handles one statement at a time, so this bounds how many readers can be
// concurrently active against one partition without serializing on either
// a single shared connection or database/sql's global pool lock.
// Tuned on Apple silicon using the Ashley sweep harness.
const defaultReadPoolSize = 2

// defaultEnvFlushBatchSize is the fallback flush threshold for the memtable
// flush path when BADGER_DUCKDB_FLUSH_BATCH_SIZE is unset/invalid.
const defaultEnvFlushBatchSize int64 = defaultFlushBatchSize

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
	// pendingKeys stores exact keys for prefix detection in ScanPrefix.
	pendingKeys map[string]struct{} // keys with unflushed AppendRow'd rows

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
	readFree  chan int // free-list of indices into readConns/readStmts
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
	pa.pendingKeyHash = make(map[uint64]struct{})
	pa.pendingKeys = make(map[string]struct{})
	return nil
}

// markPending records that a key has an unflushed row in the Appender buffer.
// Must be called with mu held for writing, immediately after a successful AppendRow.
func (pa *partitionAppender) markPending(key []byte) {
	pa.pendingKeyHash[z.MemHash(key)] = struct{}{}
	pa.pendingKeys[string(key)] = struct{}{}
}

// hasPending reports whether key has any unflushed rows in the Appender buffer.
// Must be called with mu held (read or write).
func (pa *partitionAppender) hasPending(key []byte) bool {
	_, ok := pa.pendingKeyHash[z.MemHash(key)]
	return ok
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
	// readPoolSize controls how many dedicated read connections are created per
	// partition. Tunable via BADGER_DUCKDB_READ_POOL_SIZE.
	readPoolSize int
}

// NewDuckDBStorage creates a new DuckDB storage instance.
func NewDuckDBStorage(dbPath string, numPartitions int) (*DuckDBStorage, error) {
	if numPartitions <= 0 {
		numPartitions = 8
	}
	readPoolSize := readPoolSizeFromEnv()
	flushBatchSize := flushBatchSizeFromEnv()

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	// Keep enough room for one write conn + read pool per partition plus some
	// slack for setup/compaction operations.
	db.SetMaxOpenConns(numPartitions * (readPoolSize + 4))
	db.SetMaxIdleConns(numPartitions * (readPoolSize + 1))

	s := &DuckDBStorage{
		db:             db,
		ctx:            context.Background(),
		partCalc:       newPartitionCalculator(numPartitions),
		numParts:       numPartitions,
		flushBatchSize: flushBatchSize,
		readPoolSize:   readPoolSize,
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
			appender:       appender,
			pendingKeys:    make(map[string]struct{}),
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
// partition.  No automatic flush is triggered here — all rows stay in the
// Appender (tracked by pendingKeys) until a Read() flushes them on demand.
//
// Why no auto-flush: DirectAppendEntries fans out one goroutine per partition.
// If partition P1 auto-flushed mid-fan-out, its rows would become visible in
// DuckDB SQL while partition P2's rows for the same transaction are still
// buffered.  A concurrent reader could then observe from=new (P1) and to=old
// (P2) for the same committed transaction — an inconsistent snapshot that
// bypasses conflict detection when commitTs ≤ readTs.  Keeping all rows in
// pendingKeys until a read explicitly flushes them preserves the invariant
// that every key written by a transaction is equally visible or invisible to
// any snapshot query.
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
// Correctness invariant: the partition lock (pa.mu) is held for the entire
// window from the pendingKeys check through the SQL query.  This closes the
// TOCTTOU race where a concurrent appendPartitionDirect could write the key
// into the Appender buffer between the check and the query, making the SQL
// result stale.
//
// Lock protocol:
//   - Key NOT in pendingKeys: hold RLock through the SQL query.  Multiple
//     concurrent readers proceed in parallel; writers block until every
//     in-progress reader finishes.
//   - Key IS in pendingKeys: upgrade to write-lock, flush (so all buffered
//     rows are now in DuckDB), then run the SQL query while still holding
//     the write-lock.  The write-lock prevents a new concurrent write with
//     commitTs ≤ readTs from landing in the Appender between flush and query.
func (s *DuckDBStorage) Read(key []byte, readTs CustomTs) (*Entry, error) {
	partition := s.partCalc.getPartition(key)
	pa := s.partAppenders[partition]

	pa.mu.RLock()
	needFlush := pa.hasPending(key)

	if needFlush {
		// Upgrade to write-lock: release RLock first (Go sync.RWMutex does not
		// support atomic upgrade).
		pa.mu.RUnlock()
		pa.mu.Lock()
		defer pa.mu.Unlock()

		// Re-check: another goroutine may have already flushed while we
		// re-acquired the lock.
		if pa.hasPending(key) {
			if err := pa.flush(); err != nil {
				return nil, fmt.Errorf("flush pending before read: %w", err)
			}
		}
		// Fall through to SQL query while holding write-lock.
	} else {
		// Hold RLock through the SQL query so no concurrent write can slip in
		// between the pendingKeys check and the query.
		defer pa.mu.RUnlock()
	}

	var entry Entry
	var epochID, brokerID, assignedTs int64
	var deleted bool

	// Check out one of this partition's dedicated read connections/statements
	// (pre-compiled, to avoid SQL re-parsing overhead) instead of going
	// through database/sql's global pool.
	ridx := pa.acquireRead()
	err := pa.readStmts[ridx].QueryRowContext(
		s.ctx,
		key,
		readTs.EpochID,
		readTs.EpochID, readTs.BrokerID,
		readTs.EpochID, readTs.BrokerID, readTs.AssignedTs,
	).Scan(&entry.Key, &epochID, &brokerID, &assignedTs, &entry.Value, &deleted)
	pa.releaseRead(ridx)

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

		// Hold the partition lock for the entire check+flush+query window to
		// close the TOCTTOU race (same invariant as Read): no concurrent
		// appendPartitionDirect can slip a row into the Appender buffer between
		// our pendingKeys check and the SQL query.
		pa.mu.RLock()
		needFlush := false
		for _, r := range reqs {
			if pa.hasPending(r.key) {
				needFlush = true
				break
			}
		}

		if needFlush {
			// Upgrade to write-lock.
			pa.mu.RUnlock()
			pa.mu.Lock()
			// Re-check after lock upgrade.
			stillNeedsFlush := false
			for _, r := range reqs {
				if pa.hasPending(r.key) {
					stillNeedsFlush = true
					break
				}
			}
			if stillNeedsFlush {
				if err := pa.flush(); err != nil {
					pa.mu.Unlock()
					return nil, fmt.Errorf("ReadBatch flush partition %d: %w", pid, err)
				}
			}
			// SQL queries below run under write-lock; released after each query.
		}
		// If needFlush==false we still hold RLock through the SQL query.

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
			ridx := pa.acquireRead()
			err := pa.readStmts[ridx].QueryRowContext(
				s.ctx,
				r.key,
				readTs.EpochID,
				readTs.EpochID, readTs.BrokerID,
				readTs.EpochID, readTs.BrokerID, readTs.AssignedTs,
			).Scan(&key, &epochID, &brokerID, &ts, &value, &deleted)
			pa.releaseRead(ridx)
			if needFlush {
				pa.mu.Unlock()
			} else {
				pa.mu.RUnlock()
			}
			if err == sql.ErrNoRows {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("ReadBatch scan partition %d key %q: %w", pid, r.key, err)
			}
			if !deleted {
				results[r.idx].Found = true
				results[r.idx].Value = value
				results[r.idx].Timestamp = CustomTs{EpochID: epochID, BrokerID: brokerID, AssignedTs: ts}
			}
			continue
		}

		// Multiple keys in the same partition — single IN-clause query with
		// ROW_NUMBER() OVER (PARTITION BY key) fetches the latest visible row
		// for every key in one SQL round-trip.
		tableName := fmt.Sprintf("partition_%d", pid)
		placeholders := make([]string, len(reqs))
		args := make([]interface{}, 0, len(reqs)+6)
		for i, r := range reqs {
			placeholders[i] = "?"
			args = append(args, r.key)
		}
		inClause := strings.Join(placeholders, ", ")
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
				WHERE key IN (%s)
				  AND (epoch_id < ? OR
				       (epoch_id = ? AND broker_id < ?) OR
				       (epoch_id = ? AND broker_id = ? AND assigned_ts <= ?))
			) sub
			WHERE rn = 1`, tableName, inClause)

		// Ad hoc IN-clause SQL varies per call (placeholder count depends on
		// len(reqs)), so it can't use a pre-prepared statement. Still, run it
		// on one of this partition's dedicated read connections rather than
		// s.db, to avoid database/sql's global pool-checkout lock. The
		// connection stays checked out until rows.Close() below — a single
		// DuckDB connection can only serve one open Rows at a time.
		ridx := pa.acquireRead()
		rows, err := pa.readConns[ridx].QueryContext(s.ctx, querySQL, args...)
		if needFlush {
			pa.mu.Unlock()
		} else {
			pa.mu.RUnlock()
		}
		if err != nil {
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
				pa.releaseRead(ridx)
				return nil, fmt.Errorf("ReadBatch scan partition %d: %w", pid, err)
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
// if any pending (unflushed) key matches the prefix, upgrade to the write lock,
// flush, and query under the write lock. Callers needing snapshot consistency
// across partitions must first pass the NewTransactionAt read barrier
// (duckDBTracker.waitUntil) with the same readTs, exactly as for Read.
func (s *DuckDBStorage) ScanPrefix(prefix []byte, readTs CustomTs) ([]ReadBatchResult, error) {
	ub := prefixUpperBound(prefix)
	prefixStr := string(prefix)
	var out []ReadBatchResult

	for pid := 0; pid < s.numParts; pid++ {
		pa := s.partAppenders[pid]

		pa.mu.RLock()
		needFlush := false
		for k := range pa.pendingKeys {
			if strings.HasPrefix(k, prefixStr) {
				needFlush = true
				break
			}
		}

		if needFlush {
			// Upgrade to write lock (no atomic upgrade in Go).
			pa.mu.RUnlock()
			pa.mu.Lock()
			still := false
			for k := range pa.pendingKeys {
				if strings.HasPrefix(k, prefixStr) {
					still = true
					break
				}
			}
			if still {
				if err := pa.flush(); err != nil {
					pa.mu.Unlock()
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
		ridx := pa.acquireRead()
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
		// Close this partition's dedicated read-connection pool.
		for j, stmt := range pa.readStmts {
			if stmt != nil {
				_ = stmt.Close()
			}
			if pa.readConns[j] != nil {
				_ = pa.readConns[j].Close()
			}
		}
		pa.readStmts = nil
		pa.readConns = nil
		pa.pendingRows = 0
		pa.pendingKeyHash = nil
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
	// Co-locate related keys: if the key contains ':', hash only the prefix
	// (bytes before the first ':') so that e.g. "42:accounts_id",
	// "42:savings_bal", "42:checking_bal" all land in the same partition.
	// Keys without ':' are hashed in full (backward-compatible).
	partKey := key
	if idx := bytes.IndexByte(key, ':'); idx >= 0 {
		partKey = key[:idx]
	}
	return int(z.MemHash(partKey) % uint64(pc.numPartitions))
}
