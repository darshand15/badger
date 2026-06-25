//go:build duckdb

/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"fmt"
	"math"
	"sync"

	"github.com/dgraph-io/badger/v4/duckdb"
	"github.com/dgraph-io/badger/v4/types"
	"github.com/dgraph-io/badger/v4/y"
)

// toDuckTs converts a types.CustomTs (uint32 fields) to a duckdb.CustomTs
// (int64 fields) suitable for DuckDB appender and SQL queries.
func toDuckTs(ts types.CustomTs) duckdb.CustomTs {
	return duckdb.CustomTs{
		EpochID:    int64(ts.EpochID),
		BrokerID:   int64(ts.BrokerID),
		AssignedTs: int64(ts.AssignedTs),
	}
}

// makeDivyTs maps the Badger types.CustomTs version directly to a duckdb.CustomTs
// so that DuckDB's lexicographic (epoch_id, broker_id, assigned_ts) ordering
// matches Badger's MVCC snapshot semantics exactly.
func makeDivyTs(version types.CustomTs) duckdb.CustomTs {
	return toDuckTs(version)
}

// makeDivyTsFast is the hot-path variant — same as makeDivyTs.
func makeDivyTsFast(version types.CustomTs) duckdb.CustomTs {
	return toDuckTs(version)
}

// duckDBWriteTask encapsulates write operations routed to the isolated worker thread.
type duckDBWriteTask struct {
	entries  []duckEntry
	isDirect bool
	done     chan error
}

// duckDBStorageWrapper wraps *duckdb.DuckDBStorage to implement duckDBIface.
type duckDBStorageWrapper struct {
	s       *duckdb.DuckDBStorage
	writeCh chan duckDBWriteTask
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// newDuckDBBackend creates a DuckDB-backed storage implementation.
func newDuckDBBackend(path string, parts int) (duckDBIface, error) {
	s, err := duckdb.NewDuckDBStorage(path, parts)
	if err != nil {
		return nil, err
	}
	w := &duckDBStorageWrapper{
		s:       s,
		writeCh: make(chan duckDBWriteTask, 4096), // Robust buffer bounds for stress tests
		closeCh: make(chan struct{}),
	}
	w.wg.Add(1)
	go w.duckDBWriteWorker()
	return w, nil
}

// duckDBWriteWorker processes all writes sequentially to completely eliminate
// DuckDB's cross-goroutine transactional deadlocks.
func (w *duckDBStorageWrapper) duckDBWriteWorker() {
	defer w.wg.Done()
	for {
		select {
		case task := <-w.writeCh:
			var err error
			if task.isDirect {
				darshanEntries := make([]*duckdb.DarshanEntry, 0, len(task.entries))
				for _, e := range task.entries {
					darshanEntries = append(darshanEntries, &duckdb.DarshanEntry{
						Key:       e.Key,
						Value:     e.Value,
						Deleted:   e.Deleted,
						Timestamp: makeDivyTsFast(e.Version),
						Version:   uint64(e.Version.EpochID),
					})
				}
				err = w.s.DirectAppendEntries(darshanEntries)
			} else {
				darshanEntries := make([]*duckdb.DarshanEntry, len(task.entries))
				for i, e := range task.entries {
					darshanEntries[i] = &duckdb.DarshanEntry{
						Key:       e.Key,
						Value:     e.Value,
						Version:   uint64(e.Version.EpochID),
						Deleted:   e.Deleted,
						Timestamp: makeDivyTs(e.Version),
					}
				}
				err = w.s.FlushDarshanEntries(darshanEntries)
			}
			// Respond to block barrier ensuring strict durability/visibility
			task.done <- err

		case <-w.closeCh:
			return
		}
	}
}

func (w *duckDBStorageWrapper) Read(key []byte, readTs types.CustomTs) ([]byte, types.CustomTs, error) {
	dts := toDuckTs(readTs)
	if dts.EpochID < 0 {
		dts.EpochID = math.MaxInt64
	}
	if dts.BrokerID < 0 {
		dts.BrokerID = math.MaxInt64
	}
	if dts.AssignedTs < 0 {
		dts.AssignedTs = math.MaxInt64
	}

	entry, err := w.s.Read(key, dts)
	if err != nil || entry == nil {
		return nil, types.CustomTs{}, err
	}
	retTs := types.CustomTs{
		EpochID:    uint32(entry.Timestamp.EpochID),
		BrokerID:   uint32(entry.Timestamp.BrokerID),
		AssignedTs: uint32(entry.Timestamp.AssignedTs),
	}
	return entry.Value, retTs, nil
}

func (w *duckDBStorageWrapper) FlushEntries(entries []duckEntry) error {
	if len(entries) == 0 {
		return nil
	}
	done := make(chan error, 1)
	w.writeCh <- duckDBWriteTask{entries: entries, isDirect: false, done: done}
	return <-done
}

func (w *duckDBStorageWrapper) DirectFlush(entries []duckEntry) error {
	if len(entries) == 0 {
		return nil
	}
	done := make(chan error, 1)
	w.writeCh <- duckDBWriteTask{entries: entries, isDirect: true, done: done}
	return <-done
}

func (w *duckDBStorageWrapper) ScanPrefix(prefix []byte, readTs types.CustomTs) ([]duckReadBatchResult, error) {
	dts := toDuckTs(readTs)
	if dts.EpochID < 0 {
		dts.EpochID = math.MaxInt64
	}
	if dts.BrokerID < 0 {
		dts.BrokerID = math.MaxInt64
	}
	if dts.AssignedTs < 0 {
		dts.AssignedTs = math.MaxInt64
	}

	duckResults, err := w.s.ScanPrefix(prefix, dts)
	if err != nil {
		return nil, err
	}
	results := make([]duckReadBatchResult, len(duckResults))
	for i, dr := range duckResults {
		results[i] = duckReadBatchResult{
			Key:   dr.Key,
			Value: dr.Value,
			Found: dr.Found,
			Version: types.CustomTs{
				EpochID:    uint32(dr.Timestamp.EpochID),
				BrokerID:   uint32(dr.Timestamp.BrokerID),
				AssignedTs: uint32(dr.Timestamp.AssignedTs),
			},
		}
	}
	return results, nil
}

func (w *duckDBStorageWrapper) CompactPartitions() error {
	return w.s.CompactPartitions()
}

func (w *duckDBStorageWrapper) ReadBatch(requests []duckReadBatchReq) ([]duckReadBatchResult, error) {
	duckReqs := make([]duckdb.ReadBatchRequest, len(requests))
	for i, req := range requests {
		dts := toDuckTs(req.ReadTs)
		if dts.EpochID < 0 {
			dts.EpochID = math.MaxInt64
		}
		if dts.BrokerID < 0 {
			dts.BrokerID = math.MaxInt64
		}
		if dts.AssignedTs < 0 {
			dts.AssignedTs = math.MaxInt64
		}
		duckReqs[i] = duckdb.ReadBatchRequest{Key: req.Key, ReadTs: dts}
	}

	duckResults, err := w.s.ReadBatch(duckReqs)
	if err != nil {
		return nil, err
	}

	results := make([]duckReadBatchResult, len(duckResults))
	for i, dr := range duckResults {
		results[i] = duckReadBatchResult{
			Key:   dr.Key,
			Value: dr.Value,
			Found: dr.Found,
			Version: types.CustomTs{
				EpochID:    uint32(dr.Timestamp.EpochID),
				BrokerID:   uint32(dr.Timestamp.BrokerID),
				AssignedTs: uint32(dr.Timestamp.AssignedTs),
			},
		}
	}
	return results, nil
}

func (w *duckDBStorageWrapper) Close() error {
	close(w.closeCh)
	w.wg.Wait()
	return w.s.Close()
}

// handleMemTableFlushPartitioned flushes a memtable to DuckDB (full implementation).
func (db *DB) handleMemTableFlushPartitioned(mt *memTable, dropPrefixes [][]byte) error {
	if db.duckDBStorage == nil {
		return db.handleMemTableFlushClassic(mt, dropPrefixes)
	}

	itr := mt.sl.NewUniIterator(false)
	defer itr.Close()

	var entries []duckEntry
	for itr.Rewind(); itr.Valid(); itr.Next() {
		rawKey := itr.Key()
		vs := itr.Value()
		logicalKey := append([]byte(nil), y.ParseKey(rawKey)...)
		version := y.ParseTs(rawKey)
		deleted := vs.Meta&bitDelete != 0
		var val []byte
		if !deleted {
			val = append([]byte(nil), vs.Value...)
		}
		entries = append(entries, duckEntry{Key: logicalKey, Value: val, Version: version, Deleted: deleted})
	}

	if len(entries) == 0 {
		return nil
	}

	if err := db.duckDBStorage.FlushEntries(entries); err != nil {
		return fmt.Errorf("DuckDB flush failed: %w", err)
	}

	return nil
}
