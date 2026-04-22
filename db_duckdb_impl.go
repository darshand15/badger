//go:build duckdb

/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4/duckdb"
	"github.com/dgraph-io/badger/v4/types"
	"github.com/dgraph-io/badger/v4/y"
)

// divyTsCounter is an atomic counter that provides unique AssignedTs values
// for the hot DirectFlush path without calling out to a remote oracle.
var divyTsCounter int64

// toDuckTs converts a types.CustomTs (uint32 fields) to a duckdb.CustomTs
// (int64 fields) suitable for DuckDB appender and SQL queries.
func toDuckTs(ts types.CustomTs) duckdb.CustomTs {
	return duckdb.CustomTs{
		EpochID:    int64(ts.EpochID),
		BrokerID:   int64(ts.BrokerID),
		AssignedTs: int64(ts.AssignedTs),
	}
}

// makeDivyTs converts a types.CustomTs Badger version to a duckdb.CustomTs,
// assigning a unique monotonic AssignedTs counter so concurrent writes within
// the same EpochID+BrokerID are still totally ordered.
func makeDivyTs(version types.CustomTs) duckdb.CustomTs {
	return duckdb.CustomTs{
		EpochID:    int64(version.EpochID),
		BrokerID:   int64(version.BrokerID),
		AssignedTs: atomic.AddInt64(&divyTsCounter, 1),
	}
}

// makeDivyTsFast is the same as makeDivyTs but intended for the DirectFlush
// hot path where every call must be sub-microsecond.
func makeDivyTsFast(version types.CustomTs) duckdb.CustomTs {
	return duckdb.CustomTs{
		EpochID:    int64(version.EpochID),
		BrokerID:   int64(version.BrokerID),
		AssignedTs: atomic.AddInt64(&divyTsCounter, 1),
	}
}

// duckDBStorageWrapper wraps *duckdb.DuckDBStorage to implement duckDBIface.
type duckDBStorageWrapper struct {
	s *duckdb.DuckDBStorage
}

// newDuckDBBackend creates a DuckDB-backed storage implementation.
func newDuckDBBackend(path string, parts int) (duckDBIface, error) {
	s, err := duckdb.NewDuckDBStorage(path, parts)
	if err != nil {
		return nil, err
	}
	return &duckDBStorageWrapper{s: s}, nil
}

func (w *duckDBStorageWrapper) Read(key []byte, readTs types.CustomTs) ([]byte, types.CustomTs, error) {
	dts := toDuckTs(readTs)
	// For MaxTs reads, cap to MaxInt64 so the SQL WHERE clause matches all rows.
	if dts.EpochID < 0 {
		dts.EpochID = math.MaxInt64
	}
	if dts.BrokerID < 0 {
		dts.BrokerID = math.MaxInt64
	}
	if dts.AssignedTs < 0 {
		dts.AssignedTs = math.MaxInt64
	}
	// Always read up to max broker/assigned so we see all rows for this epoch.
	dts.BrokerID = math.MaxInt64
	dts.AssignedTs = math.MaxInt64

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
	darshanEntries := make([]*duckdb.DarshanEntry, len(entries))
	for i, e := range entries {
		darshanEntries[i] = &duckdb.DarshanEntry{
			Key:       e.Key,
			Value:     e.Value,
			Version:   uint64(e.Version.EpochID),
			Deleted:   e.Deleted,
			Timestamp: makeDivyTs(e.Version),
		}
	}
	return w.s.FlushDarshanEntries(darshanEntries)
}

func (w *duckDBStorageWrapper) DirectFlush(entries []duckEntry) error {
	darshanEntries := make([]*duckdb.DarshanEntry, 0, len(entries))
	for _, e := range entries {
		darshanEntries = append(darshanEntries, &duckdb.DarshanEntry{
			Key:       e.Key,
			Value:     e.Value,
			Deleted:   e.Deleted,
			Timestamp: makeDivyTsFast(e.Version),
			Version:   uint64(e.Version.EpochID),
		})
	}
	return w.s.DirectAppendEntries(darshanEntries)
}

func (w *duckDBStorageWrapper) CompactPartitions() error {
	return w.s.CompactPartitions()
}

func (w *duckDBStorageWrapper) Close() error {
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


