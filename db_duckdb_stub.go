//go:build !duckdb

/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

// newDuckDBBackend is a no-op stub when not built with -tags duckdb.
// Returns (nil, nil) so Open() silently skips DuckDB initialisation.
func newDuckDBBackend(_ string, _ int) (duckDBIface, error) {
	return nil, nil
}

// handleMemTableFlushPartitioned falls back to the classic SSTable path when
// DuckDB support is not compiled in.
func (db *DB) handleMemTableFlushPartitioned(mt *memTable, dropPrefixes [][]byte) error {
	return db.handleMemTableFlushClassic(mt, dropPrefixes)
}
