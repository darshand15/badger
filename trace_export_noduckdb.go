//go:build !duckdb

package badger

import "github.com/dgraph-io/badger/v4/types"

// ScanPrefixAt exports visible rows through the normal Badger iterator when
// the DuckDB backend is not compiled in.
func (db *DB) ScanPrefixAt(prefix []byte, readTs types.CustomTs) ([]VisibleEntry, error) {
	txn := db.NewTransactionAt(readTs, false)
	defer txn.Discard()
	it := txn.NewIterator(DefaultIteratorOptions)
	defer it.Close()
	result := make([]VisibleEntry, 0)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		result = append(result, VisibleEntry{
			Key:     append([]byte(nil), item.Key()...),
			Value:   value,
			Version: item.Version(),
		})
	}
	return result, nil
}
