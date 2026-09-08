package badger

import "github.com/dgraph-io/badger/v4/types"

// VisibleEntry is a backend-independent key/value row for diagnostics and
// correctness tooling. It intentionally exposes only the visible version.
type VisibleEntry struct {
	Key     []byte
	Value   []byte
	Version types.CustomTs
}
