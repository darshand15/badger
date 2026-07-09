/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import "github.com/dgraph-io/badger/v4/types"

// OpenManaged returns a new DB, which allows more control over setting
// transaction timestamps, aka managed mode.
//
// This is only useful for databases built on top of Badger (like Dgraph), and
// can be ignored by most users.
func OpenManaged(opts Options) (*DB, error) {
	opts.managedTxns = true
	return Open(opts)
}

// NewTransactionAt follows the same logic as DB.NewTransaction(), but uses the
// provided read timestamp.
//
// This is only useful for databases built on top of Badger (like Dgraph), and
// can be ignored by most users.
func (db *DB) NewTransactionAt(readTs types.CustomTs, update bool) *Txn {
	if !db.opt.managedTxns {
		panic("Cannot use NewTransactionAt with managedDB=false. Use NewTransaction instead.")
	}
	txn := db.newTransaction(update, true)
	txn.readTs = readTs
	// DuckDB read barrier: wait for all in-flight commits with ts ≤ readTs to
	// complete DirectFlush across all partitions before this transaction reads.
	//
	// Without this, a multi-key read (e.g. SUM_CHECK over all accounts) can see
	// a partial view of a committed transaction: it reads partition P1 before
	// commit C writes to P1, then reads partition P2 after C writes to P2,
	// producing an inconsistent snapshot (money appears created or destroyed).
	//
	// duckDBTracker.begin/done is called around each DirectFlush in txn.go.
	// waitUntil spins (via cond.Wait) until no pending commit has ts ≤ readTs.
	if db.opt.UseDuckDB && db.duckDBStorage != nil {
		db.orc.duckDBTracker.waitUntil(readTs)
	}
	return txn
}

// RegisterPendingCommit registers a commit timestamp as in-flight with the
// DuckDB commit tracker BEFORE CommitAt is called. It must be invoked
// atomically with timestamp issuance (e.g. from the register callback of
// divytime.Oracle.GetCommitTimestamp, which runs while the oracle's issue lock
// is held).
//
// Why: NewTransactionAt's read barrier (duckDBTracker.waitUntil) can only wait
// for commits it knows about. Without pre-registration there is a window
// between a commitTs being issued and CommitAt registering it inside
// newCommitTs — a window that widens with writeChLock contention — during
// which a reader with a higher readTs starts, reads stale data, and later
// passes conflict detection because hasConflict skips committed txns with
// ts <= readTs. That is a lost update (observed as bank-invariant violations
// at high worker counts).
//
// Contract: after calling this, the caller MUST call CommitAt with the same
// ts. Every abort path inside CommitAt (conflict, precheck failure, empty
// write set, flush error) deregisters the ts; an abandoned registration
// blocks all future readers at readTs >= ts.
func (db *DB) RegisterPendingCommit(ts types.CustomTs) {
	if !db.opt.managedTxns {
		panic("Cannot use RegisterPendingCommit with managedDB=false.")
	}
	db.orc.duckDBTracker.begin(ts)
}

// NewWriteBatchAt is similar to NewWriteBatch but it allows user to set the commit timestamp.
// NewWriteBatchAt is supposed to be used only in the managed mode.
func (db *DB) NewWriteBatchAt(commitTs types.CustomTs) *WriteBatch {
	if !db.opt.managedTxns {
		panic("cannot use NewWriteBatchAt with managedDB=false. Use NewWriteBatch instead")
	}

	wb := db.newWriteBatch(true)
	wb.commitTs = commitTs
	wb.txn.commitTs = commitTs
	return wb
}
func (db *DB) NewManagedWriteBatch() *WriteBatch {
	if !db.opt.managedTxns {
		panic("cannot use NewManagedWriteBatch with managedDB=false. Use NewWriteBatch instead")
	}

	wb := db.newWriteBatch(true)
	return wb
}

// CommitAt commits the transaction, following the same logic as Commit(), but
// at the given commit timestamp. This will panic if not used with managed transactions.
//
// This is only useful for databases built on top of Badger (like Dgraph), and
// can be ignored by most users.
func (txn *Txn) CommitAt(commitTs types.CustomTs, callback func(error)) error {
	if !txn.db.opt.managedTxns {
		panic("Cannot use CommitAt with managedDB=false. Use Commit instead.")
	}
	txn.commitTs = commitTs
	if callback == nil {
		return txn.Commit()
	}
	txn.CommitWith(callback)
	return nil
}

// SetDiscardTs sets a timestamp at or below which, any invalid or deleted
// versions can be discarded from the LSM tree, and thence from the value log to
// reclaim disk space. Can only be used with managed transactions.
func (db *DB) SetDiscardTs(ts types.CustomTs) {
	if !db.opt.managedTxns {
		panic("Cannot use SetDiscardTs with managedDB=false.")
	}
	db.orc.setDiscardTs(ts)
}
