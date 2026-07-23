//go:build duckdb

/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

// Command duckdbcrash is a standalone helper process used by the DuckDB
// crash-recovery test suite (see ../../db_duckdb_crash_test.go). It opens a
// DuckDB-backed managed Badger DB at -dir and writes sequentially-keyed,
// checksum-verifiable entries in a loop, printing a "WROTE <n>" progress line
// to stdout after every committed-and-flushed batch.
//
// This process is deliberately NOT closed cleanly when used for the
// mid-write-kill and torn-write crash tiers -- the parent test process reads
// its stdout to learn how far it got, then sends it SIGKILL and reopens the
// same directory in-process to check what survived. For the clean-restart
// tier, the parent instead lets it run to completion (-keys is small) and
// waits for a normal exit.
//
// Every value is `valueForKey(i)`: an 8-byte big-endian key echo followed by
// a deterministic fill pattern. On reopen, the parent test recomputes
// valueForKey(i) for every key it reads back and compares -- any mismatch
// means a torn or corrupted write survived un-flagged, which is exactly what
// this harness exists to catch.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/types"
)

var (
	dir       = flag.String("dir", "", "badger directory to open (required)")
	numKeys   = flag.Int("keys", 1000000, "max number of keys to write before exiting cleanly")
	batchSize = flag.Int("batch", 50, "keys per transaction before a FlushToStorage + progress print")
	fanOut    = flag.Int("fanout", 4, "PartitionFanOut for the DuckDB backend")
	valueSize = flag.Int("valuesize", 64, "size in bytes of each value (must be >= 8)")
)

// ValueForKey derives a deterministic, verifiable value for key i so a
// reader can detect torn/corrupted writes by recomputing and comparing
// rather than trusting whatever bytes happen to be on disk. Exported (capital
// V) so the parent test package can import and call it directly for
// verification without duplicating the encoding.
func ValueForKey(i uint64, size int) []byte {
	if size < 8 {
		size = 8
	}
	val := make([]byte, size)
	binary.BigEndian.PutUint64(val[0:8], i)
	for j := 8; j < len(val); j++ {
		val[j] = byte(i%251) + byte(j)
	}
	return val
}

// KeyForIndex returns an ASCII key to avoid embedding NUL bytes in the key
// during crash tests. Some integrations are mostly exercised with text keys,
// and using the same shape here makes recovery assertions less brittle.
func KeyForIndex(i uint64) []byte {
	return []byte(fmt.Sprintf("k%020d", i))
}

func main() {
	flag.Parse()
	if *dir == "" {
		log.Fatal("duckdbcrash: -dir is required")
	}

	opts := badger.DefaultOptions(*dir)
	opts.UseDuckDB = true
	opts.PartitionFanOut = *fanOut
	opts.CompactL0OnClose = false
	opts.SyncWrites = true

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatalf("duckdbcrash: open failed: %v", err)
	}
	// Deliberately no defer db.Close() for the kill tiers -- the whole point
	// of this process is to be SIGKILLed before a clean shutdown happens. The
	// clean-restart tier relies on the explicit db.Close() call at the
	// bottom of main() instead, since that path is expected to run to
	// completion.

	var i uint64
	for int(i) < *numKeys {
		end := i + uint64(*batchSize)
		if err := db.Update(func(txn *badger.Txn) error {
			for ; i < end && int(i) < *numKeys; i++ {
				key := KeyForIndex(i)
				if err := txn.Set(key, ValueForKey(i, *valueSize)); err != nil {
					return fmt.Errorf("set failed at key %d: %w", i, err)
				}
			}
			return nil
		}); err != nil {
			log.Fatalf("duckdbcrash: update failed at key %d: %v", i, err)
		}
		if err := db.FlushToStorage(); err != nil {
			log.Fatalf("duckdbcrash: flush failed at key %d: %v", i, err)
		}
		// Progress lines go to stdout (not log.Printf, which defaults to
		// stderr) because the parent test process scans this process's
		// stdout pipe to learn how many keys were confirmed
		// committed+flushed before it sends SIGKILL.
		fmt.Printf("WROTE %d\n", i)
		_ = os.Stdout.Sync()
	}

	checkRead := func(stage string, db *badger.DB, idx uint64) {
		key := KeyForIndex(idx)
		err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			expected := ValueForKey(idx, *valueSize)
			if len(val) != len(expected) {
				return fmt.Errorf("%s key=%d length mismatch got=%d want=%d", stage, idx, len(val), len(expected))
			}
			for j := range expected {
				if val[j] != expected[j] {
					return fmt.Errorf("%s key=%d byte mismatch at %d", stage, idx, j)
				}
			}
			return nil
		})
		if err != nil {
			log.Fatalf("duckdbcrash: %s read check failed for key %d: %v", stage, idx, err)
		}
	}

	checkReadAtMaxTs := func(stage string, db *badger.DB, idx uint64) {
		key := KeyForIndex(idx)
		txn := db.NewTransactionAt(types.MaxTs, false)
		defer txn.Discard()
		item, err := txn.Get(key)
		if err != nil {
			log.Fatalf("duckdbcrash: %s read check failed for key %d: %v", stage, idx, err)
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			log.Fatalf("duckdbcrash: %s value read failed for key %d: %v", stage, idx, err)
		}
		expected := ValueForKey(idx, *valueSize)
		if len(val) != len(expected) {
			log.Fatalf("duckdbcrash: %s key=%d length mismatch got=%d want=%d", stage, idx, len(val), len(expected))
		}
		for j := range expected {
			if val[j] != expected[j] {
				log.Fatalf("duckdbcrash: %s key=%d byte mismatch at %d", stage, idx, j)
			}
		}
	}

	if *numKeys > 0 {
		checkRead("pre-close", db, 0)
		checkRead("pre-close", db, uint64(*numKeys-1))
		fmt.Println("SELFCHK pre-close OK")
		_ = os.Stdout.Sync()
	}

	// Only reached if -keys is small enough to finish before being killed --
	// used by the clean-restart tier.
	if err := db.Close(); err != nil {
		log.Fatalf("duckdbcrash: close failed: %v", err)
	}

	db2, err := badger.OpenManaged(opts)
	if err != nil {
		log.Fatalf("duckdbcrash: reopen failed after close: %v", err)
	}
	if *numKeys > 0 {
		checkReadAtMaxTs("post-reopen", db2, 0)
		checkReadAtMaxTs("post-reopen", db2, uint64(*numKeys-1))
		fmt.Println("SELFCHK post-reopen OK")
		_ = os.Stdout.Sync()
	}
	if err := db2.Close(); err != nil {
		log.Fatalf("duckdbcrash: close after reopen failed: %v", err)
	}
	fmt.Println("DONE")
}
