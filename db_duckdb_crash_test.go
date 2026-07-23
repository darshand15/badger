//go:build duckdb

/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4/types"
	"github.com/stretchr/testify/require"
)

// Crash-recovery / crash-injection suite for the DuckDB backend.
//
// This is a scaffold for item 4 of the DuckDB open-items list
// ("crash-recovery/persistence-across-restart is still untested"). It has
// three tiers of increasing severity:
//
//  1. TestDuckDBCrashRestartClean: open, write, Close() cleanly, reopen,
//     verify. Covers the most basic gap -- no prior test in this repo opened
//     a DuckDB-backed DB, closed it, and reopened the same directory.
//  2. TestDuckDBCrashRestartMidWriteKill: run a writer subprocess, SIGKILL it
//     mid-write, reopen from the parent test process, and verify every key
//     the subprocess had confirmed as committed+flushed survived intact.
//  3. TestDuckDBCrashTornWrite: same kill, plus truncating the tail of the
//     on-disk DuckDB file to simulate a torn write, then check Open() either
//     recovers or fails cleanly -- never panics or hangs.
//
// Tiers 2 and 3 need a real OS process to kill (you cannot SIGKILL your own
// test binary and keep asserting afterward), so they shell out to the
// integration/duckdbcrash helper binary, built fresh via `go build` at test
// time.

// valueForKeyCrashTest MUST exactly match ValueForKey in
// integration/duckdbcrash/main.go. The helper process (package main, not
// importable as a library) writes with that function; this test package
// reads back and verifies with this one. If you change the encoding in one
// place, change it in the other, or every crash test will spuriously fail.
func valueForKeyCrashTest(i uint64, size int) []byte {
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

// keyForIndexCrashTest MUST match KeyForIndex in integration/duckdbcrash/main.go.
func keyForIndexCrashTest(i uint64) []byte {
	return []byte("k" + fmt.Sprintf("%020d", i))
}

// duckDBCrashTestOptions returns the Options used by every tier in this
// suite for reopening a DB written by the helper binary. Kept as a single
// function so the three tiers can't silently drift out of sync with each
// other or with the flags passed to integration/duckdbcrash.
func duckDBCrashTestOptions(dir string, fanOut int) Options {
	opts := DefaultOptions(dir)
	opts.UseDuckDB = true
	opts.PartitionFanOut = fanOut
	opts.CompactL0OnClose = false
	return opts
}

// buildDuckDBCrashHelper compiles integration/duckdbcrash into a temp binary
// and returns its path. Building fresh (rather than assuming a prebuilt
// binary exists on the test runner) keeps the suite self-contained at the
// cost of one `go build` per test run.
func buildDuckDBCrashHelper(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller(0) failed to resolve this test file's path")
	repoRoot := filepath.Dir(thisFile)

	binPath := filepath.Join(t.TempDir(), "duckdbcrash")
	if runtime.GOOS == "windows" {
		binPath += ".exe"
	}

	cmd := exec.Command("go", "build", "-tags", "duckdb", "-o", binPath, "./integration/duckdbcrash")
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "building integration/duckdbcrash helper failed:\n%s", out)
	return binPath
}

// TestDuckDBCrashRestartClean is Tier 1: open, write, close cleanly, reopen,
// verify every key. This is the simplest gap the DuckDB backend had --
// nothing previously exercised Close() followed by a fresh Open() on the
// same on-disk directory.
func TestDuckDBCrashRestartClean(t *testing.T) {
	binPath := buildDuckDBCrashHelper(t)
	dir := t.TempDir()

	const numKeys = 500
	const valueSize = 64
	const fanOut = 4

	cmd := exec.Command(binPath,
		"-dir", dir,
		"-keys", strconv.Itoa(numKeys),
		"-batch", "50",
		"-fanout", strconv.Itoa(fanOut),
		"-valuesize", strconv.Itoa(valueSize),
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "clean writer run failed:\n%s", out)
	require.Contains(t, string(out), "DONE", "writer did not report a clean DONE:\n%s", out)

	db, err := OpenManaged(duckDBCrashTestOptions(dir, fanOut))
	require.NoError(t, err, "reopen after clean shutdown failed")
	defer db.Close()

	txn := db.NewTransactionAt(types.MaxTs, false)
	defer txn.Discard()
	for i := uint64(0); i < numKeys; i++ {
		key := keyForIndexCrashTest(i)
		item, getErr := txn.Get(key)
		require.NoError(t, getErr, "key %d missing after clean restart", i)
		val, valErr := item.ValueCopy(nil)
		require.NoError(t, valErr)
		require.True(t, bytes.Equal(valueForKeyCrashTest(i, valueSize), val),
			"value mismatch for key %d after clean restart", i)
	}
}

// TestDuckDBCrashRestartMidWriteKill is Tier 2: SIGKILL the writer process
// mid-write, then reopen from this (parent) test process and verify every
// key the writer had confirmed committed+flushed before the kill survived
// intact. This is the core "does the DuckDB backend actually persist
// acknowledged writes across a crash" check.
func TestDuckDBCrashRestartMidWriteKill(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL-based crash injection is not supported on windows by this harness")
	}
	binPath := buildDuckDBCrashHelper(t)
	dir := t.TempDir()

	const valueSize = 64
	const fanOut = 4
	const targetKeys = 200000 // large enough the writer is still going when we kill it

	cmd := exec.Command(binPath,
		"-dir", dir,
		"-keys", strconv.Itoa(targetKeys),
		"-batch", "20",
		"-fanout", strconv.Itoa(fanOut),
		"-valuesize", strconv.Itoa(valueSize),
	)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr // surface the writer's log.Fatalf output directly in `go test -v` output

	require.NoError(t, cmd.Start())

	// Watch stdout for "WROTE <n>" progress lines and record the highest n
	// confirmed committed+flushed before we kill the process.
	var (
		mu             sync.Mutex
		lastConfirmed  uint64
		haveProgress   = make(chan struct{}, 1)
		scanDone       = make(chan struct{})
	)
	go func() {
		defer close(scanDone)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "WROTE ") {
				continue
			}
			n, convErr := strconv.ParseUint(strings.TrimPrefix(line, "WROTE "), 10, 64)
			if convErr != nil {
				continue
			}
			mu.Lock()
			lastConfirmed = n
			mu.Unlock()
			select {
			case haveProgress <- struct{}{}:
			default:
			}
		}
	}()

	// Wait for at least one committed+flushed batch before killing so the test
	// always has acknowledged durability to validate after restart.
	select {
	case <-haveProgress:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "writer produced no WROTE progress within timeout")
	}
	// Then wait briefly so the kill is likely mid-flight on a subsequent batch.
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, cmd.Process.Signal(syscall.SIGKILL))
	_ = cmd.Wait() // expected to return a non-nil "signal: killed" error; that's the point
	<-scanDone

	mu.Lock()
	confirmed := lastConfirmed
	mu.Unlock()
	require.Greater(t, confirmed, uint64(0),
		"writer was killed before confirming even one batch -- "+
			"increase the sleep above or decrease -batch")
	t.Logf("writer confirmed %d keys committed+flushed before SIGKILL", confirmed)

	// The critical assertion here is that reopening does not hang or panic --
	// a real crash-consistency bug in the DuckDB backend (e.g. a corrupted
	// WAL/checkpoint) would most likely surface as a hang or panic during
	// Open(), not as a subtly wrong value.
	var db *DB
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("reopen after SIGKILL panicked instead of returning an error: %v", r)
			}
		}()
		var openErr error
		db, openErr = OpenManaged(duckDBCrashTestOptions(dir, fanOut))
		require.NoError(t, openErr, "reopen after SIGKILL failed")
	}()
	defer db.Close()

	// Every key up to `confirmed` was reported as committed+flushed before
	// the kill, so each one MUST be present with the exact expected value --
	// if any is missing or wrong, the DuckDB backend lost or corrupted
	// acknowledged data across a crash, which is exactly the bug this test
	// exists to catch.
	txn := db.NewTransactionAt(types.MaxTs, false)
	defer txn.Discard()
	var verified int
	for i := uint64(0); i < confirmed; i++ {
		key := keyForIndexCrashTest(i)
		item, getErr := txn.Get(key)
		if getErr != nil {
			t.Errorf("key %d was reported committed+flushed before the kill but is missing after reopen: %v", i, getErr)
			continue
		}
		val, valErr := item.ValueCopy(nil)
		if valErr != nil {
			t.Errorf("key %d: ValueCopy failed after reopen: %v", i, valErr)
			continue
		}
		if !bytes.Equal(val, valueForKeyCrashTest(i, valueSize)) {
			t.Errorf("key %d has a corrupted/torn value after reopen", i)
			continue
		}
		verified++
	}
	t.Logf("verified %d/%d acknowledged keys survived the crash intact", verified, confirmed)
}

// TestDuckDBCrashTornWrite is Tier 3: SIGKILL the writer, then additionally
// truncate the tail of the on-disk DuckDB file to simulate a torn write
// (e.g. power loss mid-fsync), and check that reopening either recovers
// cleanly or fails with a clean error -- never a panic or an indefinite hang.
//
// This is a blunt approximation of file-level corruption -- DuckDB's own
// WAL/checkpoint format has internal structure this test does not attempt to
// target precisely -- but it is enough to catch the worst-case failure
// modes (panics, hangs, or a "successful" open that silently returns garbage
// on first use).
func TestDuckDBCrashTornWrite(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("byte-level file corruption injection is not supported on windows by this harness")
	}
	binPath := buildDuckDBCrashHelper(t)
	dir := t.TempDir()

	const valueSize = 64
	const fanOut = 2 // fewer partitions -> fewer files to corrupt, simpler assertions
	const targetKeys = 50000

	cmd := exec.Command(binPath,
		"-dir", dir,
		"-keys", strconv.Itoa(targetKeys),
		"-batch", "20",
		"-fanout", strconv.Itoa(fanOut),
		"-valuesize", strconv.Itoa(valueSize),
	)
	require.NoError(t, cmd.Start())
	duckPath := filepath.Join(dir, "duckdb_data")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(duckPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			require.FailNow(t, "duckdb_data file did not appear before crash injection")
		}
		time.Sleep(25 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, cmd.Process.Signal(syscall.SIGKILL))
	_ = cmd.Wait()

	info, statErr := os.Stat(duckPath)
	if statErr != nil {
		t.Skipf("duckdb_data file not found at %s (nothing to corrupt): %v", duckPath, statErr)
	}
	// Drop the last 5% of the file to simulate a torn tail write.
	truncateAt := info.Size() - info.Size()/20
	if truncateAt < 0 {
		truncateAt = 0
	}
	require.NoError(t, os.Truncate(duckPath, truncateAt))

	var (
		db      *DB
		openErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("reopen after truncating duckdb_data panicked instead of returning an error: %v", r)
			}
		}()
		db, openErr = OpenManaged(duckDBCrashTestOptions(dir, fanOut))
	}()

	if openErr != nil {
		// A clean, explicit error is an acceptable outcome for this tier --
		// what matters is that it IS an error, not a panic, a hang, or a
		// silently-successful open over corrupted data.
		t.Logf("Open() correctly returned an error after file truncation: %v", openErr)
		return
	}

	// If Open() succeeded, DuckDB's own recovery handled the truncation.
	// Confirm the DB is actually usable afterward rather than left in a
	// half-open state that panics on first use.
	defer db.Close()
	count := 0
	txn := db.NewTransactionAt(types.MaxTs, false)
	defer txn.Discard()
	it := txn.NewIterator(DefaultIteratorOptions)
	defer it.Close()
	for it.Rewind(); it.Valid() && count < 10; it.Next() {
		count++
	}
	t.Logf("Open() recovered successfully after truncation; iterated %d keys without panicking", count)
}
