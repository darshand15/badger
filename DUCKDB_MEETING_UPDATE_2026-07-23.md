# DuckDB Backend — Meeting Update

Date: 2026-07-23
Branch: duckdb-integration (base commit fdedaf3, hardening commit 8ecbebf)

## What shipped since the last meeting brief

**Read-pool sweep (100k customers, 128 workers):** confirmed DuckDB throughput
rises monotonically with read-pool size — pool 2: 3220.5 ops/s (6.04x vs
Badger), pool 4: 3872.6 (9.51x), pool 8: 3987.0 (7.39x), pool 12: 4076.0
(7.53x). This revises the earlier tuned default of 2 (set to save memory) —
8–12 is a better operating point at high concurrency. Committed as
`fdedaf3` with `readpool_sweep_100k_128.csv`/`_summary.md` and a fresh
`compare_summary.md`.

**Four of the five open architecture items were implemented today** (item 4,
crash-recovery testing, is scoped as separate follow-up — see below):

1. **Protobuf version-packing truncation bug (was: streaming panic).**
   Turned out the panic itself was already fixed (`CustomTs.ToUint64()` /
   `CustomTsFromUint64()` already existed and are used in `stream.go`,
   `backup.go`). What wasn't fixed: `ToUint64()` packs EpochID/BrokerID into
   16 bits each, so either exceeding 65535 silently drops bits — corrupting
   version ordering with no error. Added `CanRoundtripUint64()` and wired a
   guard into the three production call sites (`stream.go`'s `KeyToList`,
   `backup.go`'s `Backup`, both the primary and discard-marker version) so
   this now fails loudly instead of silently. `publisher.go`'s pub/sub path
   also calls `ToUint64()` but wasn't changed — it's a low-stakes
   notification path (worst case a subscriber sees a wrong version number,
   not data loss) and adding an error return there means reworking
   `publishUpdates`'s calling contract, which felt like scope creep for
   today; flagging it as a known gap rather than silently leaving it
   unmentioned. Added `types/custom_ts_test.go` pinning the 65535 boundary.

2. **`BenchmarkDbGrowth` quadratic growth.** Note: the actual
   `BenchmarkDbGrowth` that showed the `4100572646 ns/op` quadratic number
   lives on the `dd_exp_dbgrowth` branch, not on `duckdb-integration` — this
   branch doesn't have a DuckDB-tagged growth benchmark to rerun directly.
   The underlying cause (the `PRIMARY KEY (key, epoch_id, broker_id,
   assigned_ts)` constraint forcing a dedup scan on every appended row) is
   real here too, so it's fixed at the source: dropped the `PRIMARY KEY` in
   `duckdb/storage.go`'s `initializeTables()`, replaced with a plain index on
   `key`. No write path used `ON CONFLICT`/upsert semantics that depended on
   the constraint, and every read path already tolerates duplicate rows
   (`ORDER BY ... LIMIT 1` / `ROW_NUMBER() ... rn = 1`), so this should be a
   safe, pure win — but it needs validation via `go test -tags duckdb -run
   TestDuckDBBank` and the compaction correctness tests before merging (see
   caveat below).

3. **Compaction history loss / fixed partition fan-out.** Two changes:
   - `CompactPartitions()` used to hard-code "keep only the latest version."
     It now respects `NumVersionsToKeep` (threaded through from `Options`,
     same field Badger's own SST compaction uses) — `rn = 1` became
     `rn <= NumVersionsToKeep`, so setting `NumVersionsToKeep > 1` now
     actually retains history through DuckDB compaction instead of always
     collapsing to one version.
   - `PartitionFanOut` is still fixed at DB-open time (making it dynamically
     resizable is a genuinely large feature, out of scope today), but
     reopening an existing on-disk DB with a *different* fan-out than it was
     created with used to silently corrupt reads (keys hash to the wrong
     partition table with no error). Added a small `_badger_duckdb_meta`
     table that records the fan-out on first creation and errors loudly on
     mismatch on every subsequent open.

4. **`TimestampOracle` interface + `LocalOracle`/`RemoteOracle`.** Extracted
   a `TimestampOracle` interface in `divytime/divytime.go` matching the
   existing `Oracle.GetTimestamp`/`GetCommitTimestamp` signatures.
   `LocalOracle` is a type alias for the existing `Oracle` (not a rename —
   `Oracle` is used directly in 6 test files, so aliasing avoids touching all
   of them), plus a `RemoteOracle` stub that satisfies the interface but
   panics on every call until a real Divy broker connection is implemented.
   One honest caveat: `divytime.Oracle` isn't actually held anywhere in
   `Options`/`DB` today — it's only ever constructed directly inside test
   files — so there was no live integration point to wire a
   `TimestampOracle` field into. The interface exists and compiles against
   both implementations; swapping in a real `RemoteOracle` later is a
   drop-in replacement wherever tests currently construct `divytime.Oracle`
   directly.

## Validation status (compiled and tested)

The changes above have now been compiled and tested in this environment.

Executed commands and outcomes:

- `go build -tags duckdb ./...` -> PASS
- `go test -tags duckdb -run 'TestDuckDB|TestOracleFastPath|TestBankBadgerVsDuckDB' -timeout 600s ./...` -> PASS
- `go test ./types/... -run TestToUint64 -v` -> PASS

`go vet -tags duckdb ./...` reports failures, but all reported findings are
pre-existing and unrelated to this change set (notably lock-copy warnings in
`trie`/`publisher` paths and one non-test goroutine `Fatalf` warning in
`db_test.go`). No new vet issue was introduced by these DuckDB/timestamp edits.

## Follow-up: crash-recovery / crash-injection suite scaffold (item 4)

Built out as a follow-up on top of `8ecbebf`, not yet committed/pushed —
see the caveat below. Three tiers, all shelling out to a new standalone
helper process (`integration/duckdbcrash`, build-tagged `duckdb`) since you
cannot `SIGKILL` your own test binary and keep asserting afterward:

1. **`TestDuckDBCrashRestartClean`** — the writer helper opens a DuckDB-backed
   managed DB, writes 500 checksum-verifiable keys, and closes cleanly; the
   test process then reopens the same on-disk directory and verifies every
   key and value byte-for-byte. This is the most basic gap that existed
   before today: no prior test in this repo opened a DuckDB-backed DB,
   closed it, and reopened the same directory.
2. **`TestDuckDBCrashRestartMidWriteKill`** — the writer helper is launched
   targeting 200,000 keys, its stdout is scanned for `WROTE <n>` progress
   lines (committed + `FlushToStorage()`'d), then after 300ms it is sent
   `SIGKILL`. The test process reopens the same directory and asserts (a)
   `Open()` doesn't panic or hang — wrapped in `recover()` to turn a panic
   into a clear test failure rather than a crashed test binary — and (b)
   every key up to the last confirmed `WROTE` line is present with the exact
   expected value. This is the core "does the backend actually persist
   acknowledged writes across a crash" check.
3. **`TestDuckDBCrashTornWrite`** — same kill, plus truncating the last 5% of
   the on-disk `duckdb_data` file afterward to simulate a torn tail write
   (e.g. power loss mid-fsync). Reopening must either recover cleanly or fail
   with an explicit error — never panic or hang. This is a blunt
   approximation (it doesn't target DuckDB's WAL/checkpoint structure
   precisely), enough to catch the worst failure modes, not a substitute for
   a real fault-injection framework.

Every value the helper writes is `key || deterministic-fill-pattern` so a
torn/corrupted value is detected by recomputing and comparing rather than
trusting whatever bytes are on disk; the encoding lives in
`integration/duckdbcrash/main.go`'s `ValueForKey` and is duplicated (by
necessity — the helper is `package main`, not importable) in
`db_duckdb_crash_test.go`'s `valueForKeyCrashTest`. Both are commented
pointing at each other so they don't silently drift apart.

**Execution status (run in this environment):**

```bash
go build -tags duckdb ./...                               # PASS
go test -tags duckdb -run '^TestDuckDBCrashRestartClean$' -v -timeout 180s .   # PASS
go test -tags duckdb -run '^TestDuckDBCrashRestartMidWriteKill$' -v -timeout 240s . # PASS
go test -tags duckdb -run '^TestDuckDBCrashTornWrite$' -v -timeout 180s .       # PASS
go test -tags duckdb -run 'TestDuckDBCrash' -v -timeout 300s .                   # PASS
```

Root causes found and fixed during validation:

- **Partition routing used process-seeded hash (`z.MemHash`)** in `partitionCalculator.getPartition`, which is unstable across processes and broke helper-process writes vs test-process reads after restart. Fixed by switching to deterministic FNV-1a hashing.
- **`FlushToStorage()` was a no-op for DuckDB**, so `WROTE` lines in the crash helper did not imply durable flush. Fixed by extending `duckDBIface` with `FlushAllPending()` and wiring `DB.FlushToStorage()` to flush all partition Appender buffers.
- **DuckDB write worker shutdown could exit before draining queued writes**. Fixed by closing `writeCh` and ranging until drained in `duckDBWriteWorker`, then closing storage.
- **Helper post-reopen self-check used non-managed reads** (`db.View`) and could observe low bootstrap readTs. Fixed helper post-reopen checks to use managed snapshots at `types.MaxTs`.

Result: all three crash tiers now pass locally, and the helper self-check passes both pre-close and post-reopen.

## Files touched today

**In commit `8ecbebf`:**
- `types/custom_ts.go`, `types/custom_ts_test.go` (new)
- `stream.go`, `backup.go`
- `duckdb/storage.go` (`initializeTables`, `NewDuckDBStorage`/
  `NewDuckDBStorageWithOptions`, `verifyOrRecordFanOut` (new),
  `CompactPartitions`)
- `db_duckdb_impl.go`, `db_duckdb_stub.go`, `db.go` (threading
  `NumVersionsToKeep` through to the DuckDB backend)
- `divytime/divytime.go` (`TimestampOracle`, `LocalOracle`, `RemoteOracle`)

**Crash-suite follow-up files (validated):**
- `integration/duckdbcrash/main.go` (new)
- `db_duckdb_crash_test.go` (new)
- `db.go` (`duckDBIface` + `FlushToStorage` DuckDB wiring)
- `db_duckdb_impl.go` (write-worker drain on close + `FlushAllPending` forwarding)
- `duckdb/storage.go` (deterministic partition hash + close/flush hardening)
