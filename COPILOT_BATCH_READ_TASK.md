# Task: Implement Batched Read Optimization for DuckDB Backend

## Background

This repo is `darshan-badger`, a fork of Badger v4 that replaces the LSM storage
engine with DuckDB as an alternative backend. The DuckDB path is compiled with
`-tags duckdb` and guarded by `UseDuckDB: true` in options.

We ran a SmallBank microbenchmark (`db_duckdb_smallbank_bench_test.go`) that
revealed the following:

```
=== Phase Breakdown (10,000 runs, single-threaded) ===
Transaction        Read avg    Write avg   Commit avg   Total avg
Balance            1.35ms      0µs         0µs          1.35ms
DepositChecking    1.71ms      1µs         64µs         1.78ms
SendPayment        3.63ms      2µs         77µs         3.71ms
```

**Reads consume 95%+ of all latency. Writes take 1–2µs. Commits take 64–79µs.**

Each `txn.Get()` call fires one SQL query through CGo into DuckDB:
- Balance (3 reads): 1.35ms / 3 = ~0.45ms per query
- SendPayment (4 reads): 3.63ms / 4 = ~0.9ms per query

The fix is to add a **batched read** method that reads all keys a transaction
needs in a single SQL query using `WHERE key IN (...)` instead of N separate
queries. This reduces CGo round-trips from N to 1 per transaction.

---

## Files to Modify

| File | What to change |
|---|---|
| `duckdb/storage.go` | Add `ReadBatch()` method to `DuckDBStorage` |
| `db.go` | Add `ReadBatch()` to `duckDBIface` interface |
| `db_duckdb_impl.go` | Add `ReadBatch()` to `duckDBStorageWrapper` |
| `db_duckdb_stub.go` | Add no-op `ReadBatch()` stub for non-duckdb builds |
| `txn.go` | Use `ReadBatch()` when a transaction reads more than 1 key |

---

## Step 1 — Add `ReadBatch` to `duckdb/storage.go`

File: `duckdb/storage.go`

Add this method to `DuckDBStorage`, after the existing `Read()` method (around line 427).

`ReadBatch` reads the latest value for multiple keys in a single SQL query per
partition. Keys are grouped by partition, then one `SELECT ... WHERE key IN (...)`
is issued per partition instead of one query per key.

```go
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
// as possible. Keys are grouped by partition; one query is issued per partition
// that contains at least one requested key. This eliminates the per-key CGo
// round-trip cost that makes individual Read() calls expensive under OLTP workloads.
//
// The returned slice is in the same order as the input requests.
// If a key is not found its ReadBatchResult has Found=false and Value=nil.
//
// All requests must share the same ReadTs (this is the normal case for a
// single badger transaction). If keys have different ReadTs values, fall
// back to calling Read() individually for each key.
func (s *DuckDBStorage) ReadBatch(requests []ReadBatchRequest) ([]ReadBatchResult, error) {
    if len(requests) == 0 {
        return nil, nil
    }

    results := make([]ReadBatchResult, len(requests))
    for i, req := range requests {
        results[i].Key = req.Key
    }

    // Index from key string → slice positions (a key may appear more than once
    // in a transaction, though that is rare for SmallBank).
    keyToIndices := make(map[string][]int, len(requests))
    for i, req := range requests {
        k := string(req.Key)
        keyToIndices[k] = append(keyToIndices[k], i)
    }

    // Group requests by partition.
    type partReq struct {
        key    []byte
        readTs CustomTs
        idx    int // original position in requests
    }
    partGroups := make(map[int][]partReq)
    for i, req := range requests {
        pid := s.partCalc.getPartition(req.Key)
        partGroups[pid] = append(partGroups[pid], partReq{req.Key, req.ReadTs, i})
    }

    // For each partition: flush if any requested key is pending, then query.
    for pid, reqs := range partGroups {
        pa := s.partAppenders[pid]

        // Check under RLock whether any key in this partition needs a flush.
        pa.mu.RLock()
        needFlush := false
        for _, r := range reqs {
            if pa.hasPending(r.key) {
                needFlush = true
                break
            }
        }
        pa.mu.RUnlock()

        if needFlush {
            pa.mu.Lock()
            if err := pa.flush(); err != nil {
                pa.mu.Unlock()
                return nil, fmt.Errorf("ReadBatch flush partition %d: %w", pid, err)
            }
            pa.mu.Unlock()
        }

        // Use the ReadTs from the first request in the group. In a single
        // badger transaction all reads share the same readTs.
        readTs := reqs[0].readTs

        // Build the IN list of keys.
        tableName := fmt.Sprintf("partition_%d", pid)
        placeholders := make([]string, len(reqs))
        args := make([]interface{}, 0, len(reqs)+7)
        for i, r := range reqs {
            placeholders[i] = "?"
            args = append(args, r.key)
        }
        inClause := strings.Join(placeholders, ", ")

        // Append the 7 timestamp args used in the WHERE clause.
        args = append(args,
            readTs.EpochID,
            readTs.EpochID, readTs.BrokerID,
            readTs.EpochID, readTs.BrokerID, readTs.AssignedTs,
        )

        // One query returns the latest visible row for EACH key in the IN list.
        // The QUALIFY / ROW_NUMBER trick picks the most-recent row per key in
        // a single pass — DuckDB supports this natively.
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

        rows, err := s.db.QueryContext(s.ctx, querySQL, args...)
        if err != nil {
            return nil, fmt.Errorf("ReadBatch query partition %d: %w", pid, err)
        }

        for rows.Next() {
            var (
                key                    []byte
                epochID, brokerID, ts  int64
                value                  []byte
                deleted                bool
            )
            if err := rows.Scan(&key, &epochID, &brokerID, &ts, &value, &deleted); err != nil {
                _ = rows.Close()
                return nil, fmt.Errorf("ReadBatch scan partition %d: %w", pid, err)
            }
            // Fill in all result slots for this key.
            for _, idx := range keyToIndices[string(key)] {
                if deleted {
                    results[idx].Found = false
                    results[idx].Value = nil
                } else {
                    results[idx].Found = true
                    results[idx].Value = value
                    results[idx].Timestamp = CustomTs{
                        EpochID:    epochID,
                        BrokerID:   brokerID,
                        AssignedTs: ts,
                    }
                }
            }
        }
        if err := rows.Close(); err != nil {
            return nil, fmt.Errorf("ReadBatch close rows partition %d: %w", pid, err)
        }
    }

    return results, nil
}
```

Also add `"strings"` to the import block at the top of `duckdb/storage.go`.

---

## Step 2 — Add `ReadBatch` to the `duckDBIface` interface

File: `db.go`, around line 89 where `duckDBIface` is defined.

Add this method to the interface, after `Read()`:

```go
// ReadBatch retrieves the latest value for multiple keys in a single SQL query
// per partition. More efficient than calling Read() N times when a transaction
// needs multiple keys, because it reduces CGo round-trips from N to 1.
// All entries in requests should share the same ReadTs.
ReadBatch(requests []duckReadBatchReq) ([]duckReadBatchResult, error)
```

Also add the two new types near the `duckEntry` struct definition (around line 113):

```go
// duckReadBatchReq is one key lookup within a ReadBatch call.
type duckReadBatchReq struct {
    Key    []byte
    ReadTs types.CustomTs
}

// duckReadBatchResult is the result for one key in a ReadBatch call.
type duckReadBatchResult struct {
    Key     []byte
    Value   []byte
    Version types.CustomTs
    Found   bool
}
```

---

## Step 3 — Implement `ReadBatch` in `db_duckdb_impl.go`

File: `db_duckdb_impl.go` (build tag: `//go:build duckdb`)

Add this method to `duckDBStorageWrapper` after the existing `Read()` method:

```go
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
```

---

## Step 4 — Add stub to `db_duckdb_stub.go`

File: `db_duckdb_stub.go` (build tag: `//go:build !duckdb`)

This file provides no-op implementations when the `duckdb` build tag is absent.
Add this stub so the code compiles without `-tags duckdb`:

```go
func (w *duckDBStorageWrapper) ReadBatch(requests []duckReadBatchReq) ([]duckReadBatchResult, error) {
    return nil, fmt.Errorf("ReadBatch: DuckDB not compiled in (missing -tags duckdb)")
}
```

---

## Step 5 — Use `ReadBatch` in `txn.go`

File: `txn.go`

The `Txn` struct already collects `pendingWrites` (keys being written in this
transaction). For the **read path**, a transaction calls `txn.Get()` one key at a
time. We cannot change the public `Get()` signature, but we can pre-fetch multiple
keys if the caller has already registered them via `txn.prefetchKeys`.

The simplest correct approach: add a `PrefetchKeys` method to `Txn` that accepts
a slice of keys, calls `ReadBatch` once, and stores the results in a map that
`Get()` consults before issuing its own SQL query.

Add to the `Txn` struct (find the struct definition and add a field):
```go
batchCache map[string]*duckReadBatchResult // pre-fetched values from ReadBatch
```

Add this new public method to `txn.go`:

```go
// PrefetchKeys pre-fetches all the given keys from DuckDB in a single batched
// SQL query and caches the results in the transaction. Subsequent calls to
// Get() for these keys will be served from the cache without another SQL
// round-trip.
//
// Call this at the start of a transaction when you know which keys will be read
// (e.g. in SmallBank, before reading savings/checking balances). Order does not
// matter. Duplicate keys are de-duplicated automatically.
//
// This is a no-op when the DuckDB backend is not active.
func (txn *Txn) PrefetchKeys(keys [][]byte) error {
    if txn.db.duckDBStorage == nil || len(keys) == 0 {
        return nil
    }
    if txn.batchCache == nil {
        txn.batchCache = make(map[string]*duckReadBatchResult, len(keys))
    }

    // De-duplicate keys already in cache.
    toFetch := keys[:0]
    for _, k := range keys {
        if _, already := txn.batchCache[string(k)]; !already {
            toFetch = append(toFetch, k)
        }
    }
    if len(toFetch) == 0 {
        return nil
    }

    reqs := make([]duckReadBatchReq, len(toFetch))
    for i, k := range toFetch {
        reqs[i] = duckReadBatchReq{Key: k, ReadTs: txn.readTs}
    }

    results, err := txn.db.duckDBStorage.ReadBatch(reqs)
    if err != nil {
        return fmt.Errorf("PrefetchKeys: %w", err)
    }
    for i := range results {
        r := results[i] // copy so we can take address
        txn.batchCache[string(r.Key)] = &r
    }
    return nil
}
```

Then modify the `Get()` method in `txn.go` — find the DuckDB read block
(around line 472–490) and insert a cache lookup **before** the SQL call:

```go
// DuckDB first if available
if txn.db.duckDBStorage != nil {
    // ── NEW: check the prefetch cache first (zero CGo cost) ──────────────
    if txn.batchCache != nil {
        if cached, ok := txn.batchCache[string(key)]; ok {
            if !cached.Found || cached.Value == nil {
                return nil, ErrKeyNotFound
            }
            return &Item{
                key:       key,
                version:   cached.Version,
                val:       cached.Value,
                meta:      0,
                userMeta:  0,
                txn:       txn,
                expiresAt: 0,
                status:    prefetched,
            }, nil
        }
    }
    // ── END NEW ──────────────────────────────────────────────────────────

    // Cache miss — fall through to single SQL query (existing behaviour).
    if val, ver, err := txn.db.duckDBStorage.Read(key, txn.readTs); err == nil && val != nil {
        return &Item{
            key:       key,
            version:   ver,
            val:       val,
            // ...existing fields...
        }, nil
    }
    // Not found in DuckDB — fall back to LSM lookup
}
```

---

## Step 6 — Call `PrefetchKeys` in `smallbank_transactions.go`

File: `lock-free-machine/services/server/smallbank_transactions.go`

Now wire up the prefetch at the top of each SmallBank function.
Example for `SendPayment` (4 reads → now 1 batch query):

```go
func (s *Server) SendPayment(srcId, dstId, amount int64, timestamp types.CustomTs) error {
    txn := s.db.NewTransactionAt(toTs(timestamp), true)
    defer txn.Discard()

    // Prefetch all 4 keys in one SQL round-trip instead of 4 separate queries.
    _ = txn.PrefetchKeys([][]byte{
        []byte(fmt.Sprintf("%s_%s_%d", TABLENAME_ACCOUNTS, CUST_ID, srcId)),
        []byte(fmt.Sprintf("%s_%s_%d", TABLENAME_ACCOUNTS, CUST_ID, dstId)),
        []byte(fmt.Sprintf("%s_%s_%d", TABLENAME_CHECKING, BALANCE, srcId)),
        []byte(fmt.Sprintf("%s_%s_%d", TABLENAME_CHECKING, BALANCE, dstId)),
    })

    // All txn.Get() calls below will hit the cache — no more SQL queries.
    // ... rest of function unchanged ...
}
```

Apply the same pattern to all 6 SmallBank functions:

| Function | Keys to prefetch |
|---|---|
| `Balance` | accounts/id, savings/bal, checking/bal (same custId) |
| `DepositChecking` | accounts/id, checking/bal (same custId) |
| `TransactSavings` | accounts/id, savings/bal (same custId) |
| `WriteCheck` | accounts/id, savings/bal, checking/bal (same custId) |
| `SendPayment` | accounts/id×2, checking/bal×2 (srcId + dstId) |
| `Amalgamate` | accounts/id×2, savings/bal (srcId), checking/bal (dstId) |

---

## Expected Outcome

Before optimization:
```
Balance         Read=1.35ms   (3 SQL queries × ~0.45ms each)
SendPayment     Read=3.63ms   (4 SQL queries × ~0.9ms each)
```

After optimization:
```
Balance         Read=~0.5ms   (1 batch SQL query for all 3 keys)
SendPayment     Read=~1.0ms   (1 batch SQL query for all 4 keys)
```

Expected throughput improvement: 3–4× for read-heavy transactions (Balance,
WriteCheck, SendPayment, Amalgamate). Write-only overhead (DepositChecking,
TransactSavings) improves less since they only have 2 reads.

---

## How to Verify

Run the existing benchmark after the change:

```bash
cd ~/GolandProjects/darshan-badger

# Phase breakdown — should show read avg drop by 3-4×
go test -v -tags duckdb -run TestSmallBankDuckDBPhases -timeout 120s

# Per-type isolation — should show higher TPS for all types
go test -v -tags duckdb -run TestSmallBankDuckDB -timeout 300s

# Correctness — must still pass
go test -v -tags duckdb -run TestDuckDBBankDivytime -timeout 60s
```

Compare new numbers against the baseline table at the top of this file.

---

## Important Constraints

- Do NOT change the public `Get()` / `Set()` / `CommitAt()` signatures — they are
  called from many places and must stay backward compatible.
- The `PrefetchKeys` method must be a no-op when `db.duckDBStorage == nil` so the
  code works with regular (non-DuckDB) badger builds unchanged.
- The batch SQL query must use the same timestamp visibility semantics as the
  single-key `Read()` — the WHERE clause logic must be identical.
- Correctness test (`TestDuckDBBankDivytime`) must continue to pass after the change.
- All new code in `duckdb/storage.go` must have the `//go:build duckdb` tag inherited
  from the file header. All new code in `db.go` and `txn.go` is pure-Go and needs
  no build tag.
