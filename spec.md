# disheap — Implementation Spec (v1)

## 0) Goals & non-goals

* **Goal:** clustered, partitioned priority messaging with **per-topic min/max heaps**, **bounded staleness (top-K)** pops, **exactly-once (engine→consumer)** delivery, WAL-backed durability, and a simple ops surface.
* **Non-goals (v1):** cross-tenant RBAC, global total order, end-to-end exactly-once (consumer side effects), HTTP push callbacks, sophisticated UI.

---

## 1) Topology & components

* **disheap-engine (Go):** owns partitions, replication (Raft), storage, heap ops, leasing, backoff, DLQ, snapshots, rebalancing.
* **disheap-api (Go):** HTTP/JSON→gRPC gateway, auth (email/password for UI; API keys for SDK), admin & CRUD, stats.
* **disheap-frontend:** minimal dashboard (heap CRUD, config, token generation, coarse metrics), periodic refresh.
* **disheap-python (asyncio SDK):** gRPC client with streaming pops, credit-based flow control, automatic lease renewal, idempotent producer helpers, structured logs, OTEL.

**Cluster coordination**

* **Membership:** gossip (SWIM/memberlist).
* **Consensus:** **Raft per partition** (RF=2 or RF=3; configurable).
* **Metadata:** a small **meta Raft group** stores cluster layout (topics, partitions, replicas, assignments, config).

---

## 2) Core model

### Topics (heaps)

* Attributes: `topic_name`, `mode` (MIN|MAX), `K` (top-K bound), `partitions` (N), `replication_factor` (2|3), `retention_time`, `dlq_policy`, `visibility_timeout_default`, `max_retries_default`, `max_payload_bytes` (default 1 MiB), `compression` (on-wire enabled).
* Partitioning:

  * **By topic** (topics independent).
  * **Within topic:** partitions map by **partition key** (consistent hashing). Large topics may additionally be **segmented by subtrees** (see §4).

### Message

```
message_id (opaque, engine-assigned ULID)
topic
partition_id
priority int64
enqueued_time (engine clock; best-effort)
payload (opaque bytes; API validates JSON if flagged)
producer_id, epoch, seq (for idempotent enqueue)
attempts, max_retries
not_before (for backoff / delayed visibility)
lease: {token, holder, deadline}  // present when out on lease
```

**Ordering rule:** `(priority, enqueued_time)` with **arbitrary order on ties**; **bounded staleness:** popped item is within **top-K** of the logical global heap for that topic.

---

## 3) Heap representation & algorithms

### Local (per-partition) structure

* **In-memory indexed binary heap** (min or max) storing `(priority, enq_time, message_id)` entries.
* Separate **timer min-heap** for `not_before` (delayed/backoff) items; ready items move to main heap.
* **On-disk:** messages and indexes persisted via Raft→storage; in-memory heaps rebuilt on restart (see §7).

**Why binary heap?** Predictable O(log n) insert/extract, simple indexing, cache-friendly, and plays well with a **hierarchical candidate index** for global pops. Pairing/Fibonacci heaps help meld, but we favor simpler, predictable GC profile and straightforward snapshots.

### Hierarchical “spine index”

* For each topic, maintain a **Spine Index**: a tiny coordinator heap over **per-partition candidates**.
* Each partition exposes a **candidate set**: its root plus **a bounded fan-out** (e.g., top 2–4 nodes) to support **top-K** global selection without roundtrips.
* **Multi-partition pop:** choose the best from the Spine Index; when a partition’s root is popped, it **promotes** its next candidate into the index.
* **Bound control:** with fan-out F and P partitions, the Spine Index tracks ≤`P*F` items, guaranteeing a **rank-K** pop with K≈`P*F`. Topic-level `K` config sets `F`.

**Suspension semantics:** during repartitioning or snapshot cutovers, affected partitions mark **throttled**; SDK `PopStream` simply blocks longer (no errors).

---

## 4) Partitioning & distribution

### Mapping

* **Partition key → partition** via **rendezvous hashing** (or ring hashing) for stable movement under scale changes.
* **Subtree segmentation:** each partition can split locally into **segments** (subtrees). Only the **near-root “spine”** nodes are exposed to the Spine Index; deep subtrees remain shard-local. No cross-shard node moves in steady state.

### Replication

* **Per-partition Raft** with leader serving reads/writes; followers apply logs and serve as hot standbys.
* **Write quorum:** leader fsync (local) before commit; followers apply asynchronously but must catch up before leadership transfer.

---

## 5) Delivery & leases

### Exactly-once (engine→consumer)

* **Leasing:** `Pop` assigns an opaque `lease_token` with `deadline = now + visibility_timeout`. The message remains **logically owned** by the consumer.
* **Ack:** persisted (Raft committed & fsynced) **before** ack response; message becomes deleted.
* **Nack/timeout:** message moves to backoff: `not_before = now + backoff(attempts)`; attempts++.
* **Lease extension:** `Extend(lease_token, +Δ)`; engine enforces caps and staleness guards.
* **No concurrent lease:** engine prevents a message from being leased to >1 consumer simultaneously.

### Flow control

* **Bidirectional gRPC stream:** consumer announces credits with `FlowControl(credits=N)`; engine sends up to N leased messages; credits decrement on delivery; client refills.

---

## 6) Idempotent enqueue

* Producers include `(producer_id, epoch, seq)`.
* Engine maintains a **dedup index** per `(topic, producer_id, epoch)` of recent `seq`s:

  * **Sliding TTL window** (configurable; default 24h) in a compact bitmap/bloom+LRU.
  * Duplicate enqueues return existing `message_id`.

**Batch atomicity (enqueue)**

* **All-or-nothing across partitions** using **2PC**:

  * **Coordinator:** engine co-locates a transient coordinator on the first partition leader touched.
  * **Prepare:** each partition appends a **prepare** entry (Raft) and reserves IDs.
  * **Commit/Abort:** after all prepares commit, coordinator issues commit; prepared entries auto-abort on timeout/leader failover.

---

## 7) Durability, storage & snapshots

### Storage engine

* **BadgerDB** (pure Go, simpler ops) per partition replica.

  * **Raft log**: dedicated CF/prefix; **messages**: keyed by `(topic, partition, message_id)`; **state** (heap index snapshot metadata, leases, dedup).
  * **Fsync policy:** ack enqueue only after **local fsync** of Raft commit (matches requirement).
* Optional on-disk compression (Snappy/Zstd) for payload blocks.

### Snapshots & recovery

* **Incremental snapshots** per partition (Raft): include message metadata, lease table, dedup index, and a compact dump of the **heap array** and **timer heap**.
* **Cutover budget:** < **10s** pause per partition; prefer **asynchronous** snapshotting with chunked transfer to followers and object storage (optional).
* **Recovery:** apply last snapshot, then replay Raft log; rebuild in-memory heaps from persisted indexes; messages with `lease` past deadline re-enter ready set via timer heap.

**Retention**

* Time-based (per topic). Compaction removes expired messages (acked or DLQ’d) beyond retention.

---

## 8) Backoff, retries, DLQ

* **Exponential backoff with jitter**; per-message `max_retries` (default from topic config).
* On `attempts > max_retries`, **move to DLQ** for the topic (dedicated heap/partition set).
* DLQ supports `Peek/List/Replay/Delete`.

---

## 9) Consistency & staleness contract

* **Linearizable writes** within a partition (Raft).
* **Global pop bounded staleness:** popped item is within **top-K** of the logical topic heap (configurable). Implementation via Spine Index fan-out sizing.
* **Clock skew:** tolerated; time only used as a tiebreaker and visibility scheduling.

---

## 10) APIs

### Transport

* **Frontend/API users:** HTTP/JSON (disheap-api) → gRPC to engine.
* **SDK:** direct gRPC to engine (mTLS optional later; API key required).

### gRPC (selected)

```proto
service Disheap {
  // Admin
  rpc MakeHeap (MakeHeapReq) returns (MakeHeapResp);
  rpc DeleteHeap (DeleteHeapReq) returns (Empty);
  rpc UpdateHeapConfig (UpdateHeapConfigReq) returns (Empty);
  rpc ListHeaps (ListHeapsReq) returns (ListHeapsResp);
  rpc Stats (StatsReq) returns (StatsResp);
  rpc Purge (PurgeReq) returns (Empty);
  rpc MoveToDLQ (MoveToDlqReq) returns (Empty);

  // Data
  rpc Enqueue (EnqueueReq) returns (EnqueueResp);             // single
  rpc EnqueueBatch (EnqueueBatchReq) returns (EnqueueBatchResp); // 2PC
  rpc PopStream (stream PopOpen) returns (stream PopItem);    // credits+items
  rpc Ack (AckReq) returns (Empty);
  rpc Nack (NackReq) returns (Empty);
  rpc Extend (ExtendReq) returns (Empty);
  rpc Peek (PeekReq) returns (PeekResp);
}

message MakeHeapReq { string topic; enum Mode { MIN=0; MAX=1; } Mode mode=2; uint32 partitions=3; uint32 rf=4; uint32 K=5; ... }
message EnqueueReq { string topic; bytes payload; int64 priority; string partition_key; string producer_id; uint64 epoch; uint64 seq; uint32 max_retries; google.protobuf.Duration not_before; }
message EnqueueResp { string message_id; bool duplicate; }
message PopOpen { oneof kind { FlowControl fc=1; PopFilter filter=2; } }
message FlowControl { uint32 credits; }
message PopItem { string message_id; bytes payload; int64 priority; string lease_token; google.protobuf.Timestamp lease_deadline; uint32 attempts; }
message AckReq { string topic; string message_id; string lease_token; }
message NackReq { string topic; string message_id; string lease_token; google.protobuf.Duration backoff_override; }
message ExtendReq { string topic; string lease_token; google.protobuf.Duration extension; }
```

### HTTP/JSON (grpc-gateway)

* `POST /v1/heaps`, `DELETE /v1/heaps/{topic}`, `PATCH /v1/heaps/{topic}`, `GET /v1/heaps`, `GET /v1/stats`.
* `POST /v1/enqueue`, `POST /v1/enqueue:batch`, `POST /v1/ack`, `POST /v1/nack`, `POST /v1/extend`, `GET /v1/peek`.
* Admin safety: destructive ops require `?force=true` + confirmation token.

### Errors → canonical gRPC codes

* Validation → `INVALID_ARGUMENT`; lease conflicts → `FAILED_PRECONDITION`; contention/2PC conflicts → `ABORTED`; missing → `NOT_FOUND`; unauth → `UNAUTHENTICATED`.

---

## 11) Python SDK (asyncio)

### Features

* `Client(api_key, endpoints, tls=None)`.
* **Async producer:** `enqueue()`, `enqueue_batch()` with idempotency helper `(producer_id, epoch, seq)` and automatic sequencing per producer.
* **Async consumer:** `pop_stream(topic, prefetch=N, visibility=…)` yielding items with context manager:

  ```python
  async with consumer.item() as msg:
      ...  # process
      await msg.ack()  # or msg.nack(backoff=...)
  ```
* **Auto-extend leases** with heartbeats while a handler is running.
* **Flow control:** automatic credit refills based on concurrency.
* **OpenTelemetry:** traces for enqueue/pop/ack with topic/partition attributes; structured logging.

---

## 12) Security & auth

* **SDK/API access:** **API keys** (engine-generated via dashboard). In v1, keys are **unscoped** (cluster-wide). Stored hashed (Argon2id) with **last-used** timestamp.
* **Frontend login:** **email/password** (PBKDF2/Argon2id) on disheap-api; short-lived JWT session for the browser.
* **Transport:** TLS everywhere (cluster internal can start plaintext with a clear migration path).

---

## 13) Observability

* **Metrics (Prometheus via OTEL):**

  * `enqueue_latency_ms{topic}`, `pop_latency_ms{topic}`, `inflight{topic}`, `backlog{topic,partition}`, `retries_total{topic}`, `dlq_total{topic}`, `lease_timeouts_total{topic}`, `raft_apply_latency_ms{partition}`, `snapshot_bytes{partition}`, `wal_bytes_written_total`.
* **Tracing:** SDK→API→Engine spans; attributes: `topic`, `partition`, `message_id` (sampled), `producer_id`.
* **Logging:** JSON; correlation IDs per request; payload redaction by default.

---

## 14) Deployment (Kubernetes)

* **StatefulSets** for engine nodes with **PV gp3** (default), tunable IOPS; **anti-affinity** across AZs; one container exposes Raft + gRPC ports.
* **Horizontal scale:** scale by partitions and nodes; manual partition count & RF; engine auto-places replicas via constraints.
* **Helm values:** storage class, RF, partitions per topic, K, resource requests/limits, TLS, max payload, visibility defaults.
* **Upgrades:** rolling; engine supports mixed versions for ≥1 minor; API/frontend can be fully destroyed/redeployed without data loss.

---

## 15) Operations & lifecycle

* **Rebalance / expand cluster:** operator adjusts partition/replica counts; engine migrates with throttled background copy; affected partitions mark **throttled** → SDK pops block.
* **Snapshots:** periodic (default 10 min) and on-demand; exported size/age exposed via metrics and UI.
* **Backups (optional):** ship snapshots to object storage.
* **Limits:** per-topic max inflight, per-consumer max concurrent leases, global max payload (reject oversize), max open streams per key.

---

## 16) Performance targets (initial)

* **Throughput:** ≥ **1000 QPS** cluster-wide under RF=3, 20 partitions.
* **Backlog:** topic heaps up to **2M messages** (distributed).
* **Latency targets (non-SLO):** enqueue p95 < 20ms; pop p95 < 50ms under light contention.
* **Storage:** fsync on commit; aim for ≤5ms median on gp3; tune Badger value log GC for steady state.

---

## 17) Validation plan

* **Correctness:** property tests for heap order under concurrent enq/pop; lease exclusivity; dedup; 2PC atomicity.
* **Chaos:** leader failover mid-commit; snapshot during heavy load; partition network splits; clock skew injection.
* **Benchmarks:** enqueue/pop microbench; end-to-end with SDK; backoff/DLQ pressure tests; large backlog recovery.

---

## 18) Frontend (v1)

* Views: **Heaps list**, **Heap detail** (config, partitions, RF, K, basic metrics), **Keys** (create/delete), **DLQ browse** (peek, replay, delete).
* Auth: email/password; JWT cookie; simple forms; polling (5–10s).

---

## 19) Wire & storage details (selected)

**Raft entries**

* `ENQ`, `ACK`, `NACK`, `LEASE_EXTEND`, `BATCH_PREPARE`, `BATCH_COMMIT/ABORT`, `CONFIG_UPDATE`, `SNAPSHOT_MARK`.

**Badger keys (prefixes)**

* `m/<topic>/<part>/<msg_id> -> payload+meta`
* `l/<topic>/<part>/<msg_id> -> lease state`
* `d/<topic>/<producer>/<epoch>/<seq> -> msg_id (TTL)`
* `c/<topic>/<part>/heap_snapshot -> binary blob`
* `t/<topic>/<part>/timer_snapshot -> binary blob`

---

## 20) Security notes

* Keys are **opaque**, revocable; last-used and creation timestamps visible in UI.
* Admin endpoints require **explicit confirmation** for destructive ops; audit log entries (JSON lines) emitted from API.

---

## 21) Open implementation choices (preset in this spec)

* **Heap:** in-memory binary heap with **Spine Index** for global top-K.
* **Storage:** **BadgerDB**; fsync on Raft commit.
* **Global pop:** Spine fan-out `F` sized from topic `K` and partition count `P` (e.g., `F = ceil(K / P)`).

---
