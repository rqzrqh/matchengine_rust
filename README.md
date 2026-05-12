### Overview

This project is an in-memory matching engine: small and simple, written in Rust as a learning exercise.
It is inspired by a longer-running C implementation of the same idea.

More detail: [design.md](https://github.com/rqzrqh/matchengine_rust/blob/master/doc/design.md).

### Test Architecture

![Test architecture diagram](https://raw.githubusercontent.com/rqzrqh/matchengine_rust/refs/heads/master/image/test_architecture.png)

For OS threads and Tokio runtimes inside the engine binary, see **`doc/thread-model.md`**; for messaging protocol notes see **`doc/design.md`**; for HTTP query endpoints see **`doc/http-api.md`**; for downstream publish tuning see **`doc/publish-performance.md`**.

### Deployment

#### Prerequisites

- **Rust** toolchain (this crate uses `edition = "2024"`).
- **MySQL** reachable from the host that runs the engine and (if used) **web-test**.
- **Apache Kafka** (`kafka-topics` on `PATH`; scripts assume `localhost:9092`).
- **Node.js 20+** and npm (only for the optional **web-test** console).

#### 1. Configure

Edit repo root **`config.yaml`** so that:

- **`market.name`** is the MySQL database name and the Kafka topic suffix (e.g. `eth_btc` → DB `eth_btc`, topics `offer.eth_btc`, `quote_deals.eth_btc`).
- Internal Kafka I/O uses MessagePack on both sides: input `offer.<market>` messages are unpacked as MessagePack, and output `quote_deals.<market>` / `settle` messages are published as MessagePack.
- **`brokers`** matches your Kafka bootstrap servers.
- **`db.addr` / `db.user` / `db.passwd`** match MySQL.
- **`main_task_queue_capacity`** bounds the queue into the single matcher thread. When Kafka input, HTTP requests, snapshot timers, or publish progress outpace the matcher, senders block instead of growing an unbounded queue.
- **`output_publish`** must define **`quote`** (`quote_deals.<market>`) and **`settle`** (single `settle` topic, partitioned by settle group) separately. Each has a **`kafka`** child for producer settings such as **`batch_num_messages`**, **`linger_ms`**, **`max_in_flight_requests_per_connection`**, local queue limits, compression, delivery timeout, and statistics interval; application pipeline settings such as bounded **`channel_capacity`**, **`drain_batch_size`**, **`max_outstanding`**, settle **`worker_max_outstanding`**, **`per_group_send_burst`**, and **`thread_count`** stay outside **`kafka`**. Settle publishing always uses Kafka idempotence with `acks=all`, so keep its `kafka.max_in_flight_requests_per_connection` ≤ 5. See root **`config.yaml`** for an example.
- **`snap_dump`** controls the forked snapshot interval; **`snap_cleanup`** controls periodic snapshot retention by max age and/or max count while protecting the snapshot that recovery would choose.

The engine reads this file (default path `./config.yaml` relative to the process working directory, or pass another path as the first CLI argument). It also accepts **`--wait-initial-status`**, used by profiling helpers to let one HTTP status request run before Kafka consumption starts. **web-test** resolves `config.yaml` from the repo root or from `web-test/../`.

#### 2. Database

From the **`deploy/`** directory:

```bash
cd deploy
./db_market.sh <MYSQL_HOST> <MYSQL_USER> <MYSQL_PASSWORD> <MARKET_NAME>
```

Example: `./db_market.sh localhost root 'secret' eth_btc`

This (re)creates the market database and loads **`deploy/table_template.sql`**.

#### 3. Kafka topics

Still under **`deploy/`**:

```bash
./kafka_market.sh <MARKET_NAME>    # offer.<market>, quote_deals.<market>
./kafka_settle.sh                  # settle topic with 64 partitions
```

Adjust bootstrap server inside the scripts if Kafka is not on `localhost:9092`. Startup validates that `offer.<market>` and `quote_deals.<market>` exist, that `settle` has at least 64 partitions, and that both the settle partition count and the engine's 64 settle groups divide cleanly across `output_publish.settle.thread_count`.

#### 4. Build and run the matching engine

From the **repository root**:

```bash
cargo build --release
RUST_LOG=info cargo run --release
# or: RUST_LOG=info cargo run --release /path/to/config.yaml
# profiling helpers may use: RUST_LOG=info cargo run --release -- --wait-initial-status /path/to/config.yaml
```

By default the engine exposes HTTP on **`http://127.0.0.1:8080`**.

#### 5. Optional: web-test console

The **`web-test/`** app is a small Express + Vite UI: it serves the page, exposes **`/api/config`** and order APIs, pushes offer traffic to Kafka (with optional outbox dispatch), and streams Kafka-derived feeds over WebSockets. The browser polls the **matching engine HTTP API directly** for status and order-book data (same URL as `engine_url` in **`GET /api/config`**, default **`http://127.0.0.1:8080`**; the engine must allow the page origin in CORS).

```bash
cd web-test
npm install
npm run dev
```

On startup, **`setupDatabase`** uses **TypeORM** (`synchronize: true`) against **`web-test/src/db/entities.ts`**: only those entity tables are created/updated; **any other tables already in the same MySQL database are left alone** (**`config.yaml` → `market.name`**, same **`db`** credentials as the engine).

Development server defaults to **`http://127.0.0.1:8000`** (see `web-test/src/config.ts`). Production-style run: **`npm run build && npm start`**.

REST surface on the engine (the UI calls these from the browser; web-test also implements **`/api/orders/*`** for placing orders):

- `GET /markets/:market/summary`
- `GET /markets/:market/status`
- `GET /markets/:market/orders/:order_id`
- `GET /markets/:market/order-book?side=&offset=&limit=`
- `GET /markets/:market/users/:user_id/orders?offset=&limit=`

### Testing

#### Rust crate

```bash
cargo test
```

The command runs the crate's unit tests and also verifies that the project builds under the test profile.

#### Matching core benchmark

The bundled Criterion benchmark measures only the in-memory matching core with a no-op publisher. It is useful for comparing code changes, but it is not the real engine TPS because it excludes Kafka consume/produce, wire encoding/decoding, channel handoff, OS scheduling, and database snapshot work:

```bash
cargo bench --bench matching_engine
```

Read Criterion's `thrpt` / `elem/s` output as a core upper-bound metric:

- `cross_limit_order_core_throughput`: in-memory limit-order crossing throughput; each submitted bid crosses one resting ask.
- `market_order_sweep_core_deal_throughput`: in-memory deal throughput when one market order sweeps many resting asks.
- `resting_limit_order_core_throughput`: in-memory order-book insert throughput for non-crossing limit orders.

To know deployable TPS, run the engine against Kafka/MySQL and drive `offer.<market>` with the same MessagePack message shape as production, then measure accepted input rate together with `quote_deals.<market>`, the partitioned `settle` topic, publish backlog, and engine latency.

#### Matching engine HTTP

With the engine running and `market.name` (e.g. `eth_btc`):

```bash
curl -sS "http://127.0.0.1:8080/markets/eth_btc/status"
curl -sS "http://127.0.0.1:8080/markets/eth_btc/summary"
```

Expect JSON responses; `404` if the path market does not match the running engine’s configured market.

#### web-test UI

With **`npm run dev`** in **`web-test/`**, open the printed URL (typically **`http://127.0.0.1:8000`**), confirm engine status / order book panels load, and that actions (if you wire orders through the UI) match your Kafka/MySQL setup.

#### Load / integration

For end-to-end integration testing, run the engine against a dev Kafka/MySQL stack, publish MessagePack orders to **`offer.<market>`** as your upstream does, and observe MessagePack outputs on **`quote_deals.<market>`** / the partitioned **`settle`** topic plus engine HTTP and DB state.

### Profiling

#### Time Profiler + HTTP state (Cursor skill)

The repo bundles a profiling skill at **`.cursor/skills/matchengine-xctrace-profile/`**. See **`SKILL.md`** there for full detail. In short:

- It records **`xcrun xctrace`** with Apple’s **Time Profiler** template, optionally stops an existing release binary (**`--stop-existing`**), and in parallel polls **`GET /markets/{market}/summary`** and **`GET /markets/{market}/status`** so CPU samples line up with matcher-visible backlog and book state.
- Default artifacts live under **`profiling/`** (override with **`--output-dir`**), including **`matchengine.trace`**, NDJSON/state logs, **`metadata.json`**, and **`PROFILE_REPORT.md`** tables that consume **`xctrace export`** where applicable.

Typical bundled run from the **repository root**:

```bash
SCRIPT_SKILL=".cursor/skills/matchengine-xctrace-profile/scripts"
"$SCRIPT_SKILL/profile_with_xctrace.sh" --stop-existing
# Optional: ./config.yaml, --time-limit 1m, --output-dir profiling/my-run
```

Open **`matchengine.trace`** in **Instruments** for interactive timeline and call-tree views; correlate timestamps with **`PROFILE_REPORT.md`** and **`doc/thread-model.md`**.

### Project layout

| Path | Role |
|------|------|
| **`src/`** | Matching engine (Rust) |
| **`deploy/`** | MySQL + Kafka bootstrap scripts |
| **`config.yaml`** | Shared YAML for engine and web-test |
| **`doc/`** | Design, HTTP API, thread model, and publish-performance notes |
| **`web-test/`** | Optional Node console (UI + APIs + Kafka helpers) |
