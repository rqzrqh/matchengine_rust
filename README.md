### Overview

This project is an in-memory matching engine: small and simple, written in Rust as a learning exercise.
It is inspired by a longer-running C implementation of the same idea.

More detail: [design.md](https://github.com/rqzrqh/matchengine_rust/blob/master/doc/design.md).

### Test Architecture

![Test architecture diagram](https://raw.githubusercontent.com/rqzrqh/matchengine_rust/refs/heads/master/image/test_architecture.png)

For OS threads and Tokio runtimes inside the engine binary, see the comment block at the top of **`src/main.rs`**; for messaging protocol notes see **`doc/design.md`**.

### Deployment

#### Prerequisites

- **Rust** toolchain (this crate uses `edition = "2024"`).
- **MySQL** reachable from the host that runs the engine and (if used) **web-test**.
- **Apache Kafka** (`kafka-topics` on `PATH`; scripts assume `localhost:9092`).
- **Node.js 20+** and npm (only for the optional **web-test** console).

#### 1. Configure

Edit repo root **`config.yaml`** so that:

- **`market.name`** is the MySQL database name and the Kafka topic suffix (e.g. `eth_btc` → DB `eth_btc`, topics `offer.eth_btc`, `quote_deals.eth_btc`).
- **`brokers`** matches your Kafka bootstrap servers.
- **`db.addr` / `db.user` / `db.passwd`** match MySQL.

The engine reads this file (default path `./config.yaml` relative to the process working directory, or pass another path as the first CLI argument). **web-test** also resolves `config.yaml` from the repo root or from `web-test/../`.

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
./kafka_settle.sh                # settle.0 .. settle.63
```

Adjust bootstrap server inside the scripts if Kafka is not on `localhost:9092`.

#### 4. Build and run the matching engine

From the **repository root**:

```bash
cargo build --release
RUST_LOG=info cargo run --release
# or: RUST_LOG=info cargo run --release /path/to/config.yaml
```

By default the engine exposes HTTP on **`http://127.0.0.1:8080`**.

#### 5. Optional: web-test console

The **`web-test/`** app is a small Express + Vite UI: it serves the page, exposes **`/api/config`** and order APIs, pushes offer traffic to Kafka (with optional outbox dispatch), and streams Kafka-derived feeds over WebSockets. The browser polls the **matching engine HTTP API directly** for status and order-book data (same URL as `engine_url` in **`GET /api/config`**, default **`http://127.0.0.1:8080`**; the engine must allow the page origin in CORS).

```bash
cd web-test
npm install
npm run dev
```

On startup, **`setupDatabase`** applies the bundled DDL in **`web-test/drizzle/schema.sql`** to the MySQL database named by **`config.yaml` → `market.name`** (using the same credentials as the engine). You only need a manual **`npm run db:migrate`** (with **`DATABASE_URL`** set) if you use Drizzle Kit CLI tools such as **`db:studio`**.

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

The crate may ship with **no** `#[test]` cases yet (`running 0 tests` is normal); the command still verifies that the project **builds** under the test profile.

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

There is no bundled load-test binary in this repo. For integration testing, run the engine against a dev Kafka/MySQL stack, publish orders to **`offer.<market>`** as your upstream does, and observe **`quote_deals.<market>`** / **`settle.*`** plus engine HTTP and DB state.

### Project layout

| Path | Role |
|------|------|
| **`src/`** | Matching engine (Rust) |
| **`deploy/`** | MySQL + Kafka bootstrap scripts |
| **`config.yaml`** | Shared YAML for engine and web-test |
| **`doc/`** | Design notes (`design.md`) |
| **`web-test/`** | Optional Node console (UI + APIs + Kafka helpers) |

### TODO

1. Use ordered batch sends for higher data-push throughput.

2. Consider a richer decimal library for rescaling: `rust_decimal`’s rescale helper only adjusts the fractional part, not the integer part.

