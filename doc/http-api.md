# Matching engine HTTP API

Read-only, in-process query endpoints. Each engine process serves **one** market (`market.name` in `config.yaml`). The `:market` path segment must match that name; otherwise the server returns **404** with `{"error":"unknown market"}`.

**Note**: single process, single market; the bind address is hard-coded in `src/main.rs` as `127.0.0.1:8080` (infer the base URL from that; if you change the source locally, use your actual run).

## General conventions

| Item | Behavior |
|------|----------|
| Method | **GET** only; other methods on undefined routes return **405** with plain text `method not allowed` |
| Content-Type | Successful bodies are **JSON** with `Content-Type: application/json` |
| CORS | Permissive (`CorsLayer::permissive`); success and some errors include `Access-Control-Allow-Origin: *` |
| Unmatched GET | **404** with plain text `not found` |
| Unknown market | **404**, JSON: `{"error":"unknown market"}` |
| Business errors | Shape `{"error":"..."}` |

Axum forwards each request to the main matcher thread; execution is a synchronous blocking read of matcher state.

---

## 1. Market summary

`GET /markets/{market}/summary`

| HTTP status | Meaning |
|-------------|---------|
| 200 | OK |
| 404 | Market name mismatch |

**Successful response fields**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Market name |
| `ask_count` | number | Number of ask orders |
| `bid_count` | number | Number of bid orders |
| `ask_stock_amount` | string | Sum of `left` across asks |
| `ask_money_amount` | string | Sum of `price * left` across asks |
| `bid_stock_amount` | string | Sum of `left` across bids |
| `bid_money_amount` | string | Sum of `price * left` across bids |

---

## 2. Market internal status / cursors

`GET /markets/{market}/status`

| HTTP status | Meaning |
|-------------|---------|
| 200 | OK |
| 404 | Market name mismatch |

**Successful response fields**

| Field | Type | Description |
|-------|------|-------------|
| `oper_id` | number | Operation id |
| `order_id` | number | Order id cursor |
| `deals_id` | number | Deal id cursor |
| `message_id` | number | Message id cursor |
| `settle_message_ids` | array | Per settle-group message ids |
| `input_offset` | number | Input cursor (e.g. Kafka) |
| `input_sequence_id` | number | Input sequence id |
| `pushed_quote_deals_id` | number | Last quote deal id confirmed published |
| `pushed_settle_message_ids` | array | Per-group ids confirmed published to settle |

---

## 3. Order detail

`GET /markets/{market}/orders/{order_id}`

Path `order_id` is an unsigned 64-bit integer.

| HTTP status | Meaning |
|-------------|---------|
| 200 | Order found |
| 404 | Unknown market or `order not found` |

**Order object** (root JSON on 200; fields from `Order::to_json`)

| Field | Type | Description |
|-------|------|-------------|
| `id` | number | Order id |
| `type` | number | `1` limit, `2` market (same constants as `market`) |
| `side` | number | `1` ask, `2` bid |
| `create_time` | number | Created at |
| `update_time` | number | Updated at |
| `user_id` | number | User id |
| `price` | string | Price (market price scale) |
| `amount` | string | Amount (`stock_prec`) |
| `taker_fee_rate` | string | Taker fee rate (`fee_rate_prec`) |
| `maker_fee_rate` | string | Maker fee rate (`fee_rate_prec`) |
| `left` | string | Remaining size; market **buy** uses **money** precision (`money_prec`), else `stock_prec` |
| `deal_stock` | string | Filled stock (`stock_prec`) |
| `deal_money` | string | Filled notional (`money_prec`) |
| `deal_fee` | string | Fee paid (`money_prec`) |

---

## 4. Order book (depth)

`GET /markets/{market}/order-book?side={side}&offset={offset}&limit={limit}`

| Param | Required | Default | Limits | Description |
|-------|----------|---------|--------|-------------|
| `side` | yes | — | must be `1` or `2` | `1` = asks, `2` = bids |
| `offset` | no | `0` | — | Skip this many levels from the start |
| `limit` | no | `12` | clamped **1–500** | Page size |

| HTTP status | Meaning |
|-------------|---------|
| 200 | OK (invalid `side` never returns 200 here) |
| 400 | `side` not 1/2: `{"error":"invalid side (use 1=ask, 2=bid)"}` |
| 404 | Market name mismatch |

**Successful response**

| Field | Type | Description |
|-------|------|-------------|
| `limit` | number | Requested limit after clamping |
| `offset` | number | Requested offset |
| `total` | number | Order count on that side |
| `orders` | array | Orders; same shape as [§3 Order detail](#3-order-detail) |
| `stock_amount` | string | Present when `side=1` (asks): market aggregate in `stock_prec` |
| `money_amount` | string | Present when `side=2` (bids): market aggregate in `money_prec` |

Asks and bids are sliced from their respective books with the engine’s ordering (price/time per implementation).

---

## 5. User open orders

`GET /markets/{market}/users/{user_id}/orders?offset={offset}&limit={limit}`

| Param | Required | Default | Limits |
|-------|----------|---------|--------|
| `offset` | no | `0` | — |
| `limit` | no | `50` | clamped **1–500** |

| HTTP status | Meaning |
|-------------|---------|
| 200 | OK (`total=0`, `orders=[]` if user has no orders) |
| 404 | Market name mismatch |

**Successful response**

| Field | Type | Description |
|-------|------|-------------|
| `limit` | number | Limit after clamping |
| `offset` | number | Offset |
| `total` | number | Count of open orders for the user |
| `orders` | array | Same shape as [§3](#3-order-detail) |

---

## Examples

Default base URL:

```http
GET http://127.0.0.1:8080/markets/BTCUSDT/summary
GET http://127.0.0.1:8080/markets/BTCUSDT/status
GET http://127.0.0.1:8080/markets/BTCUSDT/orders/1001
GET http://127.0.0.1:8080/markets/BTCUSDT/order-book?side=1&offset=0&limit=20
GET http://127.0.0.1:8080/markets/BTCUSDT/users/42/orders?offset=0&limit=50
```

Replace `BTCUSDT` with the market name from your `config.yaml`.

---

## Source map

| Doc section | Code |
|-------------|------|
| Routes, query defaults, clamps | `src/http/server.rs` |
| Status codes and JSON bodies | `src/http/handlers.rs` (`handle_http_request`) |
| Order JSON field scaling | `src/market.rs` (`Order::to_json`) |
| Operation enum | `src/task.rs` (`HttpOp`) |
