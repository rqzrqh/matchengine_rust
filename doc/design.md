# Trade system overview

![image-20210813103917795](https://github.com/rqzrqh/matchengine_rust/blob/master/image/trade_system.png?raw=true)

The client trade protocol covers `put_order` and `cancel_order`. One engine process serves one configured market (`market.name` in `config.yaml`). For market `eth_btc`, the input topic is `offer.eth_btc`, quote output is `quote_deals.eth_btc`, and user-settle output is the single Kafka topic `settle` partitioned by `user_id % 64`.

The offer service handles the client’s `put_order` / `cancel_order` requests, creates a pre-order and writes it to the database, and sends order/cancel messages to Kafka (`offer.<market>`, partition 0 for this market). The matching engine consumes `offer.<market>`, updates local state, emits quote deal ticks to `quote_deals.<market>`, and emits user-settle messages to the `settle` topic partition selected by settle group. The settle service consumes the `settle` partitions and updates users and their orders.

Internal Kafka messages are MessagePack in both directions: offer input is decoded from MessagePack, and quote / settle outputs are encoded as MessagePack with string field names.

Protocol type fields are numeric. Offer input uses `method`: `1` limit order, `2` market order, `3` cancel. Quote output uses `type=1` for quote deal ticks. Settle output uses `type`: `1` put order, `2` cancel order, `3` error, `4` deals.

## Offer input envelope

The Kafka payload on `offer.<market>` is decoded as:

| Field | Type | Meaning |
| --- | --- | --- |
| `method` | number | `1` limit order, `2` market order, `3` cancel |
| `id` | number | External request id, echoed in downstream settle messages |
| `input_sequence_id` | number | Contiguous market input sequence id |
| `params` | object | Method-specific request body |

`params` is strict per method:

| Method | Required params |
| --- | --- |
| `1` limit order | `user_id`, `side`, `amount`, `price`, `taker_fee_rate`, `maker_fee_rate` |
| `2` market order | `user_id`, `side`, `amount`, `taker_fee_rate` |
| `3` cancel | `user_id`, `order_id` |

`side` is `1` ask or `2` bid. Decimal values are strings and are validated against the configured market precision. Price precision is derived from `money_prec - stock_prec`, so startup rejects `stock_prec > money_prec`.

# Matching engine

![image-20210813104403474](https://github.com/rqzrqh/matchengine_rust/blob/master/image/match_process.png?raw=true)

The matching engine is a state machine: `put_order` / `cancel_order` messages are inputs applied to local state (the order book), and the machine outputs orders, deals, or errors. The main thread owns `Market` exclusively; Kafka input, HTTP reads, snapshot timers, and publish progress all enter through one bounded main task queue. See `thread-model.md` for thread and queue details.

Open limit orders are stored in ask/bid ordered skip lists. Asks sort by ascending price then order id; bids sort by descending price then order id. User open-order lists and the order id index are updated on the same main thread.

The engine periodically forks a child process, dumps state to a snapshot table, and persists progress to MySQL. Snapshot rows can be ordered by time and include input progress, generated output ids, and Kafka-published output progress.

![image-20210813104749921](https://github.com/rqzrqh/matchengine_rust/blob/master/image/recover_process.png?raw=true)

1. Read the newest snapshot row and use it as the output-progress anchor: `pushed_quote_deals_id` and `pushed_settle_message_ids`.
2. Walk snapshots by id from newest to oldest until you find one whose `deals_id` is not greater than `pushed_quote_deals_id` and whose per-group `settle_message_ids` are not greater than `pushed_settle_message_ids`.
3. Apply that snapshot, then resume `offer.<market>` partition 0 from `input_offset + 1`.

This avoids restoring a state that contains matcher output which was not confirmed as published to Kafka.

# Correctness

**Upstream (market ordering module)**

1. Assign a contiguous `input_sequence_id` to each request for the current market.
2. Put the market's requests on `offer.<market>` partition 0 in that sequence.
3. Resend on failure or while messages are still in flight.

**The engine**

1. Inputs must advance by contiguous `input_sequence_id`; stale ids are skipped and gaps are rejected.
2. Matching runs serially on the main thread; `Market` is not shared across worker threads.
3. Quote output is published to `quote_deals.<market>`; settle output is published to the `settle` partition for `user_id % 64`.
4. Publish workers report progress only after Kafka delivery acknowledgement. The main thread updates `pushed_quote_deals_id` and `pushed_settle_message_ids`.
5. After recovery, replay starts from the restored input position and a snapshot whose corresponding output has already been published.

**Downstream (user settle module)**

1. Consume the `settle` topic partitions.
2. Record the latest message id for each settle group and ignore non-contiguous messages.
2. Commit settle results and the settle-group offset in a single database transaction.
3. Recover state from the database.

**Downstream (quote deals module)**

1. Record the latest deal id and ignore non-contiguous deals.
2. Recover from the stored deal id.

# Scalability

The offer and settle services are user-scoped, so user and order tables can be sharded (for example by `user_id % 50`).

The in-memory matching engine is fast. One engine instance serves one market, so adding markets is straightforward.

Match results are split by output role: quote ticks use `quote_deals.<market>`, while user-settle messages use 64 settle groups mapped to Kafka partitions on the `settle` topic. Different settle partitions can be consumed and published in parallel while preserving per-group ordering.

# Fault Tolerance

Every surrounding service can be deployed with multiple instances for fault tolerance. The matching engine itself uses a process lock per market, so only one active process should own a market at a time; use process supervision to restart it after failure.

Startup validates the configured Kafka topics before matching begins: `offer.<market>` and `quote_deals.<market>` must exist, `settle` must have at least 64 partitions, and settle partitions / settle groups must divide cleanly across `output_publish.settle.thread_count`.
