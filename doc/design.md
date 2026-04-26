# Trade System Overview

![image-20210813103917795](https://github.com/rqzrqh/matchengine_rust/blob/master/image/trade_system.png?raw=true)

The client trade protocol covers `put_order` and `cancel_order`. There are two levels of message queues: the first is `offer.eth_btc`; the second is `settle.*`, with topic indices from 0 to 63, chosen as `user_id % 64`.

The offer service handles the client’s `put_order` / `cancel_order` requests, creates a pre-order and writes it to the database, and sends order/cancel messages to Kafka (topic `offer.eth_btc`). The matching engine consumes `offer.eth_btc`, updates local state, emits orders, deals, or errors, and publishes to Kafka (topic `settle.$idx`). The settle service consumes `settle.*` and updates users and their orders.

# Matching Engine

![image-20210813104403474](https://github.com/rqzrqh/matchengine_rust/blob/master/image/match_process.png?raw=true)

The matching engine is a state machine: `put_order` / `cancel_order` messages are inputs applied to local state (the order book), and the machine outputs orders, deals, or errors. Periodically it forks a process, dumps state to a snapshot, and persists it to the database. Snapshots can be ordered by time.

![image-20210813104749921](https://github.com/rqzrqh/matchengine_rust/blob/master/image/recover_process.png?raw=true)

1. Read the latest snapshot and obtain `quote_deals_id` and `min_settle_message_id`.
2. Walk snapshots by id from newest to oldest until you find one whose deal id is less than `quote_deals_id` and whose message id is less than `min_settle_message_id`.
3. Apply that snapshot.

# Correctness

**Upstream (market ordering module)**

1. Assign a sequence number to each request for the current market, push to the message queue in batches, and resend on failure or while messages are still in flight.

**The engine**

1. Inputs must be sorted by a contiguous `oper_id`; the engine discards non-contiguous requests.
2. Process serially and publish downstream data in order.
3. After recovery, replay to the correct input position and state; the corresponding output must already have been published to the message queue.

**Downstream (user settle module)**

1. Record the latest group message id and ignore non-contiguous messages.
2. Commit settle results and the settle-group offset in a single database transaction.
3. Recover state from the database.

**Downstream (quote deals module)**

1. Record the latest deal id and ignore non-contiguous deals.
2. Recover from the stored deal id.

# Scalability

The offer and settle services are user-scoped, so user and order tables can be sharded (for example by `user_id % 50`).

The in-memory matching engine is fast. One engine instance serves one market, so adding markets is straightforward.

Match results are split across multiple topics, which enables user-level load spreading while keeping per-topic ordering. Different topics can be consumed in parallel.

# Fault Tolerance

Every service can be deployed with multiple instances for fault tolerance.
