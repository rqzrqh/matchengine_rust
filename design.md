# Trade System Overview

![image-20210813103917795](https://github.com/rqzrqh/matchengine_rust/blob/master/image/trade_system.png?raw=true)

The trade protocol for client contains put_order and cancel_order.There're two level message queue, the first level is offer.eth_btc, the second level is settle.*, which is from 0~63, calculated by user_id%64.

Offer_service process client's put_order/cancel_order request, generates a pre-order and write to db, sends order/cancel_order message to kafka(topic=offer.eth_btc). Matchengine consumes offer.eth_btc, changes local state, generates order/deals/error, sends to kafka(topic=settle.$idx ).Settle_service consumes settle.* and changes the users and user orders.

# Matchengine

![image-20210813104403474](https://github.com/rqzrqh/matchengine_rust/blob/master/image/match_process.png?raw=true)

Matchengine is a state matchine, put_order/cancel_order message were treated as input, applied to local state(order book), and statemachine produces order/deals/errors as output.Periodically, it forks process, dumps state to snapshot and writes to db. Snapshots can be sorted by time.



![image-20210813104749921](https://github.com/rqzrqh/matchengine_rust/blob/master/image/recover_process.png?raw=true)

1.read the latest snap, get the quote_deals_id and min_settle_message_id

2.walk all snaps by id from large to small , find a snap which deal id is less than quote_deals_id and message_id is less than min_settle_message_id

3.apply the snap



# Correctness

upper stream(market sort module)

1.number each request of current market, push to mq in batch, if failed or on the fly, resend it.



the engine

1.input must be sorted by continuous oper_id, the engine discard incontinuous input request.

2.serial processing, push-data orderly

3.recover to the  input position and state correctly, corresponding output must has been pushed to the mq.



down stream(user settle module)

1.record latest group message id, and ignore incontinuous message.

2.commit settle result and settle group offset in database transaction

3.recover from database



down stream(quote deals module)

1.record latest deals id, and ignore incontinuous deals.

2.recover from deals id.



# Scalability

Offer_service and settle_service process user level data, so user table and order table can be splited into many tables by user_id%50.

Memory-matchengine is high performance. One matchengine corresponds one market, so it's easy to add new market.

The match results are splited into multiple topics, this made user-level load balance and topic-level message orderly. Different Topics can be  processed concurrency.



# Fault-Tolerant

All service can set up multiple services for fault-tolerant





