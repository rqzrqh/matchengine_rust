# Trade System Overview

![image-20210813103917795](https://github.com/rqzrqh/matchengine_rust/blob/master/image/trade_system.png?raw=true)

The trade protocol for client contains put_order and cancel_order.There're two level message queue, the first level is offer.eth_btc, the second level is settle.*, which is from 0~63, calculated by user_id%64.

Offer_service process client's put_order/cancel_order request, generates a pre-order and write to db, sends order/cancel_order message to kafka(topic=offer.eth_btc). Matchengine consumes offer.eth_btc, changes local state, generates order/deals/error, sends to kafka(topic=settle.$idx ).Settle_service consumes settle.* and changes the users and user orders.

# Matchengine

![image-20210813104403474](https://github.com/rqzrqh/matchengine_rust/blob/master/image/match_process.png?raw=true)

Matchengine is a state matchine, put_order/cancel_order message were treated as input, applied to local state(order book), and statemachine produces order/deals/errors as output.Periodically, it forks process, dumps state to snapshot and writes to db. Snapshots can be sorted by time.



mysql> show tables;

+-----------------------+

| Tables_in_eth_btc   |

+-----------------------+

| snap         |

| snap_order_1628750228 |

| snap_order_1628750828 |

| snap_order_1628751428 |

| snap_order_example  |

+-----------------------+

14 rows in set (0.00 sec)



mysql> select * from snap;

+----+------------+---------+----------+----------+------------+--------------+

| id | time    | oper_id | order_id | deals_id | message_id | input_offset |

+----+------------+---------+----------+----------+------------+--------------+

| 1 | 1628750228 |    4 |    4 |    1 |     6 |      3 |

| 2 | 1628750828 |    4 |    4 |    1 |     6 |      3 |

| 3 | 1628751428 |    4 |    4 |    1 |     6 |      3 |

+----+------------+---------+----------+----------+------------+--------------+

12 rows in set (0.00 sec)



mysql> select * from snap_order_1628751428;

+----+---+------+-------------+-------------+---------+-------+--------+----------------+----------------+-------+------------+------------+----------+

| id | t | side | create_time | update_time | user_id | price | amount | taker_fee_rate | maker_fee_rate | left | deal_stock | deal_money | deal_fee |

+----+---+------+-------------+-------------+---------+-------+--------+----------------+----------------+-------+------------+------------+----------+

| 1 | 1 |  2 | 1628749643 | 1628749687 |    0 | 100.0 | 100.0 | 0.1      | 0.1      | 90.0 | 10.0    | 1000.00  | 1.00   |

| 2 | 1 |  2 | 1628749645 | 1628749645 |    0 | 100.0 | 100.0 | 0.1      | 0.1      | 100.0 | 0     | 0     | 0    |

| 4 | 1 |  1 | 1628749756 | 1628749756 |   11 | 102.0 | 20.0  | 0.1      | 0.1      | 20.0 | 0     | 0     | 0    |

+----+---+------+-------------+-------------+---------+-------+--------+----------------+----------------+-------+------------+------------+----------+

3 rows in set (0.00 sec)



All ids are continuous increasing.



![image-20210813104749921](https://github.com/rqzrqh/matchengine_rust/blob/master/image/recover_process.png?raw=true)



# Correctness

Once offer_service receives a put_order request, it would check and generate a pre-order in db.

The message sequence of any users in a market the settle_service see are put_order->as taker deals->as taker deals->as maker deals->as maker deals->...->cancel_order.

Once received a put_order message, it uses extern_id in message to associate with a pre-order in db, and creates a order.

Once received a deals message, (either as taker or as maker) it changes the corresponding user asset and order asset.

Once received a cancel_order messaege, it cancels the corresponding order in db.



# Exception Handling

Settle_service is data strong consistency by write order, asset and consume offset to db in sql transaction. Matchengine dumps consume-offset and engine's state to db periodically,  input and state are consistent. When matchengine restarted, selected the latest state in db may caused output loss and raise an exception! Because the corresponding output may not had been sent to message queue! 

**How to find a corrent snapshot?**

The key is the corresponding output of the state must had been sent to message queue or processed by settle_service. Through the metadata of the message queue topic we can‘t obtain any valuable information about recovering . So we should read from the consume state in settle_service's db.



*step1: read all message_id from db*

```
select 'topic','message_id' from 'market_settle_message_id' where'market'='eth_btc'
```

| topic     | market   | message_id |
| --------- | -------- | ---------- |
| settle.0  | eth_btc  | 500        |
| ...       | eth_btc  | 503        |
| settle.63 | eth_btc  | ...        |
| settle.0  | eth_usdt | 600        |
| ...       | eth_usdt | ...        |
| settle.63 | eth_usdt | 590        |



*step2: select a correct message_id*

There are two cases for message queue with different features. 

(1)process as producer, there aren't any relationship between topics.(kafka)

select min one as message_id

(2)process as producer, messages sent to topics are ordered.  message order between topics are sequenced.

select max one as message_id



*step3: matchengine get correct snapshot*

select a snapshot whose message_id <= message_id, ensure all outputs is handled by settle_service 



**How to process the retransmit or old message?**

For settle_service, it would receive message which has been processed before. Because matchengine restart. Use message_id for deduplication, if the message of one market's message id less equal than processed before, it ignore. 

# Scalability

Offer_service and settle_service process user level data, so user table and order table can be splited into many tables by user_id%50.

Memory-matchengine is high performance. One matchengine corresponds one market, so it's easy to add new market.

The match results are splited into multiple topics, this made user-level load balance and topic-level message orderly. Different Topics can be  processed concurrency.







