//! 撮合主线程侧的 HTTP 语义：`HttpOp` → 状态码 + JSON 字符串（不含 Axum 路由，路由见 `server.rs`）。

use crate::market::*;
use crate::offer_metrics::OfferConsumerSnapshot;
use crate::publish::PublishStatusSnapshot;
use crate::task::{HttpOp, HttpResponse, MainTaskQueueSnapshot};
use json::JsonValue;
use rust_decimal::prelude::*;
use skiplist::OrderedSkipList;
use std::rc::Rc;

fn json_err(msg: &str) -> String {
    let mut o = JsonValue::new_object();
    o["error"] = msg.into();
    o.dump()
}

fn unknown_market() -> HttpResponse {
    HttpResponse {
        status: 404,
        body: json_err("unknown market"),
    }
}

/// Matcher thread only. HTTP worker threads enqueue `HttpRequestTask`s; this
/// module receives main-thread `Market` access plus immutable runtime snapshots.
pub fn handle_http_request(
    m: &Market,
    publish_snapshot: &PublishStatusSnapshot,
    main_task_queue: &MainTaskQueueSnapshot,
    offer_consumer: &OfferConsumerSnapshot,
    op: HttpOp,
) -> HttpResponse {
    match op {
        HttpOp::MarketSummary { market } => {
            if !m.name.eq(&market) {
                return unknown_market();
            }
            HttpResponse {
                status: 200,
                body: json_market_summary(m),
            }
        }
        HttpOp::MarketStatus { market } => {
            if !m.name.eq(&market) {
                return unknown_market();
            }
            HttpResponse {
                status: 200,
                body: json_market_status(m, publish_snapshot, main_task_queue, offer_consumer),
            }
        }
        HttpOp::OrderDetail { market, order_id } => {
            if !m.name.eq(&market) {
                return unknown_market();
            }
            match m.get_order(&order_id) {
                Some(o) => HttpResponse {
                    status: 200,
                    body: o.to_json(m).dump(),
                },
                None => HttpResponse {
                    status: 404,
                    body: json_err("order not found"),
                },
            }
        }
        HttpOp::OrderBook {
            market,
            side,
            offset,
            limit,
        } => {
            if !m.name.eq(&market) {
                return unknown_market();
            }
            if side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID {
                return HttpResponse {
                    status: 400,
                    body: json_err("invalid side (use 1=ask, 2=bid)"),
                };
            }
            HttpResponse {
                status: 200,
                body: json_order_book(m, side, offset, limit),
            }
        }
        HttpOp::UserOrdersPending {
            market,
            user_id,
            offset,
            limit,
        } => {
            if !m.name.eq(&market) {
                return unknown_market();
            }
            HttpResponse {
                status: 200,
                body: json_user_orders_pending(m, &user_id, offset, limit),
            }
        }
    }
}

fn json_market_summary(m: &Market) -> String {
    let mut object = JsonValue::new_object();
    let mut ask_amount = Decimal::ZERO;
    let mut ask_money_amount = Decimal::ZERO;
    let mut bid_amount = Decimal::ZERO;
    let mut bid_money_amount = Decimal::ZERO;
    for order in &m.asks {
        ask_amount += order.left.get();
        ask_money_amount += order.price * order.left.get();
    }
    for order in &m.bids {
        bid_amount += order.left.get();
        bid_money_amount += order.price * order.left.get();
    }
    object["name"] = m.name.clone().into();
    object["ask_count"] = (m.asks.len() as u32).into();
    object["ask_stock_amount"] = ask_amount.to_string().into();
    object["ask_money_amount"] = ask_money_amount.to_string().into();
    object["bid_count"] = (m.bids.len() as u32).into();
    object["bid_stock_amount"] = bid_amount.to_string().into();
    object["bid_money_amount"] = bid_money_amount.to_string().into();
    object.dump()
}

fn ids_to_json(ids: &[u64]) -> JsonValue {
    let mut array = JsonValue::new_array();
    for id in ids {
        let _ = array.push(*id);
    }
    array
}

fn usize_ids_to_json(ids: &[usize]) -> JsonValue {
    let mut array = JsonValue::new_array();
    for id in ids {
        let _ = array.push(*id as u64);
    }
    array
}

fn max_index_value(values: &[usize]) -> (usize, usize) {
    values
        .iter()
        .copied()
        .enumerate()
        .max_by_key(|(_, value)| *value)
        .unwrap_or((0, 0))
}

fn max_index_value_u64(values: &[u64]) -> (usize, u64) {
    values
        .iter()
        .copied()
        .enumerate()
        .max_by_key(|(_, value)| *value)
        .unwrap_or((0, 0))
}

fn max_group_json(group_id: usize, value: usize, value_name: &str) -> JsonValue {
    let mut object = JsonValue::new_object();
    object["group_id"] = (group_id as u64).into();
    object[value_name] = (value as u64).into();
    object
}

fn max_group_json_u64(group_id: usize, value: u64, value_name: &str) -> JsonValue {
    let mut object = JsonValue::new_object();
    object["group_id"] = (group_id as u64).into();
    object[value_name] = value.into();
    object
}

fn publish_backlog_json(snapshot: &PublishStatusSnapshot) -> JsonValue {
    let mut object = JsonValue::new_object();
    let (max_pending_group_id, max_pending) = max_index_value(&snapshot.settle_group_pending);
    let settle_worker_count = snapshot.settle_worker_outstanding.len();
    let settle_groups_per_worker = if settle_worker_count == 0 {
        0
    } else {
        snapshot.settle_group_pending.len() / settle_worker_count
    };

    object["quote_pending"] = (snapshot.quote_pending as u64).into();
    object["quote_channel_capacity"] = (snapshot.quote_channel_capacity as u64).into();
    object["quote_channel_full_count"] = (snapshot.quote_channel_full_count as u64).into();
    object["quote_channel_blocked_nanos"] = snapshot.quote_channel_blocked_nanos.into();
    object["quote_channel_max_blocked_nanos"] = snapshot.quote_channel_max_blocked_nanos.into();
    object["settle_pending"] = (snapshot.settle_pending as u64).into();
    object["settle_channel_capacity_per_worker"] =
        (snapshot.settle_channel_capacity_per_worker as u64).into();
    object["settle_worker_channel_full_counts"] =
        usize_ids_to_json(&snapshot.settle_worker_channel_full_count);
    object["settle_worker_channel_blocked_nanos"] =
        ids_to_json(&snapshot.settle_worker_channel_blocked_nanos);
    object["settle_worker_channel_max_blocked_nanos"] =
        ids_to_json(&snapshot.settle_worker_channel_max_blocked_nanos);
    object["settle_group_pending"] = usize_ids_to_json(&snapshot.settle_group_pending);
    object["settle_duplicate_dropped_count"] =
        usize_ids_to_json(&snapshot.settle_duplicate_dropped_count);
    object["max_settle_group_pending"] =
        max_group_json(max_pending_group_id, max_pending, "pending");
    object["quote_in_flight"] = (snapshot.quote_in_flight as u64).into();
    object["quote_max_outstanding"] = (snapshot.quote_max_outstanding as u64).into();
    object["quote_queue_full_count"] = (snapshot.quote_queue_full_count as u64).into();
    object["settle_worker_outstanding"] = usize_ids_to_json(&snapshot.settle_worker_outstanding);
    object["settle_max_outstanding_per_group"] =
        (snapshot.settle_max_outstanding_per_group as u64).into();
    object["settle_worker_max_outstanding"] =
        (snapshot.settle_worker_max_outstanding as u64).into();
    object["settle_worker_queue_full_counts"] =
        usize_ids_to_json(&snapshot.settle_worker_queue_full_count);
    object["settle_worker_count"] = (settle_worker_count as u64).into();
    object["settle_groups_per_worker"] = (settle_groups_per_worker as u64).into();
    object
}

fn main_task_queue_json(snapshot: &MainTaskQueueSnapshot) -> JsonValue {
    let mut object = JsonValue::new_object();
    object["capacity"] = (snapshot.capacity as u64).into();
    object["pending_or_blocked"] = (snapshot.pending_or_blocked as u64).into();
    object["max_pending_or_blocked"] = (snapshot.max_pending_or_blocked as u64).into();
    object["queue_full_count"] = (snapshot.queue_full_count as u64).into();
    object["offer_pending_or_blocked"] = (snapshot.offer_pending_or_blocked as u64).into();
    object["http_pending_or_blocked"] = (snapshot.http_pending_or_blocked as u64).into();
    object["dump_pending_or_blocked"] = (snapshot.dump_pending_or_blocked as u64).into();
    object["quote_progress_pending_or_blocked"] =
        (snapshot.quote_progress_pending_or_blocked as u64).into();
    object["settle_progress_pending_or_blocked"] =
        (snapshot.settle_progress_pending_or_blocked as u64).into();
    object["terminate_pending_or_blocked"] = (snapshot.terminate_pending_or_blocked as u64).into();
    object
}

fn offer_consumer_json(snapshot: &OfferConsumerSnapshot) -> JsonValue {
    let mut object = JsonValue::new_object();
    object["bucket_width_ms"] = snapshot.bucket_width_ms.into();
    object["window_buckets"] = (snapshot.window_buckets as u64).into();
    object["current_generation"] = snapshot.current_generation.into();
    object["start_unix_ms"] = snapshot.start_unix_ms.into();
    object["current_elapsed_ms"] = snapshot.current_elapsed_ms.into();
    object["total_messages"] = snapshot.total_messages.into();
    object["total_payload_bytes"] = snapshot.total_payload_bytes.into();
    object["total_recv_wait_nanos"] = snapshot.total_recv_wait_nanos.into();
    object["total_parse_nanos"] = snapshot.total_parse_nanos.into();
    object["total_send_nanos"] = snapshot.total_send_nanos.into();
    object["total_recv_errors"] = snapshot.total_recv_errors.into();

    let mut buckets = JsonValue::new_array();
    for bucket in &snapshot.buckets {
        let mut item = JsonValue::new_object();
        item["generation"] = bucket.generation.into();
        item["start_ms"] = bucket.start_ms.into();
        item["start_unix_ms"] = bucket.start_unix_ms.into();
        item["messages"] = bucket.messages.into();
        item["payload_bytes"] = bucket.payload_bytes.into();
        item["recv_wait_nanos"] = bucket.recv_wait_nanos.into();
        item["recv_wait_max_nanos"] = bucket.recv_wait_max_nanos.into();
        item["parse_nanos"] = bucket.parse_nanos.into();
        item["parse_max_nanos"] = bucket.parse_max_nanos.into();
        item["send_nanos"] = bucket.send_nanos.into();
        item["send_max_nanos"] = bucket.send_max_nanos.into();
        item["recv_errors"] = bucket.recv_errors.into();
        let _ = buckets.push(item);
    }
    object["buckets"] = buckets;
    object
}

fn runtime_status_json(
    m: &Market,
    publish_snapshot: &PublishStatusSnapshot,
    main_task_queue: &MainTaskQueueSnapshot,
    offer_consumer: &OfferConsumerSnapshot,
) -> JsonValue {
    let mut object = JsonValue::new_object();
    let settle_worker_count = publish_snapshot.settle_worker_outstanding.len();
    let settle_total_channel_capacity =
        publish_snapshot.settle_channel_capacity_per_worker * settle_worker_count;
    let settle_total_worker_outstanding_capacity =
        publish_snapshot.settle_worker_max_outstanding * settle_worker_count;
    let (max_pending_group_id, max_pending) =
        max_index_value(&publish_snapshot.settle_group_pending);

    let settle_group_lags: Vec<u64> = m
        .settle_message_ids
        .iter()
        .zip(m.pushed_settle_message_ids.iter())
        .map(|(produced, pushed)| produced.saturating_sub(*pushed))
        .collect();
    let (max_lag_group_id, max_lag) = max_index_value_u64(&settle_group_lags);

    let mut offer_queue = JsonValue::new_object();
    offer_queue["capacity"] = (main_task_queue.capacity as u64).into();
    offer_queue["pending_or_blocked"] = (main_task_queue.offer_pending_or_blocked as u64).into();
    offer_queue["total_main_queue_pending_or_blocked"] =
        (main_task_queue.pending_or_blocked as u64).into();
    offer_queue["max_total_main_queue_pending_or_blocked"] =
        (main_task_queue.max_pending_or_blocked as u64).into();
    offer_queue["queue_full_count"] = (main_task_queue.queue_full_count as u64).into();
    object["offer_queue"] = offer_queue;
    object["offer_consumer"] = offer_consumer_json(offer_consumer);

    let mut quote_message_queue = JsonValue::new_object();
    quote_message_queue["capacity"] = (publish_snapshot.quote_channel_capacity as u64).into();
    quote_message_queue["pending"] = (publish_snapshot.quote_pending as u64).into();
    quote_message_queue["channel_full_count"] =
        (publish_snapshot.quote_channel_full_count as u64).into();
    quote_message_queue["channel_blocked_nanos"] =
        publish_snapshot.quote_channel_blocked_nanos.into();
    quote_message_queue["channel_max_blocked_nanos"] =
        publish_snapshot.quote_channel_max_blocked_nanos.into();
    object["quote_message_queue"] = quote_message_queue;

    let mut settle_message_queue = JsonValue::new_object();
    settle_message_queue["capacity_per_worker"] =
        (publish_snapshot.settle_channel_capacity_per_worker as u64).into();
    settle_message_queue["total_capacity"] = (settle_total_channel_capacity as u64).into();
    settle_message_queue["pending"] = (publish_snapshot.settle_pending as u64).into();
    settle_message_queue["worker_channel_full_counts"] =
        usize_ids_to_json(&publish_snapshot.settle_worker_channel_full_count);
    settle_message_queue["worker_channel_blocked_nanos"] =
        ids_to_json(&publish_snapshot.settle_worker_channel_blocked_nanos);
    settle_message_queue["worker_channel_max_blocked_nanos"] =
        ids_to_json(&publish_snapshot.settle_worker_channel_max_blocked_nanos);
    settle_message_queue["group_pending"] =
        usize_ids_to_json(&publish_snapshot.settle_group_pending);
    settle_message_queue["max_group_pending"] =
        max_group_json(max_pending_group_id, max_pending, "pending");
    object["settle_message_queue"] = settle_message_queue;

    let mut quote_kafka = JsonValue::new_object();
    quote_kafka["in_flight"] = (publish_snapshot.quote_in_flight as u64).into();
    quote_kafka["max_outstanding"] = (publish_snapshot.quote_max_outstanding as u64).into();
    quote_kafka["queue_full_count"] = (publish_snapshot.quote_queue_full_count as u64).into();
    quote_kafka["publish_lag"] = m.deals_id.saturating_sub(m.pushed_quote_deals_id).into();
    object["quote_kafka"] = quote_kafka;

    let mut settle_kafka = JsonValue::new_object();
    settle_kafka["worker_outstanding"] =
        usize_ids_to_json(&publish_snapshot.settle_worker_outstanding);
    settle_kafka["worker_max_outstanding"] =
        (publish_snapshot.settle_worker_max_outstanding as u64).into();
    settle_kafka["total_worker_outstanding_capacity"] =
        (settle_total_worker_outstanding_capacity as u64).into();
    settle_kafka["max_outstanding_per_group"] =
        (publish_snapshot.settle_max_outstanding_per_group as u64).into();
    settle_kafka["worker_queue_full_counts"] =
        usize_ids_to_json(&publish_snapshot.settle_worker_queue_full_count);
    settle_kafka["group_lags"] = ids_to_json(&settle_group_lags);
    settle_kafka["max_group_lag"] = max_group_json_u64(max_lag_group_id, max_lag, "lag");
    object["settle_kafka"] = settle_kafka;

    object
}

fn publish_lag_json(m: &Market) -> JsonValue {
    let mut object = JsonValue::new_object();
    let settle_group_lags: Vec<u64> = m
        .settle_message_ids
        .iter()
        .zip(m.pushed_settle_message_ids.iter())
        .map(|(produced, pushed)| produced.saturating_sub(*pushed))
        .collect();
    let (max_lag_group_id, max_lag) = max_index_value_u64(&settle_group_lags);

    object["quote_deals"] = m.deals_id.saturating_sub(m.pushed_quote_deals_id).into();
    object["settle_group_lags"] = ids_to_json(&settle_group_lags);
    object["max_settle_group_lag"] = max_group_json_u64(max_lag_group_id, max_lag, "lag");
    object
}

fn json_market_status(
    m: &Market,
    publish_snapshot: &PublishStatusSnapshot,
    main_task_queue: &MainTaskQueueSnapshot,
    offer_consumer: &OfferConsumerSnapshot,
) -> String {
    let mut object = JsonValue::new_object();
    object["oper_id"] = m.oper_id.into();
    object["order_id"] = m.order_id.into();
    object["deals_id"] = m.deals_id.into();
    object["message_id"] = m.message_id.into();
    object["settle_message_ids"] = ids_to_json(&m.settle_message_ids);
    object["input_offset"] = m.input_offset.into();
    object["input_sequence_id"] = m.input_sequence_id.into();
    object["pushed_quote_deals_id"] = m.pushed_quote_deals_id.into();
    object["pushed_settle_message_ids"] = ids_to_json(&m.pushed_settle_message_ids);
    object["runtime_status"] =
        runtime_status_json(m, publish_snapshot, main_task_queue, offer_consumer);
    object["main_task_queue"] = main_task_queue_json(main_task_queue);
    object["offer_consumer"] = offer_consumer_json(offer_consumer);
    object["publish_backlog"] = publish_backlog_json(publish_snapshot);
    object["publish_lag"] = publish_lag_json(m);
    object.dump()
}

fn get_order_by_limit(
    m: &Market,
    offset: u32,
    limit: u32,
    orders: &OrderedSkipList<Rc<Order>>,
) -> JsonValue {
    let mut array = JsonValue::new_array();
    if offset < (orders.len() as u32) {
        let mut count = 0;
        while count < limit {
            let index = offset + count;
            let order = orders.get(index as usize);
            if order.is_none() {
                break;
            }
            count += 1;
            let _ = array.push(order.unwrap().to_json(m));
        }
    }
    array
}

fn json_order_book(m: &Market, side: u32, offset: u32, limit: u32) -> String {
    let mut object = JsonValue::new_object();
    object["limit"] = limit.into();
    object["offset"] = offset.into();
    if side == MARKET_ORDER_SIDE_ASK {
        object["total"] = m.asks.len().into();
        object["stock_amount"] = m.stock_amount.to_string().into();
        object["orders"] = get_order_by_limit(m, offset, limit, &m.asks);
    } else {
        object["total"] = m.bids.len().into();
        object["money_amount"] = m.money_amount.to_string().into();
        object["orders"] = get_order_by_limit(m, offset, limit, &m.bids);
    }
    object.dump()
}

fn json_user_orders_pending(m: &Market, user_id: &u32, offset: u32, limit: u32) -> String {
    let mut object = JsonValue::new_object();
    object["limit"] = limit.into();
    object["offset"] = offset.into();
    if let Some(orders) = m.get_user_order_list(user_id) {
        object["total"] = orders.len().into();
        let mut array = JsonValue::new_array();
        let mut count: u32 = 0;
        while count < limit {
            let index = offset + count;
            let order = orders.get(index as usize);
            if order.is_none() {
                break;
            }
            count += 1;
            let _ = array.push(order.unwrap().to_json(m));
        }
        object["orders"] = array;
    } else {
        object["total"] = 0u32.into();
        object["orders"] = JsonValue::new_array();
    }
    object.dump()
}
