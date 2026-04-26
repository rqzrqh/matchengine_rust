use crate::market::*;
use crate::task::{RestOp, RestResponse};
use json::JsonValue;
use rust_decimal::prelude::*;
use skiplist::OrderedSkipList;
use std::rc::Rc;

fn json_err(msg: &str) -> String {
    let mut o = JsonValue::new_object();
    o["error"] = msg.into();
    o.dump()
}

fn unknown_market() -> RestResponse {
    RestResponse {
        status: 404,
        body: json_err("unknown market"),
    }
}

/// Matcher thread only.
pub fn handle_rest_request(m: &Market, op: RestOp) -> RestResponse {
    match op {
        RestOp::MarketSummary { market } => {
            if !m.name.eq(&market) {
                return unknown_market();
            }
            RestResponse {
                status: 200,
                body: json_market_summary(m),
            }
        }
        RestOp::MarketStatus { market } => {
            if !m.name.eq(&market) {
                return unknown_market();
            }
            RestResponse {
                status: 200,
                body: json_market_status(m),
            }
        }
        RestOp::OrderDetail {
            market,
            order_id,
        } => {
            if !m.name.eq(&market) {
                return unknown_market();
            }
            match m.get_order(&order_id) {
                Some(o) => RestResponse {
                    status: 200,
                    body: o.to_json().dump(),
                },
                None => RestResponse {
                    status: 404,
                    body: json_err("order not found"),
                },
            }
        }
        RestOp::OrderBook {
            market,
            side,
            offset,
            limit,
        } => {
            if !m.name.eq(&market) {
                return unknown_market();
            }
            if side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID {
                return RestResponse {
                    status: 400,
                    body: json_err("invalid side (use 1=ask, 2=bid)"),
                };
            }
            RestResponse {
                status: 200,
                body: json_order_book(m, side, offset, limit),
            }
        }
        RestOp::UserOrdersPending {
            market,
            user_id,
            offset,
            limit,
        } => {
            if !m.name.eq(&market) {
                return unknown_market();
            }
            RestResponse {
                status: 200,
                body: json_user_orders_pending(m, &user_id, offset, limit),
            }
        }
    }
}

fn json_market_summary(m: &Market) -> String {
    let mut object = JsonValue::new_object();
    let mut ask_amount = Decimal::ZERO;
    let mut bid_amount = Decimal::ZERO;
    for order in &m.asks {
        ask_amount += order.left.get();
    }
    for order in &m.bids {
        bid_amount += order.left.get();
    }
    object["name"] = m.name.clone().into();
    object["ask_count"] = (m.asks.len() as u32).into();
    object["ask_amount"] = ask_amount.to_string().into();
    object["bid_count"] = (m.bids.len() as u32).into();
    object["bid_amount"] = bid_amount.to_string().into();
    object.dump()
}

fn json_market_status(m: &Market) -> String {
    let mut object = JsonValue::new_object();
    object["oper_id"] = m.oper_id.into();
    object["order_id"] = m.order_id.into();
    object["deals_id"] = m.deals_id.into();
    object["message_id"] = m.message_id.into();
    object["input_offset"] = m.input_offset.into();
    object["input_sequence_id"] = m.input_sequence_id.into();
    object.dump()
}

fn get_order_by_limit(offset: u32, limit: u32, orders: &OrderedSkipList<Rc<Order>>) -> JsonValue {
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
            let _ = array.push(order.unwrap().to_json());
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
        object["orders"] = get_order_by_limit(offset, limit, &m.asks);
    } else {
        object["total"] = m.bids.len().into();
        object["money_amount"] = m.money_amount.to_string().into();
        object["orders"] = get_order_by_limit(offset, limit, &m.bids);
    }
    object.dump()
}

fn json_user_orders_pending(
    m: &Market,
    user_id: &u32,
    offset: u32,
    limit: u32,
) -> String {
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
            let _ = array.push(order.unwrap().to_json());
        }
        object["orders"] = array;
    } else {
        object["total"] = 0u32.into();
        object["orders"] = JsonValue::new_array();
    }
    object.dump()
}
