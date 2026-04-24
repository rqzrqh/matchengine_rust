use crate::market::*;
use json::JsonValue;
use rust_decimal::prelude::*;
use skiplist::OrderedSkipList;
use std::rc::Rc;

fn json_err(msg: &str) -> String {
    let mut o = JsonValue::new_object();
    o["error"] = msg.into();
    o.dump()
}

pub fn handle_http_request(m: &Market, content: &String) -> String {
    let parsed = match json::parse(content) {
        Ok(v) => v,
        Err(e) => {
            warn!("json parse failed {}", e);
            return json_err("invalid json");
        }
    };

    if !parsed.is_object() {
        warn!("request msg not object {}", parsed);
        return json_err("not an object");
    }

    let method = match parsed["method"].as_str() {
        Some(s) => s,
        None => return json_err("method missing or not string"),
    };
    let params = &parsed["params"];

    match method {
        "market.summary" => handle_market_summary(m, params),
        "market.status" => handle_market_status(m, params),
        "market.order_detail" => handle_order_detail(m, params),
        "market.order_book" => handle_order_book(m, params),
        "market.user_order_pending" => handle_user_order_pending(m, params),
        _ => json_err("unsupported method"),
    }
}

fn handle_market_summary(m: &Market, params: &JsonValue) -> String {
    let market_name = params["market"].as_str().unwrap();

    if !m.name.eq(market_name) {
        return String::from("market not equal");
    }

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

fn handle_market_status(m: &Market, params: &JsonValue) -> String {
    let market_name = params["market"].as_str().unwrap();
    if !m.name.eq(market_name) {
        return String::from("market not equal");
    }

    let mut object = JsonValue::new_object();
    object["oper_id"] = m.oper_id.into();
    object["order_id"] = m.order_id.into();
    object["deals_id"] = m.deals_id.into();
    object["message_id"] = m.message_id.into();
    object["input_offset"] = m.input_offset.into();
    object["input_sequence_id"] = m.input_sequence_id.into();

    object.dump()
}

fn handle_order_detail(m: &Market, params: &JsonValue) -> String {
    let market_name = params["market"].as_str().unwrap();
    let order_id = params["order_id"].as_u64().unwrap();
    if !m.name.eq(market_name) {
        return String::from("market not equal");
    }

    let order = m.get_order(&order_id);
    match order {
        Some(o) => o.to_json().dump(),
        None => String::from("not exist"),
    }
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

fn handle_order_book(m: &Market, params: &JsonValue) -> String {
    let market_name = params["market"].as_str().unwrap();
    let side = params["side"].as_u32().unwrap();
    let offset = params["offset"].as_u32().unwrap();
    let limit = params["limit"].as_u32().unwrap();

    if !m.name.eq(market_name) {
        return String::from("market not equal");
    }

    let mut object = JsonValue::new_object();
    object["limit"] = limit.into();
    object["offset"] = offset.into();

    if side == MARKET_ORDER_SIDE_ASK {
        object["total"] = m.asks.len().into();
        object["stock_amount"] = m.stock_amount.to_string().into();
        object["orders"] = get_order_by_limit(offset, limit, &m.asks);
    } else if side == MARKET_ORDER_SIDE_BID {
        object["total"] = m.bids.len().into();
        object["money_amount"] = m.money_amount.to_string().into();
        object["orders"] = get_order_by_limit(offset, limit, &m.bids);
    }

    object.dump()
}

fn handle_user_order_pending(m: &Market, params: &JsonValue) -> String {
    let market_name = params["market"].as_str().unwrap();
    let user_id = params["user_id"].as_u32().unwrap();
    let offset = params["offset"].as_u32().unwrap();
    let limit = params["limit"].as_u32().unwrap();

    if !m.name.eq(market_name) {
        return String::from("market not equal");
    }

    let mut object = JsonValue::new_object();
    object["limit"] = limit.into();
    object["offset"] = offset.into();

    let _ = m.get_user_order_list(&user_id).map(|orders| {
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
    });

    object.dump()
}
