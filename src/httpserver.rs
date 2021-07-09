use crate::market::*;
use tokio::sync::oneshot;
use json::*;
use rust_decimal::prelude::*;
use std::rc::Rc;
use skiplist::OrderedSkipList;

pub fn handle_http_request(m: &Market, content: &String, rsp: oneshot::Sender<String>) {

    let parsed = json::parse(content).unwrap();

    if !parsed.is_object() {
        warn!("request msg not object {}", parsed);
        return;
    }

    let method = parsed["method"].as_str().unwrap();
    let params = &parsed["params"];
    let res: String;

    match method {
        "market.summary" => {
            res = handle_market_summary(m, params);
        },
        "market.status" => {
            res = handle_market_status(m, params);
        },
        "market.order_detail" => {
            res = handle_order_detail(m, params);
        },
        "market.order_book" => {
            res = handle_order_book(m, params);
        },
        "market.user_order_pending" => {
            res = handle_user_order_pending(m, params);
        },
        _ => {
            res = String::from("unsupported method");
        },
    }

    let _ = rsp.send(res);
}

fn handle_market_summary(m: &Market, params: &JsonValue) -> String {
    let market_name = params["market"].as_str().unwrap();

    if !m.name.eq(market_name) {
        return String::from("market not equal")
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
        return String::from("market not equal")
    }

    let mut object = JsonValue::new_object();
    object["oper_id"] = m.oper_id.into();
    object["order_id"] = m.order_id.into();
    object["deals_id"] = m.deals_id.into();
    object["message_id"] = m.message_id.into();
    object["input_offset"] = m.input_offset.into();

    object.dump()
}

fn handle_order_detail(m: &Market, params: &JsonValue) -> String {
    let market_name = params["market"].as_str().unwrap();
    let order_id = params["order_id"].as_u64().unwrap();
    if !m.name.eq(market_name) {
        return String::from("market not equal")
    }

    let order = m.get_order(&order_id);
    match order {
        Some(o) => {
            o.to_json().dump()
        },
        None => {
            String::from("not exist")
        },
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
        return String::from("market not equal")
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
        return String::from("market not equal")
    }

    let mut object = JsonValue::new_object();
    object["limit"] = limit.into();
    object["offset"] = offset.into();

    let _ = m.get_user_order_list(&user_id).map(|orders| {
        object["total"] = orders.len().into();

        let mut array = JsonValue::new_array();
        let mut count:u32 = 0;

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