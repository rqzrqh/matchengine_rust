use crate::market::*;
use crate::publish::*;
use crate::engine::*;
use json::JsonValue;
use rust_decimal::prelude::*;
use crate::error::*;

pub fn handle_mq_message(publisher: &Publish, m: &mut Market, offset: i64, data: &String) {
    let parsed = json::parse(data).unwrap();
    info!("{}", parsed);

    m.oper_id += 1;
    m.input_offset = offset;

    if !parsed.is_object() {
        error!("mq msg not object {}", parsed);
        return;
    }

    if !parsed.has_key("method") || !parsed["method"].is_string() {
        error!("method parse failed {}", parsed);
        return;
    }

    if !parsed.has_key("id") || !parsed["id"].is_number() {
        error!("id parse failed {}", parsed);
        return;
    }

    if !parsed.has_key("params") || !parsed["params"].is_object() {
        error!("params parse failed {}", parsed);
        return;
    }

    let method = parsed["method"].as_str().unwrap();
    let extern_id = parsed["id"].as_u64().unwrap();
    let params = &parsed["params"];

    match method {
        "order.put_limit" => on_order_put_limit(&publisher, m, extern_id, &params),
        "order.put_market" => on_order_put_market(&publisher, m, extern_id, &params),
        "order.cancel" => on_order_cancel(&publisher, m, extern_id, &params),
        _ => {
            error!("method error");
        }
    };
}

fn on_order_put_limit(publisher: &Publish, m: &mut Market, extern_id: u64, params: &JsonValue) {

    if !params.has_key("user_id") || !params["user_id"].is_number() {
        error!("user_id parse failed {}", params);
        return;
    }

    if !params.has_key("side") || !params["side"].is_number() {
        error!("side parse failed {}", params);
        return;
    }

    if !params.has_key("amount") || !params["amount"].is_string() {
        error!("amount parse failed {}", params);
        return;
    }

    if !params.has_key("price") || !params["price"].is_string() {
        error!("price parse failed {}", params);
        return;
    }

    if !params.has_key("taker_fee_rate") || !params["taker_fee_rate"].is_string() {
        error!("taker_fee_rate parse failed {}", params);
        return;
    }

    if !params.has_key("maker_fee_rate") || !params["maker_fee_rate"].is_string() {
        error!("maker_fee_rate parse failed {}", params);
        return;
    }

    let user_id = params["user_id"].as_u32().unwrap();
    let side = params["side"].as_u32().unwrap();
    let mut amount = Decimal::from_str(params["amount"].as_str().unwrap()).unwrap();
    amount.rescale(m.stock_prec);
    let mut price = Decimal::from_str(params["price"].as_str().unwrap()).unwrap();
    price.rescale(m.money_prec-m.stock_prec);
    let mut taker_fee_rate = Decimal::from_str(params["taker_fee_rate"].as_str().unwrap()).unwrap();
    taker_fee_rate.rescale(m.fee_rate_prec);
    let mut maker_fee_rate = Decimal::from_str(params["maker_fee_rate"].as_str().unwrap()).unwrap();
    maker_fee_rate.rescale(m.fee_rate_prec);

    market_put_limit_order(publisher, m, extern_id, user_id, side, amount, price, taker_fee_rate, maker_fee_rate).unwrap_or_else(|e| {
        publisher.publish_error(m, extern_id, user_id, params, e);
    });
}

fn on_order_put_market(publisher: &Publish, m: &mut Market, extern_id: u64, params: &JsonValue) {

    if !params.has_key("user_id") || !params["user_id"].is_number() {
        error!("user_id parse failed {}", params);
        return;
    }

    if !params.has_key("side") || !params["side"].is_number() {
        error!("side parse failed {}", params);
        return;
    }

    if !params.has_key("amount") || !params["amount"].is_string() {
        error!("amount parse failed {}", params);
        return;
    }

    if !params.has_key("taker_fee_rate") || !params["taker_fee_rate"].is_string() {
        error!("taker_fee_rate parse failed {}", params);
        return;
    }

    let user_id = params["user_id"].as_u32().unwrap();
    let side = params["side"].as_u32().unwrap();
    let mut amount = Decimal::from_str(params["amount"].as_str().unwrap()).unwrap();

    if side == MARKET_ORDER_SIDE_ASK {
        amount.rescale(m.stock_prec);
    } else {
        amount.rescale(m.money_prec);
    }

    let mut taker_fee_rate = Decimal::from_str(params["taker_fee_rate"].as_str().unwrap()).unwrap();
    taker_fee_rate.rescale(m.fee_rate_prec);

    market_put_market_order(publisher, m, extern_id, user_id, side, amount, taker_fee_rate).unwrap_or_else(|e| {
        publisher.publish_error(m, extern_id, user_id, params, e);
    });
}

fn on_order_cancel(publisher: &Publish, m: &mut Market, extern_id: u64, params: &JsonValue) {

    if !params.has_key("user_id") || !params["user_id"].is_number() {
        error!("user_id parse failed {}", params);
        return;
    }

    if !params.has_key("order_id") || !params["order_id"].is_number() {
        error!("order_id parse failed {}", params);
        return;
    }

    let user_id = params["user_id"].as_u32().unwrap();
    let order_id = params["order_id"].as_u64().unwrap();

    match m.get_order(&order_id) {
        Some(order_ref) => {
            let order = order_ref.clone();
            publisher.publish_cancel_order(m, extern_id, &order);
            if order.side == MARKET_ORDER_SIDE_ASK {
                m.stock_amount -= order.left.get();
            } else {
                m.money_amount -= order.price*order.left.get();
            }
            m.order_finish(&order.clone());
        },
        None => {
            publisher.publish_error(m, extern_id, user_id, params, MATCH_ERROR_ORDER_NOT_FOUND);
        }
    }
}

