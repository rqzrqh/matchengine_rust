use crate::market::*;
use crate::publish::*;
use crate::engine::*;
use crate::decimal_util::parse_decimal_with_max_scale;
use json::JsonValue;
use crate::error::*;

pub fn handle_mq_message(publisher: &Publish, m: &mut Market, offset: i64, data: &String) {
    let parsed = json::parse(data).unwrap();
    info!("{}", parsed);

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

    if !parsed.has_key("input_sequence_id") || !parsed["input_sequence_id"].is_number() {
        error!("input_sequence_id parse failed {}", parsed);
        return;
    }

    let msg_seq = parsed["input_sequence_id"].as_u64().unwrap();
    let Some(expected_seq) = m.input_sequence_id.checked_add(1) else {
        error!("market input_sequence_id overflow {}", m.input_sequence_id);
        return;
    };
    if msg_seq != expected_seq {
        error!(
            "input_sequence_id mismatch: message {} expected next {}",
            msg_seq, expected_seq
        );
        return;
    }

    let method = parsed["method"].as_str().unwrap();
    let extern_id = parsed["id"].as_u64().unwrap();
    let params = &parsed["params"];

    m.oper_id += 1;
    m.input_offset = offset;
    m.input_sequence_id = msg_seq;

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

    let amount = match parse_decimal_with_max_scale(
        params["amount"].as_str().unwrap(),
        m.stock_prec,
        "amount",
    ) {
        Ok(value) => value,
        Err(e) => {
            error!("{}", e);
            return;
        }
    };

    let price = match parse_decimal_with_max_scale(
        params["price"].as_str().unwrap(),
        m.price_prec(),
        "price",
    ) {
        Ok(value) => value,
        Err(e) => {
            error!("{}", e);
            return;
        }
    };

    let taker_fee_rate = match parse_decimal_with_max_scale(
        params["taker_fee_rate"].as_str().unwrap(),
        m.fee_rate_prec,
        "taker_fee_rate",
    ) {
        Ok(value) => value,
        Err(e) => {
            error!("{}", e);
            return;
        }
    };

    let maker_fee_rate = match parse_decimal_with_max_scale(
        params["maker_fee_rate"].as_str().unwrap(),
        m.fee_rate_prec,
        "maker_fee_rate",
    ) {
        Ok(value) => value,
        Err(e) => {
            error!("{}", e);
            return;
        }
    };

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
    let amount_prec = if side == MARKET_ORDER_SIDE_ASK {
        m.stock_prec
    } else {
        m.money_prec
    };
    let amount = match parse_decimal_with_max_scale(
        params["amount"].as_str().unwrap(),
        amount_prec,
        "amount",
    ) {
        Ok(value) => value,
        Err(e) => {
            error!("{}", e);
            return;
        }
    };

    let taker_fee_rate = match parse_decimal_with_max_scale(
        params["taker_fee_rate"].as_str().unwrap(),
        m.fee_rate_prec,
        "taker_fee_rate",
    ) {
        Ok(value) => value,
        Err(e) => {
            error!("{}", e);
            return;
        }
    };

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

            m.message_id += 1;
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

pub fn update_output_progress(m: &mut Market, quote_deals_id: u64, settle_message_ids: Vec<u64>) {
    m.quote_deals_id = quote_deals_id;
    for i in 0..settle_message_ids.len() {
        m.settle_message_ids[i] = settle_message_ids[i]
    }

    println!("quote_deals_id={}", quote_deals_id);
    println!("settle_message_ids={:?}", m.settle_message_ids);
}