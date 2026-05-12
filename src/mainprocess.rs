use crate::decimal_util::parse_decimal_with_max_scale;
use crate::engine::*;
use crate::error::*;
use crate::market::*;
use crate::payload_encoding::decode_msgpack;
use crate::publish::*;
use crate::task::*;

/// RPC envelope validation after MessagePack unpacking on the Kafka consumer thread.
pub fn parse_mq_payload(data: &[u8]) -> Result<MqPayload, String> {
    let parsed: MqPayload = decode_msgpack(data)?;
    debug!("{:?}", parsed);

    Ok(parsed)
}

pub fn handle_mq_message(publisher: &Publish, m: &mut Market, mq: KafkaMqTask) {
    let KafkaMqTask { offset, payload } = mq;
    m.oper_id += 1;
    m.input_offset = offset;

    let parsed = match payload {
        Ok(p) => p,
        Err(e) => {
            error!("mq payload error at offset {}: {}", offset, e);
            return;
        }
    };

    let msg_seq = parsed.input_sequence_id;
    let Some(expected_seq) = m.input_sequence_id.checked_add(1) else {
        error!("market input_sequence_id overflow {}", m.input_sequence_id);
        return;
    };

    if msg_seq <= m.input_sequence_id {
        warn!(
            "stale input_sequence_id skipped: message {} current {}",
            msg_seq, m.input_sequence_id
        );
        return;
    }
    if msg_seq != expected_seq {
        warn!(
            "input_sequence_id mismatch: message {} expected next {}",
            msg_seq, expected_seq
        );
        return;
    }

    let method = parsed.method;
    let extern_id = parsed.id;
    let params = parsed.params;

    m.input_sequence_id = msg_seq;

    match (method, params) {
        (MQ_METHOD_ORDER_PUT_LIMIT, MqParams::PutLimit(params)) => {
            on_order_put_limit(&publisher, m, extern_id, params)
        }
        (MQ_METHOD_ORDER_PUT_MARKET, MqParams::PutMarket(params)) => {
            on_order_put_market(&publisher, m, extern_id, params)
        }
        (MQ_METHOD_ORDER_CANCEL, MqParams::Cancel(params)) => {
            on_order_cancel(&publisher, m, extern_id, params)
        }
        (method, params) => {
            error!(
                "method/params mismatch method={} params={:?}",
                method, params
            );
        }
    };
}

fn on_order_put_limit(publisher: &Publish, m: &mut Market, extern_id: u64, params: PutLimitParams) {
    let error_params = MqParams::PutLimit(params.clone());

    let amount = match parse_decimal_with_max_scale(&params.amount, m.stock_prec, "amount") {
        Ok(value) => value,
        Err(e) => {
            error!("{}", e);
            return;
        }
    };

    let price = match parse_decimal_with_max_scale(&params.price, m.price_prec(), "price") {
        Ok(value) => value,
        Err(e) => {
            error!("{}", e);
            return;
        }
    };

    let taker_fee_rate = match parse_decimal_with_max_scale(
        &params.taker_fee_rate,
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
        &params.maker_fee_rate,
        m.fee_rate_prec,
        "maker_fee_rate",
    ) {
        Ok(value) => value,
        Err(e) => {
            error!("{}", e);
            return;
        }
    };

    market_put_limit_order(
        publisher,
        m,
        extern_id,
        params.user_id,
        params.side,
        amount,
        price,
        taker_fee_rate,
        maker_fee_rate,
    )
    .unwrap_or_else(|e| {
        publisher.publish_error(m, extern_id, params.user_id, &error_params, e);
    });
}

fn on_order_put_market(
    publisher: &Publish,
    m: &mut Market,
    extern_id: u64,
    params: PutMarketParams,
) {
    let error_params = MqParams::PutMarket(params.clone());
    let amount_prec = if params.side == MARKET_ORDER_SIDE_ASK {
        m.stock_prec
    } else {
        m.money_prec
    };
    let amount = match parse_decimal_with_max_scale(&params.amount, amount_prec, "amount") {
        Ok(value) => value,
        Err(e) => {
            error!("{}", e);
            return;
        }
    };

    let taker_fee_rate = match parse_decimal_with_max_scale(
        &params.taker_fee_rate,
        m.fee_rate_prec,
        "taker_fee_rate",
    ) {
        Ok(value) => value,
        Err(e) => {
            error!("{}", e);
            return;
        }
    };

    market_put_market_order(
        publisher,
        m,
        extern_id,
        params.user_id,
        params.side,
        amount,
        taker_fee_rate,
    )
    .unwrap_or_else(|e| {
        publisher.publish_error(m, extern_id, params.user_id, &error_params, e);
    });
}

fn on_order_cancel(publisher: &Publish, m: &mut Market, extern_id: u64, params: CancelParams) {
    let error_params = MqParams::Cancel(params.clone());

    match m.get_order(&params.order_id) {
        Some(order_ref) => {
            let order = order_ref.clone();

            m.message_id += 1;
            publisher.publish_cancel_order(m, extern_id, &order);
            if order.side == MARKET_ORDER_SIDE_ASK {
                m.stock_amount -= order.left.get();
            } else {
                m.money_amount -= order.price * order.left.get();
            }
            m.order_finish(&order.clone());
        }
        None => {
            publisher.publish_error(
                m,
                extern_id,
                params.user_id,
                &error_params,
                MATCH_ERROR_ORDER_NOT_FOUND,
            );
        }
    }
}

pub fn update_quote_progress(m: &mut Market, pushed_quote_deals_id: u64) {
    m.pushed_quote_deals_id = pushed_quote_deals_id;

    debug!("pushed_quote_deals_id={}", pushed_quote_deals_id);
}

pub fn update_settle_progress_batch(
    m: &mut Market,
    progresses: &[crate::task::SettlePublishProgressTask],
) {
    for progress in progresses {
        m.pushed_settle_message_ids[progress.group_id] = progress.pushed_settle_message_id;
    }
    debug!(
        "pushed_settle_message_ids={:?}",
        m.pushed_settle_message_ids
    );
}
