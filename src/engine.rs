
use std::rc::Rc;
use crate::market::*;
use crate::publish::*;
use chrono::prelude::*;
use rust_decimal::prelude::*;
use std::cell::Cell;
use crate::error::*;
/*
fn calc_deal(price: Decimal, amount: Decimal, ask_fee_rate: Decimal, bid_fee_rate: Decimal) -> (Decimal, Decimal, Decimal) {
    let deal = price * amount;
    let ask_fee = deal * ask_fee_rate;
    let bid_fee = amount * bid_fee_rate;
    (deal, ask_fee, bid_fee)
}
*/
fn update_time(taker: &Rc<Order>, maker: &Rc<Order>) -> i64 {

    let now = Utc::now();
    let tm = now.timestamp();

    taker.update_time.set(tm);
    maker.update_time.set(tm);
    tm
}

fn execute_limit_ask_order(publisher: &Publish, m: &mut Market, extern_id: u64, taker: &Rc<Order>) {

    while !m.bids.is_empty() && taker.left.get() > Decimal::ZERO {

        let maker_ref = m.bids.front().unwrap();
        let maker = maker_ref.clone();

        if taker.price > maker.price {
            break;
        }

        let price = maker.price;

        let amount = if taker.left.get() < maker.left.get() {
            taker.left.get()
        } else {
            maker.left.get()
        };

        let deal = price*amount;
        let ask_fee = deal*taker.taker_fee_rate;
        let bid_fee = amount*maker.maker_fee_rate;
        m.deals_id += 1;

        let now = update_time(taker, &maker);

		publisher.publish_quote_deal(m, now, &price, &amount, MARKET_ORDER_SIDE_ASK);
        m.message_id += 1;
        publisher.publish_deal(m, extern_id, now, taker.user_id, maker.user_id, taker.id, MARKET_ROLE_TAKER, &price, &amount, &deal, &ask_fee, &bid_fee);
        m.message_id += 1;
        publisher.publish_deal(m, extern_id, now,  maker.user_id, taker.user_id, maker.id, MARKET_ROLE_MAKER, &price, &amount, &deal, &bid_fee, &ask_fee);

        taker.left.set(taker.left.get() - amount);
        taker.deal_stock.set(taker.deal_stock.get() + amount);
        taker.deal_money.set(taker.deal_money.get() + deal);
        taker.deal_fee.set(taker.deal_fee.get() + ask_fee);

        maker.left.set(maker.left.get() - amount);
        maker.deal_stock.set(maker.deal_stock.get() + amount);
        maker.deal_money.set(maker.deal_money.get() + deal);
        maker.deal_fee.set(maker.deal_fee.get() + bid_fee);

        m.money_amount -= deal;

        if maker.left.get() == Decimal::ZERO {
            m.order_finish(&maker.clone());
        }
    }
}

fn execute_limit_bid_order(publisher: &Publish, m: &mut Market, extern_id: u64, taker: &Rc<Order>) {

    while !m.asks.is_empty() && taker.left.get() > Decimal::ZERO {

        let maker_ref = m.asks.front().unwrap();
        let maker = maker_ref.clone();

        if taker.price < maker.price {
            break;
        }

        let price = maker.price;

        let amount = if taker.left.get() < maker.left.get() {
            taker.left.get()
        } else {
            maker.left.get()
        };

        let deal = price * amount;
        let ask_fee = deal * maker.maker_fee_rate;
        let bid_fee = amount * taker.taker_fee_rate;
        m.deals_id += 1;

        let now = update_time(taker, &maker);

        publisher.publish_quote_deal(m, now, &price, &amount, MARKET_ORDER_SIDE_BID);
        m.message_id += 1;
        publisher.publish_deal(m, extern_id, now, taker.user_id, maker.user_id, taker.id, MARKET_ROLE_TAKER, &price, &amount, &deal, &bid_fee, &ask_fee);
        m.message_id += 1;
        publisher.publish_deal(m, extern_id, now, maker.user_id, taker.user_id, maker.id, MARKET_ROLE_MAKER, &price, &amount, &deal, &ask_fee, &bid_fee);

        taker.left.set(taker.left.get() - amount);
        taker.deal_stock.set(taker.deal_stock.get() + amount);
        taker.deal_money.set(taker.deal_money.get() + deal);
        taker.deal_fee.set(taker.deal_fee.get() + bid_fee);

        maker.left.set(maker.left.get() - amount);
        maker.deal_stock.set(maker.deal_stock.get() + amount);
        maker.deal_money.set(maker.deal_money.get() + deal);
        maker.deal_fee.set(maker.deal_fee.get() + ask_fee);

        m.stock_amount -= amount;

        if maker.left.get() == Decimal::ZERO {
            m.order_finish(&maker.clone());
        }
    }
}

fn execute_market_ask_order(publisher: &Publish, m: &mut Market, extern_id: u64, taker: &Rc<Order>) {

    while !m.bids.is_empty() && taker.left.get() > Decimal::ZERO {

        let maker_ref = m.bids.front().unwrap();
        let maker = maker_ref.clone();

        let price = maker.price;
        let amount = if taker.left.get() < maker.left.get() {
            taker.left.get()
        } else {
            maker.left.get()
        };

        let deal = price * amount;
        let ask_fee = deal * taker.taker_fee_rate;
        let bid_fee = amount * maker.maker_fee_rate;
        m.deals_id += 1;

        let now = update_time(taker, &maker);

        publisher.publish_quote_deal(m, now, &price, &amount, MARKET_ORDER_SIDE_ASK);
        m.message_id += 1;
        publisher.publish_deal(m, extern_id, now, taker.user_id, maker.user_id, taker.id, MARKET_ROLE_TAKER, &price, &amount, &deal, &ask_fee, &bid_fee);
        m.message_id += 1;
        publisher.publish_deal(m, extern_id, now, maker.user_id, taker.user_id, maker.id, MARKET_ROLE_MAKER, &price, &amount, &deal, &bid_fee, &ask_fee);

        taker.left.set(taker.left.get() - amount);
        taker.deal_stock.set(taker.deal_stock.get() + amount);
        taker.deal_money.set(taker.deal_money.get() + deal);
        taker.deal_fee.set(taker.deal_fee.get() + ask_fee);

        maker.left.set(maker.left.get() - amount);
        maker.deal_stock.set(maker.deal_stock.get() + amount);
        maker.deal_money.set(maker.deal_money.get() + deal);
        maker.deal_fee.set(maker.deal_fee.get() + bid_fee);

        m.money_amount -= deal;

        if maker.left.get() == Decimal::ZERO {
            m.order_finish(&maker.clone());
        }
    }
}

fn execute_market_bid_order(publisher: &Publish, m: &mut Market, extern_id: u64, taker: &Rc<Order>) {

    let ten = Decimal::new(10, 0);
    let min = ten.powi(-(m.stock_prec as i64));

    while !m.asks.is_empty() && taker.left.get() > Decimal::ZERO {

        let maker_ref = m.asks.front().unwrap();
        let maker = maker_ref.clone();

        let price = maker.price;

        let mut amount = taker.left.get() / price;
        amount.rescale(m.stock_prec as u32);

/*
        /// let mut round = Decimal::from_str("1.45").unwrap();
        /// round.rescale(1);
        /// assert_eq!(round.to_string(), "1.5");
        /// a = 10^(-1)
        /// 1.5 - a = 1.4
*/

        loop {
            if amount * price > taker.left.get() {
                amount -= min;
            } else {
                break;
            }
        }

        if amount > maker.left.get() {
            amount = maker.left.get();
        }

        if amount == Decimal::ZERO {
            break;
        }

        let deal = price * amount;
        let ask_fee = deal * maker.maker_fee_rate;
        let bid_fee = amount * taker.taker_fee_rate;
        m.deals_id += 1;

        let now = update_time(taker, &maker);

        publisher.publish_quote_deal(m, now, &price, &amount, MARKET_ORDER_SIDE_BID);
        m.message_id += 1;
        publisher.publish_deal(m, extern_id, now, taker.user_id, maker.user_id, taker.id, MARKET_ROLE_TAKER, &price, &amount, &deal, &bid_fee, &ask_fee);
        m.message_id += 1;
        publisher.publish_deal(m, extern_id, now, maker.user_id, taker.user_id, maker.id, MARKET_ROLE_MAKER, &price, &amount, &deal, &ask_fee, &bid_fee);

        taker.left.set(taker.left.get() - deal);
        taker.deal_stock.set(taker.deal_stock.get() - amount);
        taker.deal_money.set(taker.deal_money.get() - deal);
        taker.deal_fee.set(taker.deal_fee.get() + bid_fee);

        maker.left.set(maker.left.get() - amount);
        maker.deal_stock.set(maker.deal_stock.get() + amount);
        maker.deal_money.set(maker.deal_money.get() + deal);
        maker.deal_money.set(maker.deal_money.get() + ask_fee);

        m.stock_amount -= amount;

        if maker.left.get() == Decimal::ZERO {
            m.order_finish(&maker.clone());
        }
    }
}

pub fn market_put_limit_order(publisher: &Publish, m: &mut Market, extern_id: u64, user_id: u32, side: u32, amount: Decimal, 
        price: Decimal, taker_fee_rate: Decimal, maker_fee_rate: Decimal) -> Result<(), u32> {

    if amount < m.min_amount {
        return Err(MATCH_ERROR_AMOUNT_TOO_SMALL);
    }

    if !m.check_user_order_limit(user_id) {
        return Err(MATCH_ERROR_ORDER_COUNT_LIMIT);
    }

    let now = Utc::now();

    m.order_id += 1;
    let order = Rc::new(Order{
        id : m.order_id,
        order_type: MARKET_ORDER_TYPE_LIMIT,
        side : side,
        create_time: now.timestamp(), 
        update_time: Cell::new(now.timestamp()), 
        user_id: user_id,
        price: price,
        amount: Cell::new(amount),
        taker_fee_rate: taker_fee_rate,
        maker_fee_rate: maker_fee_rate,
        left: Cell::new(amount),
        deal_stock: Cell::new(Decimal::ZERO),
        deal_money: Cell::new(Decimal::ZERO),
        deal_fee: Cell::new(Decimal::ZERO),
    });

    m.message_id += 1;
    publisher.publish_put_order(m, extern_id, &order);

    if side == MARKET_ORDER_SIDE_ASK {
        execute_limit_ask_order(publisher, m, extern_id, &order);
    } else {
        execute_limit_bid_order(publisher, m, extern_id, &order);
    }

    if order.left.get() > Decimal::ZERO {
        m.put_order(order);
    }

    Ok(())
}

// amount is stock or money
pub fn market_put_market_order(publisher: &Publish, m: &mut Market, extern_id: u64, user_id: u32, side: u32, amount: Decimal, taker_fee_rate: Decimal) -> Result<(), u32> {
    if side == MARKET_ORDER_SIDE_ASK {
        if m.bids.len() == 0 {
            return Err(MATCH_ERROR_MARKET_ORDER_EMPTY_RIVAL);
        }

        if amount < m.min_amount {
            return Err(MATCH_ERROR_AMOUNT_TOO_SMALL);
        }
    } else {
        if m.asks.len() == 0 {
            return Err(MATCH_ERROR_MARKET_ORDER_EMPTY_RIVAL);
        }

        let min_money = m.asks.front().unwrap().price * m.min_amount;

        if amount < min_money {
            return Err(MATCH_ERROR_AMOUNT_TOO_SMALL);
        }
    }

    let now = Utc::now();

    m.order_id += 1;

    let order = Rc::new(Order{
        id : m.order_id,
        order_type: MARKET_ORDER_TYPE_MARKET,
        side : side,
        create_time: now.timestamp(), 
        update_time: Cell::new(now.timestamp()), 
        user_id: user_id,
        price: Decimal::ZERO,
        amount: Cell::new(amount),
        taker_fee_rate: taker_fee_rate,
        maker_fee_rate: Decimal::ZERO,
        left: Cell::new(amount),
        deal_stock: Cell::new(Decimal::ZERO),
        deal_money: Cell::new(Decimal::ZERO),
        deal_fee: Cell::new(Decimal::ZERO),
    });

    m.message_id += 1;
    publisher.publish_put_order(m, extern_id, &order);

    if side == MARKET_ORDER_SIDE_ASK {
        execute_market_ask_order(publisher, m, extern_id, &order);
    } else {
        execute_market_bid_order(publisher, m, extern_id, &order);
    }

    Ok(())
}

