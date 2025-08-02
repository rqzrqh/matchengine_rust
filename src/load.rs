use crate::market::*;
use mysql::*;
use mysql::prelude::*;
use core::panic;
use std::rc::Rc;
use std::cell::Cell;
use rust_decimal::prelude::*;
use std::process;
use json::*;

fn load_order(m: &mut Market, conn: &mut PooledConn, timestamp: i64) {
    let table = format!("snap_order_{}", timestamp);
    let limit = 1000;
    let mut last_id: u64 = 0;

    info!("load orders from {}", table);

    loop {
        let sql1 = format!("SELECT `id`, `t`, `side`, `create_time`, `update_time`, `user_id`,
            `price`, `amount`, `taker_fee_rate`, `maker_fee_rate`, `left`, `deal_stock` FROM `{}` 
            WHERE `id` > {} ORDER BY `id` LIMIT {}", table, last_id, limit);

        let sql2 = format!("SELECT `id`, `deal_money`, `deal_fee` FROM `{}` 
            WHERE `id` > {} ORDER BY `id` LIMIT {}", table, last_id, limit);

        let res_part1:Vec<(u64, u32, u32, i64, i64, u32, String, String, String, String, String, String)> = conn.query(&sql1).unwrap();
        let res_part2:Vec<(u64, String, String)> = conn.query(&sql2).unwrap();
        if res_part1.len() != res_part2.len() {
            panic!("error");
        }

        let count = res_part1.len();

        if count == 0 {
            break;
        }

        let mut i = 0;
        while i < count {
            let order = Rc::new(Order{
                id : res_part1[i].0,
                order_type: res_part1[i].1,
                side : res_part1[i].2,
                create_time: res_part1[i].3, 
                update_time: Cell::new(res_part1[i].4), 
                user_id: res_part1[i].5,
                price: {
                    let mut d = Decimal::from_str(&res_part1[i].6).unwrap();
                    d.rescale(m.money_prec-m.stock_prec);
                    d
                },
                amount: {
                    let mut d = Decimal::from_str(&res_part1[i].7).unwrap();
                    d.rescale(m.stock_prec);
                    Cell::new(d)
                },
                taker_fee_rate: {
                    let mut d = Decimal::from_str(&res_part1[i].8).unwrap();
                    d.rescale(m.fee_rate_prec);
                    d
                },
                maker_fee_rate: {
                    let mut d = Decimal::from_str(&res_part1[i].9).unwrap();
                    d.rescale(m.fee_rate_prec);
                    d
                },
                left: {
                    let mut d = Decimal::from_str(&res_part1[i].10).unwrap();
                    d.rescale(m.stock_prec);
                    Cell::new(d)
                },
                deal_stock: {
                    let mut d = Decimal::from_str(&res_part1[i].11).unwrap();
                    d.rescale(m.stock_prec);
                    Cell::new(d)
                },
                deal_money: {
                    let mut d = Decimal::from_str(&res_part2[i].1).unwrap();
                    d.rescale(m.money_prec);
                    Cell::new(d)
                },
                deal_fee: Cell::new(Decimal::from_str(&res_part2[i].2).unwrap()),
            });

            info!("id:{} type:{} side:{} create_time:{} update_time:{} user_id:{} price:{} amount:{} taker_fee_rate:{} maker_fee_rate:{} left:{} deal_stock:{} deal_money:{} deal_fee:{}",
                order.id, order.order_type, order.side, order.create_time, order.update_time.get(), order.user_id, order.price, order.amount.get(), order.taker_fee_rate, order.maker_fee_rate, 
                order.left.get(), order.deal_stock.get(), order.deal_money.get(), order.deal_fee.get());

            // assert order_type

            last_id = order.id;

            m.put_order(order);

            i += 1;
        }
    }
}

pub fn restore_output_state(m: &mut Market, pool: &Pool) -> Option<(u64, u64, u64)> {
    let mut conn = pool.get_conn().unwrap();

    let sql = "SELECT `id`, `quote_deals_id`, `settle_message_ids` from `snap` ORDER BY `id` DESC LIMIT 1";
    let res: Option<(u64, u64, String)> = conn.query_first(&sql).unwrap();

    match res {
        Some(v) => {

                let id = v.0;
                let quote_deals_id = v.1;
                let str_settle_message_ids = v.2;

                let parsed = json::parse(&str_settle_message_ids).expect("json decode failed");
                info!("{}", parsed);

                if !parsed.is_array() {
                    error!("settle_message_ids is not array {}", parsed);
                    process::exit(0);
                }

                if m.settle_message_ids.len() != parsed.len() {
                    error!("settle message ids length not equal {} {}", m.settle_message_ids.len(), parsed.len());
                    process::exit(0);
                }

                for i in 0..parsed.len() {
                    let group_message_id = parsed[i].as_u64().unwrap();
                    m.settle_message_ids[i] = group_message_id;
                }

                // find the first non-zero value
                let mut min_settle_group_message_id:u64 = 0;
                let mut settle_message_ids = m.settle_message_ids.clone();
                settle_message_ids.sort();
                for i in 0..settle_message_ids.len() {
                    if settle_message_ids[i] != 0 {
                        min_settle_group_message_id = settle_message_ids[i];
                        break;
                    }
                }

                let fake_snap_id = id + 1;

                info!("output state id:{} quote_deals_id:{} min_settle_message_id:{}", 
                    id, quote_deals_id, min_settle_group_message_id);

                return Some((fake_snap_id, quote_deals_id, min_settle_group_message_id));
        },
        None => {
            info!("no output state");
            return None;
        },
    }
}

pub fn restore_state(m: &mut Market, pool: &Pool) {

    let mut asks = 0;
    let mut bids = 0;

    let output_state: Option<(u64, u64, u64)> = restore_output_state(m, pool);
    match output_state {
        Some(output) => {

            let mut conn = pool.get_conn().unwrap();
            let mut last_snap_id = output.0;
            let quote_deals_id = output.1;
            let min_settle_group_message_id = output.2;

            loop {
                let sql = format!("SELECT `id`, `time`, `oper_id`, `order_id`, `deals_id`, `message_id`, `input_offset`, `asks`, `bids` from `snap` WHERE `id` < {} ORDER BY `id` DESC LIMIT 1", last_snap_id);
                info!("{}", sql);
                let res: Option<(u64, i64, u64, u64, u64, u64, i64, u32, u32)> = conn.query_first(&sql).unwrap();
                match res {
                    Some(v) => {

                        let id = v.0;
                        let tm = v.1;
                        let oper_id = v.2;
                        let order_id = v.3;
                        let deals_id = v.4;
                        let message_id = v.5;
                        let input_offset = v.6;
                        asks = v.7;
                        bids = v.8;

                        info!("found snap id:{} time:{} oper_id:{} order_id:{} deals_id:{} message_id:{} input_offset:{} asks:{} bids:{}", 
                            id, tm, oper_id, order_id, deals_id, message_id, input_offset, asks, bids);

                        if deals_id > quote_deals_id || message_id > min_settle_group_message_id {

                            if deals_id > quote_deals_id {
                                info!("deals_id not meet condition {} {}", deals_id, quote_deals_id);
                            }

                            if message_id > min_settle_group_message_id {
                                info!("message_id not meet condition {} {}", message_id, min_settle_group_message_id);
                            }

                            last_snap_id = id;
                            continue;
                        }

                        info!("deals_id meet condition {} {}", deals_id, quote_deals_id);
                        info!("message_id meet condition {} {}", message_id, min_settle_group_message_id);

                        m.oper_id = oper_id;
                        m.order_id = order_id;
                        m.deals_id = deals_id;
                        m.message_id = message_id;
                        m.input_offset = input_offset;

                        load_order(m, &mut conn, tm);
                    },
                    None => {
                        info!("not found snap");
                    },
                }
                break;
            }
        },
        None => {
        }
    }

    if asks != m.asks.len() as u32 || bids != m.bids.len() as u32 {
        panic!("order count error {} {} {} {}", asks, m.asks.len(), bids, m.bids.len());
    }

    info!("restore state oper_id:{} order_id:{} deals_id:{} message_id:{} input_offset:{}", 
        m.oper_id, m.order_id, m.deals_id, m.message_id, m.input_offset);
}
