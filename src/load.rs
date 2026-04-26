use crate::correct_snap::{find_correct_restore_snap, SnapStateForRestore};
use crate::market::*;
use mysql::*;
use mysql::prelude::*;
use core::panic;
use std::rc::Rc;
use std::cell::Cell;
use rust_decimal::prelude::*;

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
                    d.rescale(m.money_prec.saturating_sub(m.stock_prec));
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
                deal_fee: {
                    let mut d = Decimal::from_str(&res_part2[i].2).unwrap();
                    d.rescale(m.money_prec);
                    Cell::new(d)
                },
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

fn apply_restored_snap_to_market(
    m: &mut Market,
    conn: &mut PooledConn,
    quote_deals_id: u64,
    settle_message_ids: [u64; USER_SETTLE_GROUP_SIZE],
    chosen: Option<SnapStateForRestore>,
) {
    m.quote_deals_id = quote_deals_id;
    m.settle_message_ids = settle_message_ids;
    if let Some(row) = chosen {
        m.oper_id = row.oper_id;
        m.order_id = row.order_id;
        m.deals_id = row.deals_id;
        m.message_id = row.message_id;
        m.input_offset = row.input_offset;
        m.input_sequence_id = row.input_sequence_id;
        load_order(m, conn, row.time);
    }
}

pub fn restore_state(m: &mut Market, pool: &Pool) {

    let mut asks = 0;
    let mut bids = 0;

    if let Some(snap) = find_correct_restore_snap(pool) {
        asks = snap.asks;
        bids = snap.bids;
        let mut conn = pool.get_conn().unwrap();
        apply_restored_snap_to_market(
            m,
            &mut conn,
            snap.quote_deals_id,
            snap.settle_message_ids,
            snap.chosen,
        );
    }

    if asks != m.asks.len() as u32 || bids != m.bids.len() as u32 {
        panic!("order count error {} {} {} {}", asks, m.asks.len(), bids, m.bids.len());
    }

    info!("restore state oper_id:{} order_id:{} deals_id:{} message_id:{} input_offset:{} input_sequence_id:{}", 
        m.oper_id, m.order_id, m.deals_id, m.message_id, m.input_offset, m.input_sequence_id);
}
