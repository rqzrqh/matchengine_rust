use crate::market::*;
use mysql::*;
use mysql::prelude::*;
use std::rc::Rc;
use std::cell::Cell;
use rust_decimal::prelude::*;
use std::process;

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

pub fn restore_state(m: &mut Market, pool: &Pool) {

    let mut conn = pool.get_conn().unwrap();

    let res = conn.query_map(
        "SELECT `time`, `oper_id`, `order_id`, `deals_id`, `message_id`, `input_offset` from `snap` ORDER BY `id` DESC LIMIT 1", 
        |(time, oper_id, order_id, deals_id, message_id, input_offset)| (time, oper_id, order_id, deals_id, message_id, input_offset)
    );

    match res {
        Ok(v) => {
            if v.len() != 0 {
                m.oper_id = v[0].1;
                m.order_id = v[0].2;
                m.deals_id = v[0].3;
                m.message_id = v[0].4;
                m.input_offset = v[0].5;

                info!("last state time:{} oper_id:{} order_id:{} deals_id:{} message_id:{} mq_offset:{}", 
                    v[0].0, m.oper_id, m.order_id, m.deals_id, m.message_id, m.input_offset);

                load_order(m, &mut conn, v[0].0);
            }
        },
        Err(e) => {
            print!("query failed {}", e);
            process::exit(0);
        },
    }
}
