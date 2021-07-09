use crate::market::*;
use nix::unistd::{fork, ForkResult};
use mysql::*;
use mysql::prelude::*;
use std::rc::Rc;
use skiplist::OrderedSkipList;
use std::process;

fn dump_order_list(conn: &mut PooledConn, table: &String, orders: &OrderedSkipList<Rc<Order>>) {
    let limit = 1000;

    let mut count: usize = 0;
    let mut sql:String = "".to_string();

    for order in orders {

        if count == 0 {
            sql = format!("INSERT INTO `{}` (`id`, `t`, `side`, `create_time`, `update_time`, `user_id`,
                    `price`, `amount`, `taker_fee_rate`, `maker_fee_rate`, `left`, `deal_stock`, `deal_money`, `deal_fee`) VALUES ", table);
        } else {
            sql = format!("{}, ", sql);
        }

        sql = format!("{} ({},{},{},{},{},{},{},{},{},{},{},{},{},{})", sql,
            order.id, order.order_type, order.side, order.create_time, order.update_time.get(), order.user_id, order.price, order.amount.get().to_string(), order.taker_fee_rate.to_string(), order.maker_fee_rate.to_string(),
            order.left.get().to_string(), order.deal_stock.get().to_string(), order.deal_money.get().to_string(), order.deal_fee.get().to_string());
            count = count + 1;

        if count >= limit {
            count = 0;
            info!("{}", sql);
            conn.query_drop(&sql).unwrap_or_else(|e| {
                panic!("{}", e.to_string());
            });
        }
    }

    if count > 0 {
        info!("{}", sql);
        conn.query_drop(&sql).unwrap_or_else(|e| {
            panic!("{}", e.to_string());
        });
    }
}

fn dump_orders_to_db(conn: &mut PooledConn, m: &Market, table: &String) {

    let sql = format!("DROP TABLE IF EXISTS {}", table);
    conn.query_drop(&sql).unwrap_or_else(|e| {
        panic!("{}", e.to_string());
    });

    let sql = format!("CREATE TABLE IF NOT EXISTS `{}` LIKE `snap_order_example`", &table);
    conn.query_drop(&sql).unwrap_or_else(|e| {
        panic!("{}", e.to_string());
    });

    dump_order_list(conn, table, &m.asks);
    dump_order_list(conn, table, &m.bids);
}

fn dump_state_version_to_db(conn: &mut PooledConn, m: &Market, tm: i64) {
    let sql = format!("INSERT INTO `snap` (`id`, `time`, `oper_id`, `order_id`, `deals_id`, `message_id`, `input_offset`) 
                                        VALUES (NULL, {}, {}, {}, {}, {}, {}) ", tm, m.oper_id, m.order_id, m.deals_id, m.message_id, m.input_offset);
    info!("{}", sql);
    conn.query_drop(&sql).unwrap_or_else(|e| {
        panic!("{}", e.to_string());
    });
}

pub fn handle_dump(m: &mut Market, tm: i64, pool: &Pool) {

    unsafe{
        match fork() {
            Ok(ForkResult::Parent { child, .. }) => {
                //info!("Continuing execution in parent process, new child has pid: {}", child);
                return;
            },
            Ok(ForkResult::Child) => {
                //info!("I'm a new child process");
            },
            Err(_) => {
                panic!("Fork failed");
            }
        }
    }

    info!("dump to {}", tm);

    let mut conn = pool.get_conn().unwrap();

    let table = format!("snap_order_{}", tm);

    dump_orders_to_db(&mut conn, m, &table);
    dump_state_version_to_db(&mut conn, m, tm);

    process::exit(0);
}