use crate::market::*;
use json::*;
use mysql::prelude::*;
use mysql::*;
use nix::unistd::{ForkResult, fork};
use skiplist::OrderedSkipList;
use std::process;
use std::rc::Rc;

fn dump_order_list(conn: &mut PooledConn, table: &String, orders: &OrderedSkipList<Rc<Order>>) {
    let limit = 1000;

    let mut count: usize = 0;
    let mut sql: String = "".to_string();

    for order in orders {
        if count == 0 {
            sql = format!("INSERT INTO `{}` (`id`, `t`, `side`, `create_time`, `update_time`, `user_id`,
                    `price`, `amount`, `taker_fee_rate`, `maker_fee_rate`, `left`, `deal_stock`, `deal_money`, `deal_fee`) VALUES ", table);
        } else {
            sql = format!("{}, ", sql);
        }

        sql = format!(
            "{} ({},{},{},{},{},{},{},{},{},{},{},{},{},{})",
            sql,
            order.id,
            order.order_type,
            order.side,
            order.create_time,
            order.update_time.get(),
            order.user_id,
            order.price,
            order.amount.get().to_string(),
            order.taker_fee_rate.to_string(),
            order.maker_fee_rate.to_string(),
            order.left.get().to_string(),
            order.deal_stock.get().to_string(),
            order.deal_money.get().to_string(),
            order.deal_fee.get().to_string()
        );
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

    let sql = format!(
        "CREATE TABLE IF NOT EXISTS `{}` LIKE `snap_order_example`",
        &table
    );
    conn.query_drop(&sql).unwrap_or_else(|e| {
        panic!("{}", e.to_string());
    });

    dump_order_list(conn, table, &m.asks);
    dump_order_list(conn, table, &m.bids);
}

fn settle_ids_to_json(ids: &[u64; USER_SETTLE_GROUP_SIZE]) -> String {
    let mut ay = JsonValue::new_array();
    for i in 0..ids.len() {
        ay[i] = ids[i].into();
    }
    ay.to_string()
}

// input and corespond state, progress of publish output
fn dump_others_to_db(conn: &mut PooledConn, m: &Market, tm: i64) {
    let settle_message_ids = settle_ids_to_json(&m.settle_message_ids);
    let pushed_settle_message_ids = settle_ids_to_json(&m.pushed_settle_message_ids);

    let sql = format!("INSERT INTO `snap` (`id`, `time`, `oper_id`, `order_id`, `deals_id`, `message_id`, `input_offset`, `input_sequence_id`, `asks`, `bids`, `settle_message_ids`, `pushed_quote_deals_id`, `pushed_settle_message_ids`) 
                                        VALUES (NULL, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', {}, '{}') ", tm, m.oper_id, m.order_id, m.deals_id, m.message_id, m.input_offset, m.input_sequence_id, m.asks.len(), m.bids.len(), settle_message_ids, m.pushed_quote_deals_id, pushed_settle_message_ids);
    info!("{}", sql);
    conn.query_drop(&sql).unwrap_or_else(|e| {
        panic!("{}", e.to_string());
    });
}

/// After `fork`, the child must not use the parent's [`Pool`]: duplicated TCP sockets cause
/// `CodecError { Packets out of sync }`. Open a new pool from `db_url` in the child only.
pub fn handle_dump(m: &mut Market, tm: i64, db_url: &str) {
    unsafe {
        match fork() {
            Ok(ForkResult::Parent { .. }) => {
                return;
            }
            Ok(ForkResult::Child) => {}
            Err(_) => {
                panic!("Fork failed");
            }
        }
    }

    info!("dump to {}", tm);

    let opts = Opts::from_url(db_url).expect("dump: invalid db url");
    let pool = Pool::new(opts).expect("dump: Pool::new in child");
    let mut conn = pool.get_conn().expect("dump: get_conn in child");

    let table = format!("snap_order_{}", tm);

    // dump orders first to keep consistence with snap table record
    dump_orders_to_db(&mut conn, m, &table);
    dump_others_to_db(&mut conn, m, tm);

    process::exit(0);
}
