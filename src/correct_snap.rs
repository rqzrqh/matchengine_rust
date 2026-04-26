use crate::market::USER_SETTLE_GROUP_SIZE;
use json::*;
use mysql::prelude::*;
use mysql::*;
use std::process;

#[derive(Clone, Copy)]
pub(crate) struct SnapStateForRestore {
    pub(crate) id: u64,
    pub(crate) time: i64,
    pub(crate) oper_id: u64,
    pub(crate) order_id: u64,
    pub(crate) deals_id: u64,
    pub(crate) message_id: u64,
    pub(crate) input_offset: i64,
    pub(crate) input_sequence_id: u64,
    pub(crate) asks: u32,
    pub(crate) bids: u32,
}

fn get_output_state(
    pool: &Pool,
    verbose: bool,
) -> Option<(u64, u64, u64, [u64; USER_SETTLE_GROUP_SIZE])> {
    let mut conn = pool.get_conn().unwrap();

    let sql = "SELECT `id`, `quote_deals_id`, `settle_message_ids` from `snap` ORDER BY `id` DESC LIMIT 1";
    let res: Option<(u64, u64, String)> = conn.query_first(&sql).unwrap();

    match res {
        Some(v) => {
            let id = v.0;
            let quote_deals_id = v.1;
            let str_settle_message_ids = v.2;

            let parsed = json::parse(&str_settle_message_ids).expect("json decode failed");
            if verbose {
                info!("{}", parsed);
            }

            if !parsed.is_array() {
                error!("settle_message_ids is not array {}", parsed);
                process::exit(0);
            }

            if parsed.len() != USER_SETTLE_GROUP_SIZE {
                error!(
                    "settle message ids length not equal expected {} (got {})",
                    USER_SETTLE_GROUP_SIZE,
                    parsed.len()
                );
                process::exit(0);
            }

            let mut settle_message_ids = [0u64; USER_SETTLE_GROUP_SIZE];
            for i in 0..USER_SETTLE_GROUP_SIZE {
                settle_message_ids[i] = parsed[i].as_u64().unwrap();
            }

            let mut min_settle_group_message_id: u64 = 0;
            let mut sorted = settle_message_ids;
            sorted.sort();
            for i in 0..sorted.len() {
                if sorted[i] != 0 {
                    min_settle_group_message_id = sorted[i];
                    break;
                }
            }

            let fake_snap_id = id + 1;

            if verbose {
                info!(
                    "output state id:{} quote_deals_id:{} min_settle_message_id:{}",
                    id, quote_deals_id, min_settle_group_message_id
                );
            }

            return Some((fake_snap_id, quote_deals_id, min_settle_group_message_id, settle_message_ids));
        }
        None => {
            if verbose {
                info!("no output state");
            }
            return None;
        }
    }
}

fn load_input_state_and_corresponding_sm_state_from_db(
    conn: &mut PooledConn,
    last_snap_id: u64,
    verbose: bool,
) -> Option<SnapStateForRestore> {
    let sql = format!(
        "SELECT `id`, `time`, `oper_id`, `order_id`, `deals_id`, `message_id`, `input_offset`, `input_sequence_id`, `asks`, `bids` from `snap` WHERE `id` < {} ORDER BY `id` DESC LIMIT 1",
        last_snap_id
    );
    if verbose {
        info!("{}", sql);
    }
    let res: Option<(u64, i64, u64, u64, u64, u64, i64, u64, u32, u32)> = conn.query_first(&sql).unwrap();
    res.map(|v| SnapStateForRestore {
        id: v.0,
        time: v.1,
        oper_id: v.2,
        order_id: v.3,
        deals_id: v.4,
        message_id: v.5,
        input_offset: v.6,
        input_sequence_id: v.7,
        asks: v.8,
        bids: v.9,
    })
}

fn snap_exceeds_saved_output_with_log(
    row: SnapStateForRestore,
    quote_deals_id: u64,
    min_settle_group_message_id: u64,
    verbose: bool,
) -> Option<u64> {
    let deals_over = row.deals_id > quote_deals_id;
    let message_over = row.message_id > min_settle_group_message_id;
    if !deals_over && !message_over {
        return None;
    }
    if verbose {
        if deals_over {
            info!("deals_id not meet condition {} {}", row.deals_id, quote_deals_id);
        }
        if message_over {
            info!(
                "message_id not meet condition {} {}",
                row.message_id, min_settle_group_message_id
            );
        }
    }
    Some(row.id)
}

pub(crate) struct CorrectRestoreSnap {
    pub(crate) quote_deals_id: u64,
    pub(crate) settle_message_ids: [u64; USER_SETTLE_GROUP_SIZE],
    pub(crate) chosen: Option<SnapStateForRestore>,
    pub(crate) asks: u32,
    pub(crate) bids: u32,
}

pub(crate) fn find_correct_restore_snap(pool: &Pool) -> Option<CorrectRestoreSnap> {
    find_correct_restore_snap_impl(pool, true)
}

fn find_correct_restore_snap_impl(pool: &Pool, verbose: bool) -> Option<CorrectRestoreSnap> {
    let output = get_output_state(pool, verbose)?;
    let mut conn = pool.get_conn().unwrap();
    let mut last_snap_id = output.0;
    let quote_deals_id = output.1;
    let min_settle_group_message_id = output.2;

    let mut asks = 0u32;
    let mut bids = 0u32;
    let mut chosen: Option<SnapStateForRestore> = None;

    loop {
        match load_input_state_and_corresponding_sm_state_from_db(&mut conn, last_snap_id, verbose) {
            Some(row) => {
                if verbose {
                    info!(
                        "found snap id:{} time:{} oper_id:{} order_id:{} deals_id:{} message_id:{} input_offset:{} input_sequence_id:{} asks:{} bids:{}",
                        row.id,
                        row.time,
                        row.oper_id,
                        row.order_id,
                        row.deals_id,
                        row.message_id,
                        row.input_offset,
                        row.input_sequence_id,
                        row.asks,
                        row.bids
                    );
                }

                asks = row.asks;
                bids = row.bids;

                if let Some(prev_snap_id) = snap_exceeds_saved_output_with_log(
                    row,
                    quote_deals_id,
                    min_settle_group_message_id,
                    verbose,
                ) {
                    last_snap_id = prev_snap_id;
                    continue;
                }

                if verbose {
                    info!("deals_id meet condition {} {}", row.deals_id, quote_deals_id);
                    info!("message_id meet condition {} {}", row.message_id, min_settle_group_message_id);
                }
                chosen = Some(row);
                break;
            }
            None => {
                if verbose {
                    info!("not found snap");
                }
                break;
            }
        }
    }

    Some(CorrectRestoreSnap {
        quote_deals_id: output.1,
        settle_message_ids: output.3,
        chosen,
        asks,
        bids,
    })
}

pub(crate) fn correct_restore_snap_row_id(pool: &Pool) -> Option<u64> {
    find_correct_restore_snap_impl(pool, false)?.chosen.map(|r| r.id)
}
