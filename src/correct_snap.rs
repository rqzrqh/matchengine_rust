use crate::market::USER_SETTLE_GROUP_SIZE;
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

struct Anchor {
    newest_id: u64,
    first_bound: u64,
    quote_deals_id: u64,
    min_settle_msg: u64,
    settle_message_ids: [u64; USER_SETTLE_GROUP_SIZE],
}

fn read_anchor_from_db(pool: &Pool, verbose: bool) -> Option<Anchor> {
    let mut conn = pool.get_conn().unwrap();
    let row: Option<(u64, u64, String)> = conn
        .query_first("SELECT `id`, `quote_deals_id`, `settle_message_ids` from `snap` ORDER BY `id` DESC LIMIT 1")
        .unwrap();
    let (id, quote_deals_id, settle_json) = match row {
        Some(r) => r,
        None => {
            if verbose {
                info!("no output state");
            }
            return None;
        }
    };

    let parsed = json::parse(&settle_json).expect("json decode failed");
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

    let settle_message_ids =
        std::array::from_fn(|i| parsed[i].as_u64().unwrap());
    let mut sorted = settle_message_ids;
    sorted.sort_unstable();
    let min_settle_msg = sorted.iter().copied().find(|&x| x != 0).unwrap_or(0);

    if verbose {
        info!(
            "output state id:{} quote_deals_id:{} min_settle_message_id:{}",
            id, quote_deals_id, min_settle_msg
        );
    }

    Some(Anchor {
        newest_id: id,
        first_bound: id + 1,
        quote_deals_id,
        min_settle_msg,
        settle_message_ids,
    })
}

fn load_snap_row(conn: &mut PooledConn, last_bound: u64, verbose: bool) -> Option<SnapStateForRestore> {
    let sql = format!(
        "SELECT `id`, `time`, `oper_id`, `order_id`, `deals_id`, `message_id`, `input_offset`, `input_sequence_id`, `asks`, `bids` from `snap` WHERE `id` < {} ORDER BY `id` DESC LIMIT 1",
        last_bound
    );
    if verbose {
        info!("{}", sql);
    }
    conn.query_first(&sql).unwrap().map(|v: (u64, i64, u64, u64, u64, u64, i64, u64, u32, u32)| {
        SnapStateForRestore {
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
        }
    })
}

#[derive(Default)]
struct RetentionTrace {
    snap_empty: bool,
    anchor_line: String,
    steps: Vec<String>,
}

impl RetentionTrace {
    fn from_anchor(a: &Anchor) -> Self {
        let nz = a.settle_message_ids.iter().filter(|&&x| x != 0).count();
        Self {
            snap_empty: false,
            anchor_line: format!(
                "snap restore trace (retention): newest_snap_id={} first_bound(id<{}) anchor_Q={} anchor_M={} nonzero_settle_slots={}/{}",
                a.newest_id, a.first_bound, a.quote_deals_id, a.min_settle_msg, nz, USER_SETTLE_GROUP_SIZE
            ),
            steps: Vec::new(),
        }
    }

    fn push_scan_step(&mut self, step: usize, bound: u64, q: u64, m: u64, row: Option<&SnapStateForRestore>) {
        let line = match row {
            None => format!("snap restore trace: step {step} — WHERE id < {bound} → no row"),
            Some(r) => {
                let (d_over, m_over) = (r.deals_id > q, r.message_id > m);
                let tag = match (d_over, m_over) {
                    (false, false) => "accept",
                    (true, true) => "reject: deals & message over anchor",
                    (true, false) => "reject: deals over anchor_Q",
                    (false, true) => "reject: message over anchor_M",
                };
                format!(
                    "snap restore trace: step {step} — WHERE id < {bound} → id={} deals_id={} message_id={} | deals_over={d_over} (>{q}) msg_over={m_over} (>{m}) → {tag}",
                    r.id, r.deals_id, r.message_id
                )
            }
        };
        self.steps.push(line);
    }

    fn log(&self) {
        if self.snap_empty {
            warn!("snap restore trace (retention): table `snap` is empty");
            return;
        }
        warn!("{}", self.anchor_line);
        for line in &self.steps {
            warn!("{}", line);
        }
        warn!("snap restore trace: no row satisfies deals_id <= anchor_Q AND message_id <= anchor_M from newest row; cleanup skipped");
    }
}

pub(crate) struct CorrectRestoreSnap {
    pub(crate) quote_deals_id: u64,
    pub(crate) settle_message_ids: [u64; USER_SETTLE_GROUP_SIZE],
    pub(crate) chosen: Option<SnapStateForRestore>,
    pub(crate) asks: u32,
    pub(crate) bids: u32,
}

pub(crate) fn find_correct_restore_snap(pool: &Pool) -> Option<CorrectRestoreSnap> {
    let mut trace: Option<RetentionTrace> = None;
    find_correct_restore_snap_impl(pool, true, &mut trace)
}

fn find_correct_restore_snap_impl(
    pool: &Pool,
    verbose: bool,
    trace: &mut Option<RetentionTrace>,
) -> Option<CorrectRestoreSnap> {
    let anchor = match read_anchor_from_db(pool, verbose) {
        Some(a) => a,
        None => {
            if let Some(t) = trace {
                t.snap_empty = true;
            }
            return None;
        }
    };
    if let Some(t) = trace.as_mut() {
        *t = RetentionTrace::from_anchor(&anchor);
    }

    let mut conn = pool.get_conn().unwrap();
    let q = anchor.quote_deals_id;
    let m = anchor.min_settle_msg;
    let mut last_bound = anchor.first_bound;
    let mut asks = 0u32;
    let mut bids = 0u32;
    let mut chosen = None;
    let mut step = 0usize;

    loop {
        step += 1;
        let bound = last_bound;
        match load_snap_row(&mut conn, last_bound, verbose) {
            Some(row) => {
                if verbose {
                    info!(
                        "found snap id:{} time:{} oper_id:{} order_id:{} deals_id:{} message_id:{} input_offset:{} input_sequence_id:{} asks:{} bids:{}",
                        row.id, row.time, row.oper_id, row.order_id, row.deals_id, row.message_id,
                        row.input_offset, row.input_sequence_id, row.asks, row.bids
                    );
                }
                asks = row.asks;
                bids = row.bids;
                let deals_over = row.deals_id > q;
                let msg_over = row.message_id > m;
                if let Some(t) = trace.as_mut() {
                    t.push_scan_step(step, bound, q, m, Some(&row));
                }
                if verbose {
                    if deals_over {
                        info!("deals_id not meet condition {} {}", row.deals_id, q);
                    }
                    if msg_over {
                        info!("message_id not meet condition {} {}", row.message_id, m);
                    }
                }
                if deals_over || msg_over {
                    last_bound = row.id;
                    continue;
                }
                if verbose {
                    info!("deals_id meet condition {} {}", row.deals_id, q);
                    info!("message_id meet condition {} {}", row.message_id, m);
                }
                chosen = Some(row);
                break;
            }
            None => {
                if verbose {
                    info!("not found snap");
                }
                if let Some(t) = trace.as_mut() {
                    t.push_scan_step(step, bound, q, m, None);
                }
                break;
            }
        }
    }

    Some(CorrectRestoreSnap {
        quote_deals_id: anchor.quote_deals_id,
        settle_message_ids: anchor.settle_message_ids,
        chosen,
        asks,
        bids,
    })
}

pub(crate) fn correct_restore_snap_row_id(pool: &Pool) -> Option<u64> {
    let mut trace = Some(RetentionTrace::default());
    let crs = find_correct_restore_snap_impl(pool, false, &mut trace);
    if let Some(id) = crs.as_ref().and_then(|c| c.chosen.as_ref().map(|r| r.id)) {
        return Some(id);
    }
    warn!("snap retention: no eligible correct snapshot row, skip cleanup");
    if let Some(t) = trace.as_ref() {
        t.log();
    }
    None
}
