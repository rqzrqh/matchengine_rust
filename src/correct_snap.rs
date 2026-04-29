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
    pub(crate) settle_message_ids: [u64; USER_SETTLE_GROUP_SIZE],
}

struct Anchor {
    newest_id: u64,
    first_bound: u64,
    pushed_quote_deals_id: u64,
    pushed_settle_message_ids: [u64; USER_SETTLE_GROUP_SIZE],
}

fn parse_settle_message_ids(
    raw: &str,
    field: &str,
    verbose: bool,
) -> [u64; USER_SETTLE_GROUP_SIZE] {
    let parsed = json::parse(raw).expect("json decode failed");
    if verbose {
        info!("{}", parsed);
    }
    if !parsed.is_array() {
        error!("{} is not array {}", field, parsed);
        process::exit(0);
    }
    if parsed.len() != USER_SETTLE_GROUP_SIZE {
        error!(
            "{} length not equal expected {} (got {})",
            field,
            USER_SETTLE_GROUP_SIZE,
            parsed.len()
        );
        process::exit(0);
    }

    std::array::from_fn(|i| parsed[i].as_u64().unwrap())
}

fn settle_ids_within_pushed(
    snapshot_ids: &[u64; USER_SETTLE_GROUP_SIZE],
    pushed_ids: &[u64; USER_SETTLE_GROUP_SIZE],
) -> bool {
    snapshot_ids
        .iter()
        .zip(pushed_ids.iter())
        .all(|(snapshot_id, pushed_id)| snapshot_id <= pushed_id)
}

fn snapshot_output_within_pushed(
    row: &SnapStateForRestore,
    pushed_quote_deals_id: u64,
    pushed_settle_message_ids: &[u64; USER_SETTLE_GROUP_SIZE],
) -> (bool, bool) {
    (
        row.deals_id <= pushed_quote_deals_id,
        settle_ids_within_pushed(&row.settle_message_ids, pushed_settle_message_ids),
    )
}

fn read_anchor_from_db(pool: &Pool, verbose: bool) -> Option<Anchor> {
    let mut conn = pool.get_conn().unwrap();
    let row: Option<(u64, u64, String)> = conn
        .query_first("SELECT `id`, `pushed_quote_deals_id`, `pushed_settle_message_ids` from `snap` ORDER BY `id` DESC LIMIT 1")
        .unwrap();
    let (id, pushed_quote_deals_id, settle_json) = match row {
        Some(r) => r,
        None => {
            if verbose {
                info!("no output state");
            }
            return None;
        }
    };

    let pushed_settle_message_ids =
        parse_settle_message_ids(&settle_json, "pushed_settle_message_ids", verbose);

    if verbose {
        info!(
            "output state id:{} pushed_quote_deals_id:{} pushed_settle_message_ids:{:?}",
            id, pushed_quote_deals_id, pushed_settle_message_ids
        );
    }

    Some(Anchor {
        newest_id: id,
        first_bound: id + 1,
        pushed_quote_deals_id,
        pushed_settle_message_ids,
    })
}

fn load_snap_row(
    conn: &mut PooledConn,
    last_bound: u64,
    verbose: bool,
) -> Option<SnapStateForRestore> {
    let sql = format!(
        "SELECT `id`, `time`, `oper_id`, `order_id`, `deals_id`, `message_id`, `input_offset`, `input_sequence_id`, `asks`, `bids`, `settle_message_ids` from `snap` WHERE `id` < {} ORDER BY `id` DESC LIMIT 1",
        last_bound
    );
    if verbose {
        info!("{}", sql);
    }
    conn.query_first(&sql).unwrap().map(
        |v: (u64, i64, u64, u64, u64, u64, i64, u64, u32, u32, String)| SnapStateForRestore {
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
            settle_message_ids: parse_settle_message_ids(&v.10, "settle_message_ids", verbose),
        },
    )
}

#[derive(Default)]
struct RetentionTrace {
    snap_empty: bool,
    anchor_line: String,
    steps: Vec<String>,
}

impl RetentionTrace {
    fn from_anchor(a: &Anchor) -> Self {
        let nz = a
            .pushed_settle_message_ids
            .iter()
            .filter(|&&x| x != 0)
            .count();
        Self {
            snap_empty: false,
            anchor_line: format!(
                "snap restore trace (retention): newest_snap_id={} first_bound(id<{}) anchor_Q={} nonzero_pushed_settle_slots={}/{}",
                a.newest_id, a.first_bound, a.pushed_quote_deals_id, nz, USER_SETTLE_GROUP_SIZE
            ),
            steps: Vec::new(),
        }
    }

    fn push_scan_step(
        &mut self,
        step: usize,
        bound: u64,
        q: u64,
        pushed_settle_message_ids: &[u64; USER_SETTLE_GROUP_SIZE],
        row: Option<&SnapStateForRestore>,
    ) {
        let line = match row {
            None => format!("snap restore trace: step {step} — WHERE id < {bound} → no row"),
            Some(r) => {
                let (deal_pushed, settle_pushed) =
                    snapshot_output_within_pushed(r, q, pushed_settle_message_ids);
                let tag = match (deal_pushed, settle_pushed) {
                    (true, true) => "accept",
                    (false, false) => "reject: deals & settle not pushed",
                    (false, true) => "reject: deals not pushed",
                    (true, false) => "reject: settle not pushed",
                };
                format!(
                    "snap restore trace: step {step} — WHERE id < {bound} → id={} deals_id={} message_id={} | deal_pushed={deal_pushed} (<= {q}) settle_pushed={settle_pushed} → {tag}",
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
        warn!(
            "snap restore trace: no row satisfies deals_id <= anchor_Q AND settle_message_ids <= pushed_settle_message_ids from newest row; cleanup skipped"
        );
    }
}

pub(crate) struct CorrectRestoreSnap {
    pub(crate) pushed_quote_deals_id: u64,
    pub(crate) pushed_settle_message_ids: [u64; USER_SETTLE_GROUP_SIZE],
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
    let q = anchor.pushed_quote_deals_id;
    let pushed_settle_message_ids = anchor.pushed_settle_message_ids;
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
                let (deal_pushed, settle_pushed) =
                    snapshot_output_within_pushed(&row, q, &pushed_settle_message_ids);
                if let Some(t) = trace.as_mut() {
                    t.push_scan_step(step, bound, q, &pushed_settle_message_ids, Some(&row));
                }
                if verbose {
                    if !deal_pushed {
                        info!("deals_id not meet condition {} {}", row.deals_id, q);
                    }
                    if !settle_pushed {
                        info!(
                            "settle_message_ids not meet condition {:?}",
                            row.settle_message_ids
                        );
                    }
                }
                if !deal_pushed || !settle_pushed {
                    last_bound = row.id;
                    continue;
                }
                if verbose {
                    info!("deals_id meet condition {} {}", row.deals_id, q);
                    info!(
                        "settle_message_ids meet condition {:?}",
                        row.settle_message_ids
                    );
                }
                chosen = Some(row);
                break;
            }
            None => {
                if verbose {
                    info!("not found snap");
                }
                if let Some(t) = trace.as_mut() {
                    t.push_scan_step(step, bound, q, &pushed_settle_message_ids, None);
                }
                break;
            }
        }
    }

    Some(CorrectRestoreSnap {
        pushed_quote_deals_id: anchor.pushed_quote_deals_id,
        pushed_settle_message_ids: anchor.pushed_settle_message_ids,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn snap_with_outputs(
        deals_id: u64,
        settle_message_ids: [u64; USER_SETTLE_GROUP_SIZE],
    ) -> SnapStateForRestore {
        SnapStateForRestore {
            id: 1,
            time: 0,
            oper_id: 0,
            order_id: 0,
            deals_id,
            message_id: 0,
            input_offset: 0,
            input_sequence_id: 0,
            asks: 0,
            bids: 0,
            settle_message_ids,
        }
    }

    #[test]
    fn settle_ids_are_within_pushed_when_all_snapshot_ids_are_not_greater() {
        let mut snapshot_ids = [3; USER_SETTLE_GROUP_SIZE];
        let pushed_ids = [3; USER_SETTLE_GROUP_SIZE];
        assert!(settle_ids_within_pushed(&snapshot_ids, &pushed_ids));

        snapshot_ids[7] = 4;
        assert!(!settle_ids_within_pushed(&snapshot_ids, &pushed_ids));
    }

    #[test]
    fn snapshot_output_within_pushed_checks_quote_and_settle_ids() {
        let pushed_settle_message_ids = [8; USER_SETTLE_GROUP_SIZE];
        let row = snap_with_outputs(5, [8; USER_SETTLE_GROUP_SIZE]);
        assert_eq!(
            snapshot_output_within_pushed(&row, 5, &pushed_settle_message_ids),
            (true, true)
        );

        let row = snap_with_outputs(6, [8; USER_SETTLE_GROUP_SIZE]);
        assert_eq!(
            snapshot_output_within_pushed(&row, 5, &pushed_settle_message_ids),
            (false, true)
        );

        let mut settle_message_ids = [8; USER_SETTLE_GROUP_SIZE];
        settle_message_ids[13] = 9;
        let row = snap_with_outputs(5, settle_message_ids);
        assert_eq!(
            snapshot_output_within_pushed(&row, 5, &pushed_settle_message_ids),
            (true, false)
        );
    }
}
