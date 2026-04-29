use crate::correct_snap::correct_restore_snap_row_id;
use chrono::Utc;
use mysql::prelude::*;
use mysql::*;
use std::collections::HashSet;

pub(crate) fn prune_snapshots(pool: &Pool, age_secs: u64, max_keep: u32) {
    if age_secs == 0 && max_keep == 0 {
        return;
    }

    let protect_id = match correct_restore_snap_row_id(pool) {
        Some(id) => id,
        None => {
            warn!("snap retention: no eligible correct snapshot row, skip cleanup");
            return;
        }
    };

    let mut conn = match pool.get_conn() {
        Ok(c) => c,
        Err(e) => {
            error!("snap retention: get_conn: {}", e);
            return;
        }
    };

    let rows: Vec<(u64, i64)> =
        match conn.query("SELECT `id`, `time` FROM `snap` ORDER BY `time` ASC") {
            Ok(r) => r,
            Err(e) => {
                error!("snap retention: list snaps: {}", e);
                return;
            }
        };

    if rows.is_empty() {
        return;
    }

    let now = Utc::now().timestamp();
    let cutoff: Option<i64> = if age_secs > 0 {
        Some(now - age_secs as i64)
    } else {
        None
    };

    let mut delete_ids: HashSet<u64> = HashSet::new();

    if let Some(cut) = cutoff {
        for &(id, tm) in &rows {
            if id == protect_id {
                continue;
            }
            if tm < cut {
                delete_ids.insert(id);
            }
        }
    }

    if max_keep > 0 && rows.len() > max_keep as usize {
        let need = rows.len() - max_keep as usize;
        let mut taken = 0usize;
        for &(id, _) in &rows {
            if taken >= need {
                break;
            }
            if id == protect_id {
                continue;
            }
            delete_ids.insert(id);
            taken += 1;
        }
    }

    if delete_ids.is_empty() {
        return;
    }

    let mut to_drop: Vec<(u64, i64)> = rows
        .into_iter()
        .filter(|(id, _)| delete_ids.contains(id))
        .collect();
    to_drop.sort_by_key(|(_, tm)| *tm);

    for (id, tm) in &to_drop {
        let order_table = format!("snap_order_{}", tm);
        let drop_sql = format!("DROP TABLE IF EXISTS `{}`", order_table);
        if let Err(e) = conn.query_drop(&drop_sql) {
            error!("snap retention: {}: {}", drop_sql, e);
        }
        let del_sql = format!("DELETE FROM `snap` WHERE `id` = {}", id);
        if let Err(e) = conn.query_drop(&del_sql) {
            error!("snap retention: {}: {}", del_sql, e);
        }
    }

    info!(
        "snap retention: removed {} snapshot row(s), protect snap id {}",
        to_drop.len(),
        protect_id
    );
}
