//! YAML configuration: market, Kafka brokers, MySQL credentials, snapshot dump and retention.

use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;

#[derive(Debug, Clone, Deserialize)]
pub struct SnapCleanupCfg {
    /// Non-zero: `snap` rows with `time` older than (now minus this many seconds) may be deleted. Zero disables age-based pruning.
    #[serde(default)]
    pub max_age_secs: u64,
    /// Non-zero: keep at most this many newest snapshots (by `time`); older rows may be deleted. Zero disables count-based pruning.
    #[serde(default)]
    pub max_snapshots: u32,
    /// How often to run the cleanup pass (`prune_snapshots` returns immediately when both limits are zero).
    #[serde(default = "default_snap_cleanup_interval_secs")]
    pub cleanup_interval_secs: u64,
}

fn default_snap_cleanup_interval_secs() -> u64 {
    60
}

impl Default for SnapCleanupCfg {
    fn default() -> Self {
        Self {
            max_age_secs: 0,
            max_snapshots: 0,
            cleanup_interval_secs: default_snap_cleanup_interval_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SnapDumpCfg {
    /// Seconds between snapshot dumps to MySQL (`snap` row + `snap_order_*` table), each in a forked child.
    #[serde(default = "default_snap_dump_interval_secs")]
    pub dump_interval_secs: u64,
}

fn default_snap_dump_interval_secs() -> u64 {
    600
}

impl Default for SnapDumpCfg {
    fn default() -> Self {
        Self {
            dump_interval_secs: default_snap_dump_interval_secs(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub market: MarketCfg,
    pub brokers: String,
    pub db: DbCfg,
    #[serde(default)]
    pub snap_cleanup: SnapCleanupCfg,
    #[serde(default)]
    pub snap_dump: SnapDumpCfg,
}

#[derive(Debug, Deserialize)]
pub struct MarketCfg {
    pub name: String,
    pub stock_prec: u32,
    pub money_prec: u32,
    pub fee_prec: u32,
    pub min_amount: String,
}

#[derive(Debug, Deserialize)]
pub struct DbCfg {
    pub addr: String,
    pub user: String,
    pub passwd: String,
}

pub fn load_config(path: &str) -> Result<Config, String> {
    let contents = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    serde_yaml::from_str(&contents).map_err(|e| e.to_string())
}

/// Fail fast before the matcher runs: precision fields must fit `rust_decimal` and relate consistently.
pub fn validate_config(cfg: &Config) -> Result<(), String> {
    const MAX_SCALE: u32 = Decimal::MAX_SCALE;

    if cfg.brokers.trim().is_empty() {
        return Err("brokers must be non-empty".into());
    }
    if cfg.db.addr.trim().is_empty() || cfg.db.user.trim().is_empty() {
        return Err("db.addr and db.user must be non-empty".into());
    }

    let m = &cfg.market;
    if m.name.trim().is_empty() {
        return Err("market.name must be non-empty".into());
    }
    if m.stock_prec > MAX_SCALE {
        return Err(format!(
            "market.stock_prec ({}) exceeds rust_decimal::Decimal::MAX_SCALE ({})",
            m.stock_prec, MAX_SCALE
        ));
    }
    if m.money_prec > MAX_SCALE {
        return Err(format!(
            "market.money_prec ({}) exceeds Decimal::MAX_SCALE ({})",
            m.money_prec, MAX_SCALE
        ));
    }
    if m.fee_prec > MAX_SCALE {
        return Err(format!(
            "market.fee_prec ({}) exceeds Decimal::MAX_SCALE ({})",
            m.fee_prec, MAX_SCALE
        ));
    }
    if m.stock_prec > m.money_prec {
        log::warn!(
            "market.stock_prec ({}) > money_prec ({}): price rescale uses money_prec.saturating_sub(stock_prec) (=0); ensure this matches your market rules",
            m.stock_prec,
            m.money_prec
        );
    }

    let mut min_amount = Decimal::from_str(&m.min_amount)
        .map_err(|e| format!("market.min_amount is not a valid decimal: {}", e))?;
    if min_amount <= Decimal::ZERO {
        return Err("market.min_amount must be > 0".into());
    }
    min_amount.rescale(m.stock_prec);
    if min_amount <= Decimal::ZERO {
        return Err(
            "market.min_amount rescale to stock_prec rounds to zero; raise min_amount or lower stock_prec"
                .into(),
        );
    }

    Ok(())
}
