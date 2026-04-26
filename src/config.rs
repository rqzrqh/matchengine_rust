use serde::Deserialize;

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
