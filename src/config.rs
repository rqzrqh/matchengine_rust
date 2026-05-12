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

fn default_main_task_queue_capacity() -> usize {
    65_536
}

impl Default for SnapDumpCfg {
    fn default() -> Self {
        Self {
            dump_interval_secs: default_snap_dump_interval_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaProducerCfg {
    pub batch_num_messages: usize,
    pub linger_ms: u64,
    pub max_in_flight_requests_per_connection: u32,
    #[serde(default = "default_queue_buffering_max_messages")]
    pub queue_buffering_max_messages: usize,
    #[serde(default = "default_queue_buffering_max_kbytes")]
    pub queue_buffering_max_kbytes: usize,
    #[serde(default = "default_compression_type")]
    pub compression_type: String,
    #[serde(default = "default_delivery_timeout_ms")]
    pub delivery_timeout_ms: u64,
    #[serde(default)]
    pub statistics_interval_ms: u64,
}

fn default_queue_buffering_max_messages() -> usize {
    100_000
}

fn default_queue_buffering_max_kbytes() -> usize {
    1_048_576
}

fn default_compression_type() -> String {
    "none".to_owned()
}

fn default_delivery_timeout_ms() -> u64 {
    5_000
}

fn default_settle_per_group_send_burst() -> usize {
    usize::MAX
}

fn default_settle_worker_max_outstanding() -> usize {
    32_768
}

fn default_output_publish_progress_flush_interval_ms() -> u64 {
    1_000
}

/// `quote_deals.<market>` producer (`output_publish.quote` in YAML).
#[derive(Debug, Clone, Deserialize)]
pub struct QuotePublishCfg {
    pub kafka: KafkaProducerCfg,
    pub channel_capacity: usize,
    pub drain_batch_size: usize,
    pub max_outstanding: usize,
}

/// `settle` topic producer (`output_publish.settle` in YAML).
#[derive(Debug, Clone, Deserialize)]
pub struct SettlePublishCfg {
    pub kafka: KafkaProducerCfg,
    pub channel_capacity: usize,
    pub drain_batch_size: usize,
    pub max_outstanding: usize,
    #[serde(default = "default_settle_worker_max_outstanding")]
    pub worker_max_outstanding: usize,
    #[serde(default = "default_settle_per_group_send_burst")]
    pub per_group_send_burst: usize,
    pub thread_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OutputPublishCfg {
    pub quote: QuotePublishCfg,
    pub settle: SettlePublishCfg,
    #[serde(default = "default_output_publish_progress_flush_interval_ms")]
    pub progress_flush_interval_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub market: MarketCfg,
    pub brokers: String,
    pub db: DbCfg,
    #[serde(default = "default_main_task_queue_capacity")]
    pub main_task_queue_capacity: usize,
    #[serde(default)]
    pub snap_cleanup: SnapCleanupCfg,
    #[serde(default)]
    pub snap_dump: SnapDumpCfg,
    pub output_publish: OutputPublishCfg,
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

fn validate_kafka_producer_cfg(prefix: &str, cfg: &KafkaProducerCfg) -> Result<(), String> {
    if cfg.batch_num_messages == 0 {
        return Err(format!("{}.batch_num_messages must be > 0", prefix));
    }
    if cfg.max_in_flight_requests_per_connection < 1 {
        return Err(format!(
            "{}.max_in_flight_requests_per_connection must be >= 1",
            prefix
        ));
    }
    if cfg.queue_buffering_max_messages == 0 {
        return Err(format!(
            "{}.queue_buffering_max_messages must be > 0",
            prefix
        ));
    }
    if cfg.queue_buffering_max_kbytes == 0 {
        return Err(format!("{}.queue_buffering_max_kbytes must be > 0", prefix));
    }
    if cfg.delivery_timeout_ms == 0 {
        return Err(format!("{}.delivery_timeout_ms must be > 0", prefix));
    }

    match cfg.compression_type.as_str() {
        "none" | "gzip" | "snappy" | "lz4" | "zstd" => Ok(()),
        _ => Err(format!(
            "{}.compression_type must be one of none, gzip, snappy, lz4, zstd",
            prefix
        )),
    }
}

/// Fail fast before the matcher runs: precision fields must fit `rust_decimal` and relate consistently.
///
/// This engine keeps the precision model intentionally simple: price-related rescaling
/// derives from `money_prec - stock_prec`, so startup rejects markets where
/// `stock_prec > money_prec` instead of supporting more complex mixed-precision rules.
pub fn validate_config(cfg: &Config) -> Result<(), String> {
    const MAX_SCALE: u32 = Decimal::MAX_SCALE;

    if cfg.brokers.trim().is_empty() {
        return Err("brokers must be non-empty".into());
    }
    if cfg.db.addr.trim().is_empty() || cfg.db.user.trim().is_empty() {
        return Err("db.addr and db.user must be non-empty".into());
    }
    if cfg.main_task_queue_capacity == 0 {
        return Err("main_task_queue_capacity must be > 0".into());
    }
    let q = &cfg.output_publish.quote;
    validate_kafka_producer_cfg("output_publish.quote.kafka", &q.kafka)?;
    if q.channel_capacity == 0 {
        return Err("output_publish.quote.channel_capacity must be > 0".into());
    }
    if q.drain_batch_size == 0 {
        return Err("output_publish.quote.drain_batch_size must be > 0".into());
    }
    if q.max_outstanding == 0 {
        return Err("output_publish.quote.max_outstanding must be > 0".into());
    }
    let s = &cfg.output_publish.settle;
    validate_kafka_producer_cfg("output_publish.settle.kafka", &s.kafka)?;
    if s.channel_capacity == 0 {
        return Err("output_publish.settle.channel_capacity must be > 0".into());
    }
    if s.drain_batch_size == 0 {
        return Err("output_publish.settle.drain_batch_size must be > 0".into());
    }
    if s.max_outstanding == 0 {
        return Err("output_publish.settle.max_outstanding must be > 0".into());
    }
    if s.worker_max_outstanding == 0 {
        return Err("output_publish.settle.worker_max_outstanding must be > 0".into());
    }
    if s.per_group_send_burst == 0 {
        return Err("output_publish.settle.per_group_send_burst must be > 0".into());
    }
    if s.kafka.max_in_flight_requests_per_connection > 5 {
        return Err(
            "output_publish.settle: Kafka idempotent producer allows at most max.in.flight=5; lower settle.kafka.max_in_flight_requests_per_connection".into(),
        );
    }
    if s.thread_count == 0 {
        return Err("output_publish.settle.thread_count must be > 0".into());
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
        return Err(format!(
            "market.stock_prec ({}) must be <= market.money_prec ({}) to keep price rescaling simple in this engine",
            m.stock_prec, m.money_prec
        ));
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
