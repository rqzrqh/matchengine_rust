use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::{env, net::SocketAddr, process, str, sync::mpsc, thread, time::Duration};
use uuid::Uuid;

use chrono::prelude::*;
use mysql::*;
use rust_decimal::prelude::*;
#[macro_use]
extern crate log;
extern crate process_lock;
use process_lock::*;

mod correct_snap;
mod decimal_util;
mod dump;
mod engine;
mod error;
mod http;
mod load;
mod mainprocess;
mod market;
mod payload_encoding;
mod publish;
mod snap_cleanup;
mod task;

mod config;

// Runtime layout (one OS process, one market). Profiler-visible thread prefixes: matcher main (unnamed OS main),
// `snap-cleanup`, `snap-timer`, `http-driver` + `http-worker`, `kafka-consumer`, `quote-publish` + `quote-pub-io`,
// `settle-publish` + `settle-pub-io` (Tokio appends `-N` suffixes).
// - Main thread: blocking loop on `main_routine_receiver`; owns `Market`, handles Kafka input, REST replies,
//   snapshot dumps, and publish progress updates.
// - HTTP (`http-driver` + nested Tokio `http-worker*`): Axum; forwards `HttpRequest` tasks to the main channel.
// - Kafka consumer: Tokio `kafka-consumer*` (single worker) on `offer.<market>`; runs `json::parse` + RPC
//   envelope checks via `parse_mq_payload`, then sends `MqTask` to the main channel (matcher + sequence checks stay main-thread).
// - Timer `snap-timer`: periodic `DumpTask` to fork snapshot writers.
// - Snapshot cleanup `snap-cleanup`: periodic `prune_snapshots` against MySQL.

async fn start_httpserver(main_routine_sender: mpsc::Sender<task::Task>, addr: SocketAddr) {
    http::serve_engine_http(
        addr,
        http::EngineHttpState {
            main_routine_sender,
        },
    )
    .await;
}

fn init_logging() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
}

fn kafka_partition_count(brokers: &str, topic: &str) -> Result<usize, String> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set(
            "group.id",
            format!("match-startup-check-{}", Uuid::new_v4()),
        )
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|e| format!("create Kafka metadata client failed: {}", e))?;

    let metadata = consumer
        .fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(5)))
        .map_err(|e| format!("fetch Kafka metadata for topic {} failed: {}", topic, e))?;

    let topic_meta = metadata
        .topics()
        .iter()
        .find(|x| x.name() == topic)
        .ok_or_else(|| format!("Kafka topic {} not found", topic))?;

    if let Some(e) = topic_meta.error() {
        return Err(format!("Kafka topic {} metadata error: {:?}", topic, e));
    }

    Ok(topic_meta.partitions().len())
}

fn validate_kafka_topics(
    brokers: &str,
    input_topic: &str,
    quote_topic: &str,
    settle_group_count: usize,
) -> Result<(), String> {
    let input_partitions = kafka_partition_count(brokers, input_topic)?;
    if input_partitions == 0 {
        return Err(format!("Kafka topic {} has no partitions", input_topic));
    }

    let quote_partitions = kafka_partition_count(brokers, quote_topic)?;
    if quote_partitions == 0 {
        return Err(format!("Kafka topic {} has no partitions", quote_topic));
    }

    let settle_partitions = kafka_partition_count(brokers, "settle")?;
    if settle_partitions < settle_group_count {
        return Err(format!(
            "Kafka topic settle has {} partition(s), but matcher requires at least {} because user settle group size is {}; recreate it with deploy/kafka_settle.sh",
            settle_partitions, settle_group_count, settle_group_count
        ));
    }

    Ok(())
}

fn main() {
    init_logging();

    let args: Vec<String> = env::args().collect();
    let cfg_path = args.get(1).map(|s| s.as_str()).unwrap_or("config.yaml");

    let cfg = config::load_config(cfg_path).unwrap_or_else(|e| {
        error!("config load failed: {} {}", cfg_path, e);
        process::exit(1);
    });

    if let Err(e) = config::validate_config(&cfg) {
        error!("config invalid: {}", e);
        process::exit(1);
    }

    let market_name = cfg.market.name.clone();
    let stock_prec = cfg.market.stock_prec;
    let money_prec = cfg.market.money_prec;
    let fee_prec = cfg.market.fee_prec;
    let min_amount = Decimal::from_str(&cfg.market.min_amount).unwrap();

    let brokers = cfg.brokers.clone();
    let db_addr = cfg.db.addr.clone();
    let db_user = cfg.db.user.clone();
    let db_passwd = cfg.db.passwd.clone();
    let output_publish_cfg = cfg.output_publish.clone();

    const ENGINE_HTTP_IP: &str = "127.0.0.1";
    const ENGINE_HTTP_PORT: u16 = 8080;
    let http_addr: SocketAddr = format!("{}:{}", ENGINE_HTTP_IP, ENGINE_HTTP_PORT)
        .parse()
        .expect("engine http bind addr");

    info!(
        "Matching engine REST base: http://{}:{}/markets/<market>/…",
        ENGINE_HTTP_IP, ENGINE_HTTP_PORT
    );

    let input_topic = format!("offer.{}", market_name);
    let quote_topic = format!("quote_deals.{}", market_name);

    if let Err(e) = validate_kafka_topics(
        &brokers,
        &input_topic,
        &quote_topic,
        market::USER_SETTLE_GROUP_SIZE,
    ) {
        error!("Kafka topic validation failed: {}", e);
        process::exit(1);
    }

    let mut lock =
        ProcessLock::new(String::from(format!("matchengine.{}", market_name)), None).unwrap();
    let guard = lock.trylock().unwrap_or_else(|e| {
        error!("get process lock failed {}", e.to_string());
        process::exit(0);
    });

    if guard.is_none() {
        error!("get process lock failed");
        process::exit(0);
    }

    let mut mk = market::Market::new(&market_name, stock_prec, money_prec, fee_prec, &min_amount);

    let url = format!(
        "mysql://{}:{}@{}/{}",
        db_user, db_passwd, db_addr, market_name
    );
    let ops = Opts::from_url(url.as_str()).unwrap();
    let pool = Pool::new(ops).unwrap();

    load::restore_state(&mut mk, &pool);

    let pool_snap_cleanup = pool.clone();
    let snap_cleanup_cfg = cfg.snap_cleanup.clone();
    let _snap_cleanup_thread = thread::Builder::new()
        .name("snap-cleanup".to_owned())
        .spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(
                    snap_cleanup_cfg.cleanup_interval_secs.max(1),
                ));
                snap_cleanup::prune_snapshots(
                    &pool_snap_cleanup,
                    snap_cleanup_cfg.max_age_secs,
                    snap_cleanup_cfg.max_snapshots,
                );
            }
        })
        .expect("spawn snap-cleanup thread");

    let (main_routine_sender, main_routine_receiver) = mpsc::channel();

    let publisher = publish::Publish::new(
        brokers.clone(),
        main_routine_sender.clone(),
        mk.pushed_quote_deals_id,
        mk.pushed_settle_message_ids.to_vec(),
        publish::PublishDriverCfg {
            quote_batch_size: output_publish_cfg.quote.batch_size,
            quote_linger_ms: output_publish_cfg.quote.linger_ms,
            quote_max_in_flight_requests_per_connection: output_publish_cfg
                .quote
                .max_in_flight_requests_per_connection,
            settle_batch_size: output_publish_cfg.settle.batch_size,
            settle_linger_ms: output_publish_cfg.settle.linger_ms,
            settle_max_in_flight_requests_per_connection: output_publish_cfg
                .settle
                .max_in_flight_requests_per_connection,
        },
    );

    let main_routine_sender_http_clone = main_routine_sender.clone();
    let http_thread = thread::Builder::new()
        .name("http-driver".to_owned())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("http-worker")
                .build()
                .expect("http tokio runtime");
            rt.block_on(start_httpserver(main_routine_sender_http_clone, http_addr));
        })
        .expect("spawn http thread");

    let main_routine_sender_mq_clone = main_routine_sender.clone();
    let brokers_clone2 = brokers.clone();
    let input_offset = mk.input_offset;

    let consumer_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1) // consumer loop only
        // Stable name for profilers (Instruments / sample): distinguishes this from HTTP `tokio-runtime-worker`.
        .thread_name("kafka-consumer")
        .enable_all()
        .build()
        .unwrap();

    let consumer_handler = consumer_rt.spawn(async move {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers_clone2)
            .set("enable.partition.eof", "false")
            // Unique group id per run so this process reads the full topic from the assigned offset.
            .set("group.id", format!("match-{}", Uuid::new_v4()))
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            .set("enable.auto.offset.store", "false")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .expect("Failed to create client");

        // `Market.input_offset` is the last Kafka offset consumed; assign partition 0 at `input_offset + 1`.
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&input_topic, 0, Offset::Offset(input_offset + 1))
            .unwrap();
        consumer.assign(&tpl).expect("failed to assign");

        loop {
            match consumer.recv().await {
                Ok(message) => {
                    debug!(
                        "Received message at topic: {} partition: {} offset: {} payload: {:?}",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        message
                            .payload()
                            .map(|p| String::from_utf8_lossy(p).into_owned())
                    );

                    let data = message
                        .payload()
                        .map(|p| String::from_utf8_lossy(p).to_string())
                        .unwrap();
                    let payload = mainprocess::parse_mq_payload(&data);
                    let task = task::KafkaMqTask {
                        offset: message.offset(),
                        payload,
                    };
                    main_routine_sender_mq_clone
                        .send(task::Task::MqTask(task))
                        .expect("send mqtask failed");
                }
                Err(e) => {
                    error!("consume failed {}", e);
                }
            }
        }
    });

    let dump_every_secs = cfg.snap_dump.dump_interval_secs.max(1);
    let main_routine_sender_timer_clone = main_routine_sender.clone();
    let timer_thread = thread::Builder::new()
        .name("snap-timer".to_owned())
        .spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(dump_every_secs));
                let now = Utc::now();
                let task = task::SqlDumpTask {
                    tm: now.timestamp(),
                };
                main_routine_sender_timer_clone
                    .send(task::Task::DumpTask(task))
                    .expect("send dumptask failed");
            }
        })
        .expect("spawn snap dump timer thread");

    loop {
        let task = main_routine_receiver.recv().unwrap();

        match task {
            task::Task::MqTask(t) => {
                mainprocess::handle_mq_message(&publisher, &mut mk, t);
            }
            task::Task::HttpRequest(a) => {
                let res = http::handle_http_request(&mk, a.op);
                let _ = a.rsp.send(res);
            }
            task::Task::DumpTask(b) => {
                dump::handle_dump(&mut mk, b.tm, url.as_str());
            }
            task::Task::QuoteProgressUpdateTask(t) => {
                mainprocess::update_quote_progress(&mut mk, t.pushed_quote_deals_id);
            }
            task::Task::SettleProgressUpdateTask(t) => {
                mainprocess::update_settle_progress(
                    &mut mk,
                    t.group_id,
                    t.pushed_settle_message_id,
                );
            }
            task::Task::Terminate => {
                warn!("terminate and exit");
                break;
            }
        }
    }

    //consumer_handler.await.unwrap();
    let _ = timer_thread.join();
    let _ = http_thread.join();
}
