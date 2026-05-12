use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::{env, net::SocketAddr, process, thread, time::Duration};
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
mod offer_metrics;
mod payload_encoding;
mod publish;
mod snap_cleanup;
mod task;

mod config;

// Runtime layout (one OS process, one market). Profiler-visible thread prefixes: matcher main (unnamed OS main),
// `snap-cleanup`, `snap-timer`, `http-driver` + `http-worker`, `offer-consumer`, `quote-pub-1of1`,
// configured `settle-pub-<n>of<count>` worker threads (Tokio appends `-N` suffixes to the HTTP/consumer runtime threads).
// - Main thread: blocking loop on `main_routine_receiver`; owns `Market`, handles Kafka input, REST replies,
//   snapshot dumps, and publish progress updates.
// - HTTP (`http-driver` + nested Tokio `http-worker*`): Axum; forwards `HttpRequest` tasks to the bounded main channel.
// - Kafka consumer: Tokio `offer-consumer*` (single worker) on `offer.<market>`; unpacks MessagePack + RPC
//   envelope checks via `parse_mq_payload`, then sends `MqTask` to the bounded main channel (matcher + sequence checks stay main-thread).
// - Timer `snap-timer`: periodic `DumpTask` to fork snapshot writers.
// - Snapshot cleanup `snap-cleanup`: periodic `prune_snapshots` against MySQL.

async fn start_httpserver(
    main_routine_sender: task::MainTaskSender,
    addr: SocketAddr,
    ready_tx: Option<std::sync::mpsc::Sender<()>>,
) {
    http::serve_engine_http(
        addr,
        http::EngineHttpState {
            main_routine_sender,
        },
        ready_tx,
    )
    .await;
}

fn init_logging() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
}

fn kafka_partition_count(brokers: &str, topic: &str) -> Result<usize, String> {
    let client_id = format!("matchengine.startup-check.{}", topic);
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("client.id", client_id)
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
    settle_publish_thread_count: usize,
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
    if settle_partitions % settle_publish_thread_count != 0 {
        return Err(format!(
            "Kafka topic settle has {} partition(s), which must be an integer multiple of output_publish.settle.thread_count ({})",
            settle_partitions, settle_publish_thread_count
        ));
    }

    Ok(())
}

fn main() {
    init_logging();

    let args: Vec<String> = env::args().collect();
    let mut cfg_path = "config.yaml";
    let mut wait_initial_status = false;
    for arg in args.iter().skip(1) {
        match arg.as_str() {
            "--wait-initial-status" => wait_initial_status = true,
            "-h" | "--help" => {
                eprintln!("Usage: {} [--wait-initial-status] [CONFIG_PATH]", args[0]);
                process::exit(0);
            }
            value if value.starts_with('-') => {
                error!("unknown argument: {}", value);
                process::exit(1);
            }
            value => cfg_path = value,
        }
    }

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
    let main_task_queue_capacity = cfg.main_task_queue_capacity;
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
    if market::USER_SETTLE_GROUP_SIZE % output_publish_cfg.settle.thread_count != 0 {
        error!(
            "config invalid: user settle group size ({}) must be an integer multiple of output_publish.settle.thread_count ({})",
            market::USER_SETTLE_GROUP_SIZE,
            output_publish_cfg.settle.thread_count
        );
        process::exit(1);
    }

    if let Err(e) = validate_kafka_topics(
        &brokers,
        &input_topic,
        &quote_topic,
        market::USER_SETTLE_GROUP_SIZE,
        output_publish_cfg.settle.thread_count,
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

    let (main_routine_sender, main_routine_receiver) =
        task::MainTaskSender::channel(main_task_queue_capacity);

    let publisher = publish::Publish::new(
        brokers.clone(),
        market_name.clone(),
        main_routine_sender.clone(),
        mk.pushed_quote_deals_id,
        mk.pushed_settle_message_ids.to_vec(),
        publish::PublishDriverCfg {
            quote_topic: quote_topic.clone(),
            quote: publish::KafkaProducerDriverCfg {
                batch_num_messages: output_publish_cfg.quote.kafka.batch_num_messages,
                linger_ms: output_publish_cfg.quote.kafka.linger_ms,
                max_in_flight_requests_per_connection: output_publish_cfg
                    .quote
                    .kafka
                    .max_in_flight_requests_per_connection,
                queue_buffering_max_messages: output_publish_cfg
                    .quote
                    .kafka
                    .queue_buffering_max_messages,
                queue_buffering_max_kbytes: output_publish_cfg
                    .quote
                    .kafka
                    .queue_buffering_max_kbytes,
                compression_type: output_publish_cfg.quote.kafka.compression_type.clone(),
                delivery_timeout_ms: output_publish_cfg.quote.kafka.delivery_timeout_ms,
                statistics_interval_ms: output_publish_cfg.quote.kafka.statistics_interval_ms,
            },
            quote_channel_capacity: output_publish_cfg.quote.channel_capacity,
            quote_drain_batch_size: output_publish_cfg.quote.drain_batch_size,
            quote_max_outstanding: output_publish_cfg.quote.max_outstanding,
            settle: publish::KafkaProducerDriverCfg {
                batch_num_messages: output_publish_cfg.settle.kafka.batch_num_messages,
                linger_ms: output_publish_cfg.settle.kafka.linger_ms,
                max_in_flight_requests_per_connection: output_publish_cfg
                    .settle
                    .kafka
                    .max_in_flight_requests_per_connection,
                queue_buffering_max_messages: output_publish_cfg
                    .settle
                    .kafka
                    .queue_buffering_max_messages,
                queue_buffering_max_kbytes: output_publish_cfg
                    .settle
                    .kafka
                    .queue_buffering_max_kbytes,
                compression_type: output_publish_cfg.settle.kafka.compression_type.clone(),
                delivery_timeout_ms: output_publish_cfg.settle.kafka.delivery_timeout_ms,
                statistics_interval_ms: output_publish_cfg.settle.kafka.statistics_interval_ms,
            },
            settle_channel_capacity: output_publish_cfg.settle.channel_capacity,
            settle_drain_batch_size: output_publish_cfg.settle.drain_batch_size,
            settle_max_outstanding: output_publish_cfg.settle.max_outstanding,
            settle_worker_max_outstanding: output_publish_cfg.settle.worker_max_outstanding,
            settle_per_group_send_burst: output_publish_cfg.settle.per_group_send_burst,
            settle_thread_count: output_publish_cfg.settle.thread_count,
            progress_flush_interval: Duration::from_millis(
                output_publish_cfg.progress_flush_interval_ms,
            ),
        },
    );

    let offer_consumer_stats = offer_metrics::OfferConsumerStats::new();
    let (http_ready_sender, http_ready_receiver) = if wait_initial_status {
        let (sender, receiver) = std::sync::mpsc::channel();
        (Some(sender), Some(receiver))
    } else {
        (None, None)
    };
    let main_routine_sender_http_clone = main_routine_sender.clone();
    let http_thread = thread::Builder::new()
        .name("http-driver".to_owned())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("http-worker")
                .build()
                .expect("http tokio runtime");
            rt.block_on(start_httpserver(
                main_routine_sender_http_clone,
                http_addr,
                http_ready_sender,
            ));
        })
        .expect("spawn http thread");

    if wait_initial_status {
        let http_ready_receiver = http_ready_receiver.expect("http ready receiver");
        http_ready_receiver
            .recv()
            .expect("http server ready signal failed");
        match main_routine_receiver.recv_timeout(Duration::from_secs(5)) {
            Ok(task) => {
                main_routine_sender.mark_dequeued(&task);
                match task {
                    task::Task::HttpRequest(a) => {
                        let publish_snapshot = publisher.status_snapshot();
                        let main_task_queue_snapshot = main_routine_sender.status_snapshot();
                        let offer_consumer_snapshot = offer_consumer_stats.snapshot();
                        let res = http::handle_http_request(
                            &mk,
                            &publish_snapshot,
                            &main_task_queue_snapshot,
                            &offer_consumer_snapshot,
                            a.op,
                        );
                        let _ = a.rsp.send(res);
                    }
                    other => {
                        warn!("unexpected task before profiling initial status");
                        drop(other);
                    }
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                warn!("timed out waiting for profiling initial status request");
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("main task channel disconnected before startup")
            }
        }
    }

    let main_routine_sender_mq_clone = main_routine_sender.clone();
    let brokers_clone2 = brokers.clone();
    let input_offset = mk.input_offset;
    let offer_consumer_stats_for_consumer = offer_consumer_stats.clone();

    let consumer_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1) // consumer loop only
        // Stable name for profilers (Instruments / sample): distinguishes this from HTTP `tokio-runtime-worker`.
        .thread_name("offer-consumer")
        .enable_all()
        .build()
        .unwrap();

    let consumer_handler = consumer_rt.spawn(async move {
        let client_id = format!("matchengine.{}.offer-consumer", market_name);
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers_clone2)
            .set("client.id", client_id)
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
            let recv_started = std::time::Instant::now();
            match consumer.recv().await {
                Ok(message) => {
                    let recv_wait = recv_started.elapsed();
                    let data = message.payload();
                    debug!(
                        "Received message at topic: {} partition: {} offset: {} payload_len: {:?}",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        data.map(|p| p.len())
                    );

                    let payload_bytes = data.map(|p| p.len()).unwrap_or(0);
                    let parse_started = std::time::Instant::now();
                    let payload = data
                        .map(mainprocess::parse_mq_payload)
                        .unwrap_or_else(|| Err("empty mq payload".to_owned()));
                    let parse_elapsed = parse_started.elapsed();
                    let task = task::KafkaMqTask {
                        offset: message.offset(),
                        payload,
                    };
                    let send_started = std::time::Instant::now();
                    main_routine_sender_mq_clone
                        .send(task::Task::MqTask(task))
                        .expect("send mqtask failed");
                    offer_consumer_stats_for_consumer.record_message(
                        payload_bytes,
                        recv_wait,
                        parse_elapsed,
                        send_started.elapsed(),
                    );
                }
                Err(e) => {
                    offer_consumer_stats_for_consumer.record_recv_error(recv_started.elapsed());
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
        main_routine_sender.mark_dequeued(&task);

        match task {
            task::Task::MqTask(t) => {
                mainprocess::handle_mq_message(&publisher, &mut mk, t);
            }
            task::Task::HttpRequest(a) => {
                // HTTP never reads `Market` or publish-worker state directly. The
                // main thread owns `Market`, and publish exposes only atomic snapshots.
                let publish_snapshot = publisher.status_snapshot();
                let main_task_queue_snapshot = main_routine_sender.status_snapshot();
                let offer_consumer_snapshot = offer_consumer_stats.snapshot();
                let res = http::handle_http_request(
                    &mk,
                    &publish_snapshot,
                    &main_task_queue_snapshot,
                    &offer_consumer_snapshot,
                    a.op,
                );
                let _ = a.rsp.send(res);
            }
            task::Task::DumpTask(b) => {
                dump::handle_dump(&mut mk, b.tm, url.as_str());
            }
            task::Task::QuoteProgressUpdateTask(t) => {
                mainprocess::update_quote_progress(&mut mk, t.pushed_quote_deals_id);
            }
            task::Task::SettleProgressBatchUpdateTask(t) => {
                mainprocess::update_settle_progress_batch(&mut mk, &t.progresses);
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
