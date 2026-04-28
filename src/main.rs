use std::{thread, net::SocketAddr, str, sync::mpsc, env, process, time::Duration};
use rdkafka::consumer::{BaseConsumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use rdkafka::consumer::Consumer;
use rdkafka::config::RDKafkaLogLevel;
use uuid::{uuid, Uuid};

use rust_decimal::prelude::*;
use mysql::*;
use chrono::prelude::*;
extern crate pretty_env_logger;
#[macro_use] extern crate log;
extern crate process_lock;
use process_lock::*;

mod config;
mod decimal_util;
mod error;
mod engine;
mod market;
mod publish;
mod dump;
mod correct_snap;
mod snap_cleanup;
mod load;
mod mainprocess;
mod http;
mod task;

// Runtime layout (one OS process, one market):
// - Main thread: blocking loop on `main_routine_receiver`; owns `Market`, handles Kafka input, REST replies,
//   snapshot dumps, and publish progress updates.
// - HTTP thread: nested Tokio runtime running Axum; forwards `HttpRequest` tasks to the main channel.
// - Kafka consumer: Tokio runtime (single worker) on `offer.<market>`; sends `MqTask` to the main channel.
// - Timer thread: periodic `DumpTask` to fork snapshot writers.
// - Snapshot cleanup thread: periodic `prune_snapshots` against MySQL.

async fn start_httpserver(
    main_routine_sender: mpsc::Sender<task::Task>,
    market_name: String,
    publish_backlog: std::sync::Arc<publish::PublishBacklog>,
    addr: SocketAddr,
) {
    http::serve_engine_http(
        addr,
        http::EngineHttpState {
            main_routine_sender,
            market_name,
            publish_backlog,
        },
    )
    .await;
}

fn main() {

    pretty_env_logger::init();

    let args: Vec<String> = env::args().collect();
    let cfg_path = args.get(1).map(|s| s.as_str()).unwrap_or("config.yaml");

    let cfg = config::load_config(cfg_path).unwrap_or_else(|e| {
        eprintln!("config load failed: {}", e);
        process::exit(1);
    });

    if let Err(e) = config::validate_config(&cfg) {
        eprintln!("config invalid: {}", e);
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

    println!(
        "Matching engine REST base: http://{}:{}/markets/<market>/…",
        ENGINE_HTTP_IP, ENGINE_HTTP_PORT
    );

    let input_topic = format!("offer.{}", market_name);

    let mut lock = ProcessLock::new(String::from(format!("matchengine.{}", market_name)), None).unwrap();    
    let guard = lock.trylock().unwrap_or_else(|e| {
        error!("get process lock failed {}", e.to_string());
        process::exit(0);
    });

    if guard.is_none() {
        error!("get process lock failed");
        process::exit(0);
    }

    let mut mk = market::Market::new(&market_name, stock_prec, money_prec, fee_prec, &min_amount);

    let url = format!("mysql://{}:{}@{}/{}", db_user, db_passwd, db_addr, market_name);
    let ops = Opts::from_url(url.as_str()).unwrap();
    let pool = Pool::new(ops).unwrap();

    load::restore_state(&mut mk, &pool);

    let pool_snap_cleanup = pool.clone();
    let snap_cleanup_cfg = cfg.snap_cleanup.clone();
    let _snap_cleanup_thread = thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(snap_cleanup_cfg.cleanup_interval_secs.max(1)));
        snap_cleanup::prune_snapshots(
            &pool_snap_cleanup,
            snap_cleanup_cfg.max_age_secs,
            snap_cleanup_cfg.max_snapshots,
        );
    });

    let (main_routine_sender, main_routine_receiver) = mpsc::channel();

    let publisher = publish::Publish::new(
        brokers.clone(),
        main_routine_sender.clone(),
        mk.quote_deals_id,
        mk.settle_message_ids.to_vec(),
        output_publish_cfg.batch_size,
        output_publish_cfg.linger_ms,
    );

    let main_routine_sender_http_clone = main_routine_sender.clone();
    let market_name_http = market_name.clone();
    let publish_backlog_http = publisher.backlog();
    let http_thread = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(start_httpserver(
            main_routine_sender_http_clone,
            market_name_http,
            publish_backlog_http,
            http_addr,
        ));
    });

    let main_routine_sender_mq_clone = main_routine_sender.clone();
    let brokers_clone2 = brokers.clone();
    let input_offset = mk.input_offset;

    let consumer_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1) // consumer loop only
        .enable_all()
        .build()
        .unwrap();

    let consumer_handler = consumer_rt.spawn(async move {

        let consumer:StreamConsumer = ClientConfig::new()
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

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&input_topic, 0, Offset::Offset(input_offset + 1))
            .unwrap();
        consumer.assign(&tpl).expect("failed to assign");

        loop {

            match consumer.recv().await {
                Ok(message) => {
                    println!(
                        "Received message at topic: {} partition: {} offset: {} payload: {:?}",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        message.payload().map(|p| String::from_utf8_lossy(p).into_owned())
                    );

                    let task = task::KafkaMqTask {
                            offset: message.offset(),
                            data: message.payload().map(|p| String::from_utf8_lossy(p).to_string()).unwrap(),
                    };
                    main_routine_sender_mq_clone.send(task::Task::MqTask(task)).expect("send mqtask failed");

                }
                Err(e) => {
                    eprintln!("consume failed {}", e);
                }
            }
        }
    });

    let dump_every_secs = cfg.snap_dump.dump_interval_secs.max(1);
    let main_routine_sender_timer_clone = main_routine_sender.clone();
    let timer_thread = thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(dump_every_secs));
            let now = Utc::now();
            let task = task::SqlDumpTask{
                tm: now.timestamp(),
            };
            main_routine_sender_timer_clone.send(task::Task::DumpTask(task)).expect("send dumptask failed");
        }
    });

    loop {
        let task = main_routine_receiver.recv().unwrap();

        match task {
            task::Task::MqTask(t) => {
                mainprocess::handle_mq_message(&publisher, &mut mk, t.offset, &t.data);
            },
            task::Task::HttpRequest(a) => {
                let res = http::handle_http_request(&mk, a.op);
                let _ = a.rsp.send(res);
            },
            task::Task::DumpTask(b) => {
                dump::handle_dump(&mut mk, b.tm, url.as_str());
            },
            task::Task::QuoteProgressUpdateTask(t) => {
                mainprocess::update_quote_progress(&mut mk, t.quote_deals_id);
            }
            task::Task::SettleProgressUpdateTask(t) => {
                mainprocess::update_settle_progress(&mut mk, t.group_id, t.message_id);
            }
            task::Task::Terminate => {
                warn!("terminate and exit");
                break;
            },
        }
    }

    //consumer_handler.await.unwrap();
    let _ = timer_thread.join();
    let _ = http_thread.join();
}
