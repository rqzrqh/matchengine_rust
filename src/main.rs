use std::{thread, net::SocketAddr, str, sync::mpsc, fs::File, io::prelude::*, env, process, time::Duration};
use rdkafka::consumer::{BaseConsumer, StreamConsumer, CommitMode};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::consumer::Consumer;
use rdkafka::config::RDKafkaLogLevel;

use hyper::{Body, Response, Server, Error};
use hyper::service::{make_service_fn, service_fn};
use rust_decimal::prelude::*;
use mysql::*;
use chrono::prelude::*;
use tokio::sync::oneshot;
extern crate pretty_env_logger;
#[macro_use] extern crate log;
extern crate process_lock;
use process_lock::*;

mod error;
mod engine;
mod market;
mod publish;
mod dump;
mod load;
mod mainprocess;
mod httpserver;
mod task;

#[tokio::main]
async fn start_httpserver(main_routine_sender: mpsc::Sender<task::Task>) {
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));

    let make_svc = make_service_fn(move |_| {

        let main_routine_sender1 = main_routine_sender.clone();

        async move {

            Ok::<_, Error>(service_fn(move |req| {

                let main_routine_sender2 = main_routine_sender1.clone();

                async move {

                    let (tx, rx) = oneshot::channel();

                    let msg = hyper::body::to_bytes(req).await.unwrap();
                    let str = String::from_utf8(msg.to_vec()).unwrap();

                    let task = task::HttpQueryTask {
                        content: str,
                        rsp: tx,
                    };

                    main_routine_sender2.send(task::Task::QueryTask(task)).expect("send httptask failed");

                    let res = rx.await;

                    Ok::<_, Error>(Response::new(Body::from(res.unwrap())))
                }
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_svc);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

fn main() {

    println!("#####matchengine start####");

    pretty_env_logger::init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("args error");
        process::exit(0);
    }

    let mut file = File::open(&args[1]).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    let cfg = json::parse(&contents).unwrap();

    let market_name = cfg["market"]["name"].to_string();
    let stock_prec = cfg["market"]["stock_prec"].as_u32().unwrap();
    let money_prec = cfg["market"]["money_prec"].as_u32().unwrap();
    let fee_prec = cfg["market"]["fee_prec"].as_u32().unwrap();
    // TODO consider prec relationship
    let min_amount = Decimal::from_str(cfg["market"]["min_amount"].as_str().unwrap()).unwrap();

    let brokers = cfg["brokers"].to_string();
    let db_addr = cfg["db"]["addr"].to_string();
    let db_user = cfg["db"]["user"].to_string();
    let db_passwd = cfg["db"]["passwd"].to_string();

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

    let (main_routine_sender, main_routine_receiver) = mpsc::channel();

    let main_routine_sender_http_clone = main_routine_sender.clone();
    let http_thread = thread::spawn(|| {
        start_httpserver(main_routine_sender_http_clone);
    });

    let main_routine_sender_mq_clone = main_routine_sender.clone();
    let brokers_clone2 = brokers.clone();
    let input_offset = mk.input_offset;

    let consumer_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)// one thread
        .enable_all()
        .build()
        .unwrap();

    consumer_rt.spawn(async move {

        let consumer:StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers_clone2)
            .set("enable.partition.eof", "false")
            .set("group.id", "my_consumer_group2")
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            .set("enable.auto.offset.store", "false")
            .set_log_level(RDKafkaLogLevel::Debug)
            // We'll give each session its own (unique) consumer group id,
            // so that each session will receive all messages
            //.set("group.id", format!("chat-{}", Uuid::new_v4()))
            .create()
            .expect("Failed to create client");

        let mut tpl = TopicPartitionList::new();
        //
        // tpl.add_partition_offset(&topic_name, 0, Offset::Beginning)
        //     .unwrap();
        // tpl.add_partition_offset(&topic_name, 1, Offset::End)
        //     .unwrap();
        //

        tpl.add_partition_offset(&input_topic, 0, Offset::Offset(input_offset+1)).unwrap();
        //consumer.seek_partitions(tpl, None).expect("failed to seek_partitions"); // not work

        // update session commit offset to replay from specific offset
        consumer.commit(&tpl, CommitMode::Sync).expect("failed to commit");

        consumer.subscribe(&[&input_topic]).expect("failed to subscribe");

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

                    // should decode data here

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

    let main_routine_sender_timer_clone = main_routine_sender.clone();
    let timer_thread = thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(600));
            let now = Utc::now();
            let task = task::SqlDumpTask{
                tm: now.timestamp(),
            };
            main_routine_sender_timer_clone.send(task::Task::DumpTask(task)).expect("send dumptask failed");
        }
    });

    let publisher = publish::Publish::new(brokers, main_routine_sender.clone(), mk.quote_deals_id, mk.settle_message_ids.to_vec());

    loop {
        let task = main_routine_receiver.recv().unwrap();

        match task {
            task::Task::MqTask(t) => {
                mainprocess::handle_mq_message(&publisher, &mut mk, t.offset, &t.data);
            },
            task::Task::QueryTask(a) => {
                httpserver::handle_http_request(&mk, &a.content, a.rsp);
            },
            task::Task::DumpTask(b) => {
                dump::handle_dump(&mut mk, b.tm, &pool);
            },
            task::Task::ProgressUpdateTask(t) => {
                mainprocess::update_output_progress(&mut mk, t.quote_deals_id, t.settle_message_ids);
            }
            task::Task::Terminate => {
                warn!("terminate and exit");
                break;
            },
        }
    }

    let _ = timer_thread.join();
    let _ = http_thread.join();
}
