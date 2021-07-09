use std::{thread, net::SocketAddr, str, sync::mpsc, fs::File, io::prelude::*, env, process, time::Duration};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::producer::{Producer, Record, RequiredAcks};

use hyper::{Body, Response, Server, Error};
use hyper::service::{make_service_fn, service_fn};
use rust_decimal::prelude::*;
use mysql::*;
use chrono::prelude::*;
use tokio::sync::oneshot;
extern crate pretty_env_logger;
#[macro_use] extern crate log;

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

    let mut mk = market::Market::new(&market_name, stock_prec, money_prec, fee_prec, &min_amount);

    let url = format!("mysql://{}:{}@{}/{}", db_user, db_passwd, db_addr, market_name);
    let ops = Opts::from_url(url.as_str()).unwrap();
    let pool = Pool::new(ops).unwrap();

    load::restore_state(&mut mk, &pool);

    let (main_routine_sender, main_routine_receiver) = mpsc::channel();
    let (publisher_sender, publisher_receiver) = mpsc::channel();

    let main_routine_sender_http_clone = main_routine_sender.clone();
    let http_thread = thread::spawn(|| {
        start_httpserver(main_routine_sender_http_clone);
    });

    let brokers_clone1 = brokers.clone();
    let mq_producer_thread = thread::spawn(move || {
        let mut producer = Producer::from_hosts(vec!(brokers_clone1.to_owned()))
                .with_ack_timeout(Duration::from_secs(1))
                .with_required_acks(RequiredAcks::One)
                .create()
                .unwrap();

        loop {
            let received:Box<publish::PublishInfo> = publisher_receiver.recv().unwrap();
            let topic = received.topic;
            let data = &received.data;
            producer.send(&Record::from_value(&topic, data.to_string().as_bytes())).unwrap();
        }
    });

    let main_routine_sender_mq_clone = main_routine_sender.clone();
    let brokers_clone2 = brokers.clone();
    let mq_consumer_thread = thread::spawn(move || {

        let mut consumer =
        Consumer::from_hosts(vec!(brokers_clone2.to_owned()))
            .with_topic_partitions(input_topic.to_owned(), &[0, 0])
            .with_fallback_offset(FetchOffset::Earliest) // TODO set offset
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap();
        loop {
            for ms in consumer.poll().unwrap().iter() {
                for m in ms.messages() {

                    let str = match str::from_utf8(m.value) {
                        Ok(v) => v,
                        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                    };

                    let task = task::KafkaMqTask {
                        offset: m.offset,
                        data: str.to_string(),
                    };

                    main_routine_sender_mq_clone.send(task::Task::MqTask(task)).expect("send mqtask failed");
                }
                consumer.consume_messageset(ms).unwrap_or_else(|e| panic!("{}", e.to_string()));
            }
            consumer.commit_consumed().unwrap_or_else(|e| panic!("{}", e.to_string()));
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

    let publisher = publish::Publish::new(publisher_sender.clone());

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
            task::Task::Terminate => {
                warn!("terminate and exit");
                break;
            },
        }
    }

    let _ = timer_thread.join();
    let _ = mq_consumer_thread.join();
    let _ = mq_producer_thread.join();
    let _ = http_thread.join();
}
