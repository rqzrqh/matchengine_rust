use crate::market::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
    thread,
    str,
};
use std::rc::Rc;
use rust_decimal::prelude::*;
use json::*;
use crate::task::*;

use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::{ config::ClientConfig };

pub struct Publish {
    quote_sender: mpsc::Sender<QuotePublishTaskInfo>,
    settle_sender: mpsc::Sender<SettlePublishTaskInfo>,
    backlog: Arc<PublishBacklog>,
    settle_group_count:u32,
}

pub struct PublishBacklog {
    quote_pending: AtomicUsize,
    settle_pending: AtomicUsize,
    settle_group_pending: Vec<AtomicUsize>,
}

pub struct PublishBacklogSnapshot {
    pub quote_pending: usize,
    pub settle_pending: usize,
    pub total_pending: usize,
    pub settle_group_pending: Vec<usize>,
}

impl PublishBacklog {
    fn new(settle_group_count: usize) -> Self {
        Self {
            quote_pending: AtomicUsize::new(0),
            settle_pending: AtomicUsize::new(0),
            settle_group_pending: (0..settle_group_count)
                .map(|_| AtomicUsize::new(0))
                .collect(),
        }
    }

    fn inc_quote(&self) {
        self.quote_pending.fetch_add(1, Ordering::Relaxed);
    }

    fn dec_quote(&self) {
        let prev = self.quote_pending.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(prev > 0, "quote backlog underflow");
    }

    fn inc_settle(&self, group_id: usize) {
        self.settle_pending.fetch_add(1, Ordering::Relaxed);
        self.settle_group_pending[group_id].fetch_add(1, Ordering::Relaxed);
    }

    fn dec_settle(&self, group_id: usize) {
        let total_prev = self.settle_pending.fetch_sub(1, Ordering::Relaxed);
        let group_prev = self.settle_group_pending[group_id].fetch_sub(1, Ordering::Relaxed);
        debug_assert!(total_prev > 0, "settle backlog underflow");
        debug_assert!(group_prev > 0, "settle group backlog underflow");
    }

    pub fn snapshot(&self) -> PublishBacklogSnapshot {
        let quote_pending = self.quote_pending.load(Ordering::Relaxed);
        let settle_pending = self.settle_pending.load(Ordering::Relaxed);
        let settle_group_pending = self
            .settle_group_pending
            .iter()
            .map(|x| x.load(Ordering::Relaxed))
            .collect::<Vec<_>>();

        PublishBacklogSnapshot {
            quote_pending,
            settle_pending,
            total_pending: quote_pending + settle_pending,
            settle_group_pending,
        }
    }
}

struct QuotePublishTaskInfo {
    deals_id:u64,
    topic:String,
    data:JsonValue,
}

struct SettlePublishTaskInfo {
    group_id:u32,
    message_id:u64,
    data:JsonValue,
}

fn collect_publish_batch<T>(receiver: &mpsc::Receiver<T>, batch_size: usize) -> Vec<T> {
    let mut batch = Vec::with_capacity(batch_size);
    batch.push(receiver.recv().unwrap());

    while batch.len() < batch_size {
        match receiver.try_recv() {
            Ok(task) => batch.push(task),
            Err(mpsc::TryRecvError::Empty) => break,
            Err(mpsc::TryRecvError::Disconnected) => panic!("publish task channel disconnected"),
        }
    }

    batch
}

fn enqueue_publish(
    producer: &FutureProducer,
    topic: &str,
    data: &JsonValue,
    ) -> DeliveryFuture {
    let payload = data.to_string();
    let record = FutureRecord::to(topic).payload(payload.as_bytes());
    producer
        .send_result::<Vec<u8>, _>(record)
        .unwrap_or_else(|(e, _)| panic!("publish enqueue failed topic={} error={}", topic, e))
}

fn enqueue_settle_publish(
    producer: &FutureProducer,
    partition: i32,
    data: &JsonValue,
) -> DeliveryFuture {
    let payload = data.to_string();
    let record = FutureRecord::to("settle")
        .partition(partition)
        .payload(payload.as_bytes());
    producer
        .send_result::<Vec<u8>, _>(record)
        .unwrap_or_else(|(e, _)| {
            panic!(
                "settle publish enqueue failed partition={} error={}",
                partition, e
            )
        })
}

fn build_kafka_producer(brokers: &str, batch_size: usize, linger_ms: u64) -> FutureProducer {
    let linger_ms_str = linger_ms.to_string();
    let batch_size_str = batch_size.to_string();

    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        // Allow duplicates, but keep strict per-topic ordering by sending at most one
        // in-flight request per connection.
        .set("max.in.flight.requests.per.connection", "1")
        .set("linger.ms", &linger_ms_str)
        .set("batch.num.messages", &batch_size_str)
        .create()
        .expect("Producer creation error")
}

fn spawn_quote_publish_thread(
    brokers: String,
    main_routine_sender: mpsc::Sender<Task>,
    quote_deals_id: u64,
    batch_size: usize,
    linger_ms: u64,
    receiver: mpsc::Receiver<QuotePublishTaskInfo>,
    backlog: Arc<PublishBacklog>,
) {
    thread::spawn(move || {
        let producer_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        producer_rt.block_on(async move {
            let producer = build_kafka_producer(&brokers, batch_size, linger_ms);
            let mut quote_deals_id = quote_deals_id;

            loop {
                let batch = collect_publish_batch(&receiver, batch_size);
                let batch_len = batch.len();
                let mut pending = Vec::with_capacity(batch_len);
                let mut next_quote_deals_id = quote_deals_id;

                for task in batch {
                    if task.deals_id > next_quote_deals_id {
                        pending.push((task.deals_id, enqueue_publish(&producer, &task.topic, &task.data)));
                        next_quote_deals_id = task.deals_id;
                    } else {
                        backlog.dec_quote();
                    }
                }

                if pending.len() > 1 {
                    info!(
                        "quote batch flush: queued={} delivered_after_ack={} linger_ms={}",
                        batch_len,
                        pending.len(),
                        linger_ms
                    );
                }

                for (deals_id, delivery) in pending {
                    match delivery.await {
                        Ok(Ok(_)) => {}
                        Ok(Err((e, _))) => panic!("quote publish delivery failed: {}", e),
                        Err(e) => panic!("quote publish delivery canceled: {}", e),
                    }

                    backlog.dec_quote();
                    quote_deals_id = deals_id;
                    let task = QuotePublishProgressTask { quote_deals_id };
                    main_routine_sender
                        .send(Task::QuoteProgressUpdateTask(task))
                        .expect("send quote progress update task failed");
                }
            }
        })
    });
}

fn spawn_settle_publish_thread(
    brokers: String,
    main_routine_sender: mpsc::Sender<Task>,
    settle_message_ids: Vec<u64>,
    batch_size: usize,
    linger_ms: u64,
    receiver: mpsc::Receiver<SettlePublishTaskInfo>,
    backlog: Arc<PublishBacklog>,
) {
    thread::spawn(move || {
        let producer_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        producer_rt.block_on(async move {
            let producer = build_kafka_producer(&brokers, batch_size, linger_ms);
            let mut settle_message_ids = settle_message_ids;

            loop {
                let batch = collect_publish_batch(&receiver, batch_size);
                let batch_len = batch.len();
                let mut pending = Vec::with_capacity(batch_len);
                let mut next_settle_message_ids = settle_message_ids.clone();

                for task in batch {
                    let group_id = task.group_id as usize;
                    if task.message_id > next_settle_message_ids[group_id] {
                        pending.push((
                            group_id,
                            task.message_id,
                            enqueue_settle_publish(&producer, task.group_id as i32, &task.data),
                        ));
                        next_settle_message_ids[group_id] = task.message_id;
                    } else {
                        backlog.dec_settle(group_id);
                    }
                }

                if pending.len() > 1 {
                    info!(
                        "settle batch flush: queued={} delivered_after_ack={} linger_ms={}",
                        batch_len,
                        pending.len(),
                        linger_ms
                    );
                }

                for (group_id, message_id, delivery) in pending {
                    match delivery.await {
                        Ok(Ok(_)) => {}
                        Ok(Err((e, _))) => panic!("settle publish delivery failed: {}", e),
                        Err(e) => panic!("settle publish delivery canceled: {}", e),
                    }

                    backlog.dec_settle(group_id);
                    settle_message_ids[group_id] = message_id;
                    let task = SettlePublishProgressTask { group_id, message_id };
                    main_routine_sender
                        .send(Task::SettleProgressUpdateTask(task))
                        .expect("send settle progress update task failed");
                }
            }
        })
    });
}

impl Publish {
    pub fn new(
        brokers: String,
        main_routine_sender: mpsc::Sender<Task>,
        quote_deals_id: u64,
        settle_message_ids: Vec<u64>,
        batch_size: usize,
        linger_ms: u64,
    ) -> Publish {
        let (quote_sender, quote_receiver) = mpsc::channel();
        let (settle_sender, settle_receiver) = mpsc::channel();
        let settle_group_count = settle_message_ids.len() as u32;
        let backlog = Arc::new(PublishBacklog::new(settle_group_count as usize));

        spawn_quote_publish_thread(
            brokers.clone(),
            main_routine_sender.clone(),
            quote_deals_id,
            batch_size,
            linger_ms,
            quote_receiver,
            backlog.clone(),
        );
        spawn_settle_publish_thread(
            brokers,
            main_routine_sender,
            settle_message_ids,
            batch_size,
            linger_ms,
            settle_receiver,
            backlog.clone(),
        );

        Publish {
            quote_sender,
            settle_sender,
            backlog,
            settle_group_count: settle_group_count,
        }
    }

    pub fn backlog(&self) -> Arc<PublishBacklog> {
        self.backlog.clone()
    }

    pub fn publish_put_order(&self, m: &Market, extern_id: u64, order: &Rc<Order>) {

        let mut object = JsonValue::new_object();
        object["type"] = "put_order".into();
        object["market"] = m.name.clone().into();
        object["msgid"] = m.message_id.into();
        object["txid"] = extern_id.into();
        object["order"] = order.to_json(m);

        let group_id = order.user_id%self.settle_group_count;

        debug!("settle partition={} {}", group_id, object);

        let message = SettlePublishTaskInfo {
            group_id:group_id,
            message_id:m.message_id,
            data: object,
        };

        self.backlog.inc_settle(group_id as usize);
        let res = self.settle_sender.send(message);
        match res {
            Ok(_) => {
            },
            Err(e) => {
                self.backlog.dec_settle(group_id as usize);
                panic!("publish_put_order failed.{}", e);
            }
        }
    }

    pub fn publish_cancel_order(&self, m: &Market, extern_id: u64, order: &Rc<Order>) {

        let mut object = JsonValue::new_object();
        object["type"] = "cancel_order".into();
        object["market"] = m.name.clone().into();
        object["msgid"] = m.message_id.into();
        object["txid"] = extern_id.into();
        object["order"] = order.to_json(m);

        let group_id = order.user_id%self.settle_group_count;

        debug!("settle partition={} {}", group_id, object);

        let message = SettlePublishTaskInfo {
            group_id:group_id,
            message_id:m.message_id,
            data: object,
        };

        self.backlog.inc_settle(group_id as usize);
        let res = self.settle_sender.send(message);
        match res {
            Ok(_) => {
            },
            Err(e) => {
                self.backlog.dec_settle(group_id as usize);
                panic!("publish_cancel_order failed.{}", e);
            }
        }
    }

    pub fn publish_deal(&self, m: &Market, extern_id: u64, tm: i64, user_id: u32, rival_user_id: u32, order_id: u64, role: u32,
            price: &Decimal, amount: &Decimal, deal: &Decimal, fee: &Decimal, rival_fee: &Decimal) {

        let mut object = JsonValue::new_object();
        object["type"] = "deals".into();
        object["market"] = m.name.clone().into();
        object["msgid"] = m.message_id.into();
        object["txid"] = extern_id.into();

        let mut deals = JsonValue::new_object();
        deals["time"] = tm.into();
        deals["user_id"] = user_id.into();
        deals["rival_user_id"] = rival_user_id.into();
        deals["order_id"] = order_id.into();
        deals["deal_id"] = m.deals_id.into();
        deals["role"] = role.into();

        deals["price"] = price.to_string().into();
        deals["amount"] = amount.to_string().into();
        deals["deal"] = deal.to_string().into();
        deals["fee"] = fee.to_string().into();
        deals["rival_fee"] = rival_fee.to_string().into();

        object["deals"] = deals;


        let group_id = user_id%self.settle_group_count;

        debug!("settle partition={} {}", group_id, object);

        let message = SettlePublishTaskInfo {
            group_id:group_id,
            message_id:m.message_id,
            data: object,
        };

        self.backlog.inc_settle(group_id as usize);
        let res = self.settle_sender.send(message);
        match res {
            Ok(_) => {
            },
            Err(e) => {
                self.backlog.dec_settle(group_id as usize);
                panic!("publish_deal failed.{}", e);
            }
        }
    }

    pub fn publish_error(&self, m: &mut Market, extern_id: u64, user_id: u32, params: &JsonValue, code: u32) {

        m.message_id += 1;

        let mut object = JsonValue::new_object();
        object["type"] = "error".into();
        object["market"] = m.name.clone().into();
        object["msgid"] = m.message_id.into();
        object["txid"] = extern_id.into();
        object["params"] = params.clone();
        object["code"] = code.into();

        let group_id = user_id%self.settle_group_count;

        debug!("settle partition={} {}", group_id, object);

        let message = SettlePublishTaskInfo {
            group_id:group_id,
            message_id:m.message_id,
            data: object,
        };

        self.backlog.inc_settle(group_id as usize);
        let res = self.settle_sender.send(message);
        match res {
            Ok(_) => {
            },
            Err(e) => {
                self.backlog.dec_settle(group_id as usize);
                panic!("publish_error failed.{}", e);
            }
        }
    }

    pub fn publish_quote_deal(&self, m: &Market, tm: i64, price: &Decimal, amount: &Decimal, side: u32) {
        let mut object = JsonValue::new_object();

        object["type"] = "quote_deals".into();
        object["market"] = m.name.clone().into();

        let mut info = JsonValue::new_object();
        info["time"] = tm.into();
        info["side"] = side.into();
        info["deal_id"] = m.deals_id.into();

        info["price"] = price.to_string().into();
        info["amount"] = amount.to_string().into();

        object["info"] = info;

        let topic = format!("quote_deals.{}", m.name);

        debug!("{} {}", topic, object);

        let message = QuotePublishTaskInfo {
            deals_id: m.deals_id,
            topic: topic,
            data: object,
        };

        self.backlog.inc_quote();
        let res = self.quote_sender.send(message);
        match res {
            Ok(_) => {
            },
            Err(e) => {
                self.backlog.dec_quote();
                panic!("publish_quote_deal failed.{}", e);
            }
        }
    }
}
