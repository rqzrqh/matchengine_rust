use crate::market::*;
use crate::payload_encoding::encode_msgpack_named;
use crate::task::*;
use rust_decimal::prelude::*;
use serde::Serialize;
use std::rc::Rc;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
};

use rdkafka::config::ClientConfig;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use tokio::sync::mpsc as tokio_mpsc;

/// Kafka publish tuning: quote vs settle each have batching and producer options
/// (`output_publish.quote` / `output_publish.settle` in YAML).
#[derive(Debug, Clone)]
pub struct PublishDriverCfg {
    pub quote_batch_size: usize,
    pub quote_linger_ms: u64,
    pub quote_max_in_flight_requests_per_connection: u32,
    pub settle_batch_size: usize,
    pub settle_linger_ms: u64,
    pub settle_max_in_flight_requests_per_connection: u32,
}

pub struct Publish {
    quote_sender: mpsc::Sender<QuotePublishTaskInfo>,
    settle_sender: mpsc::Sender<SettlePublishTaskInfo>,
    backlog: Arc<PublishBacklog>,
}

pub trait MatchPublisher {
    fn publish_put_order(&self, m: &mut Market, extern_id: u64, order: &Rc<Order>);

    fn publish_deal(
        &self,
        m: &mut Market,
        extern_id: u64,
        tm: i64,
        user_id: u32,
        rival_user_id: u32,
        order_id: u64,
        role: u32,
        price: &Decimal,
        amount: &Decimal,
        deal: &Decimal,
        fee: &Decimal,
        rival_fee: &Decimal,
    );

    fn publish_quote_deal(&self, m: &Market, tm: i64, price: &Decimal, amount: &Decimal, side: u32);
}

pub struct PublishBacklog {
    quote_pending: AtomicUsize,
    settle_pending: AtomicUsize,
    settle_group_pending: Vec<AtomicUsize>,
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
}

/// Matches `Order::to_json`: decimal fields are JSON strings from `Decimal::to_string()`.
#[derive(Debug, Serialize)]
struct KafkaOrderPayload {
    id: u64,
    #[serde(rename = "type")]
    order_type: u32,
    side: u32,
    create_time: i64,
    update_time: i64,
    user_id: u32,
    price: String,
    amount: String,
    taker_fee_rate: String,
    maker_fee_rate: String,
    left: String,
    deal_stock: String,
    deal_money: String,
    deal_fee: String,
}

fn kafka_order_payload(order: &Order) -> KafkaOrderPayload {
    KafkaOrderPayload {
        id: order.id,
        order_type: order.order_type,
        side: order.side,
        create_time: order.create_time,
        update_time: order.update_time.get(),
        user_id: order.user_id,
        price: order.price.to_string(),
        amount: order.amount.get().to_string(),
        taker_fee_rate: order.taker_fee_rate.to_string(),
        maker_fee_rate: order.maker_fee_rate.to_string(),
        left: order.left.get().to_string(),
        deal_stock: order.deal_stock.get().to_string(),
        deal_money: order.deal_money.get().to_string(),
        deal_fee: order.deal_fee.get().to_string(),
    }
}

#[derive(Debug, Serialize)]
struct DealsNested {
    time: i64,
    user_id: u32,
    rival_user_id: u32,
    order_id: u64,
    deal_id: u64,
    role: u32,
    price: String,
    amount: String,
    deal: String,
    fee: String,
    rival_fee: String,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum SettlePublishBody {
    PutOrder(SettlePutOrderMsg),
    CancelOrder(SettleCancelOrderMsg),
    Error(SettleErrorMsg),
    Deals(SettleDealsMsg),
}

#[derive(Debug, Serialize)]
struct SettlePutOrderMsg {
    #[serde(rename = "type")]
    message_type: u32,
    market: String,
    msgid: u64,
    settle_message_id: u64,
    txid: u64,
    order: KafkaOrderPayload,
}

#[derive(Debug, Serialize)]
struct SettleCancelOrderMsg {
    #[serde(rename = "type")]
    message_type: u32,
    market: String,
    msgid: u64,
    settle_message_id: u64,
    txid: u64,
    order: KafkaOrderPayload,
}

#[derive(Debug, Serialize)]
struct SettleErrorMsg {
    #[serde(rename = "type")]
    message_type: u32,
    market: String,
    msgid: u64,
    settle_message_id: u64,
    txid: u64,
    params: MqParams,
    code: u32,
}

#[derive(Debug, Serialize)]
struct SettleDealsMsg {
    #[serde(rename = "type")]
    message_type: u32,
    market: String,
    msgid: u64,
    settle_message_id: u64,
    txid: u64,
    deals: DealsNested,
}

#[derive(Debug, Serialize)]
struct QuoteDealInfoKafka {
    time: i64,
    side: u32,
    deal_id: u64,
    price: String,
    amount: String,
}

#[derive(Debug, Serialize)]
struct QuoteDealsKafkaMsg {
    #[serde(rename = "type")]
    message_type: u32,
    market: String,
    info: QuoteDealInfoKafka,
}

struct QuotePublishTaskInfo {
    deals_id: u64,
    topic: String,
    body: QuoteDealsKafkaMsg,
}

struct SettlePublishTaskInfo {
    group_id: u32,
    settle_message_id: u64,
    body: SettlePublishBody,
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
    body: &QuoteDealsKafkaMsg,
) -> DeliveryFuture {
    let payload = encode_msgpack_named(body);
    let record = FutureRecord::to(topic).payload(&payload);
    producer
        .send_result::<Vec<u8>, _>(record)
        .unwrap_or_else(|(e, _)| panic!("publish enqueue failed topic={} error={}", topic, e))
}

fn enqueue_settle_publish(
    producer: &FutureProducer,
    partition: i32,
    body: &SettlePublishBody,
) -> DeliveryFuture {
    let payload = encode_msgpack_named(body);
    let record = FutureRecord::to("settle")
        .partition(partition)
        .payload(&payload);
    producer
        .send_result::<Vec<u8>, _>(record)
        .unwrap_or_else(|(e, _)| {
            panic!(
                "settle publish enqueue failed partition={} error={}",
                partition, e
            )
        })
}

fn build_kafka_producer(
    brokers: &str,
    batch_size: usize,
    linger_ms: u64,
    max_in_flight_requests_per_connection: u32,
    use_idempotence: bool,
) -> FutureProducer {
    let linger_ms_str = linger_ms.to_string();
    let batch_size_str = batch_size.to_string();
    let in_flight_str = max_in_flight_requests_per_connection.to_string();

    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .set(
            "max.in.flight.requests.per.connection",
            in_flight_str.as_str(),
        )
        .set("linger.ms", &linger_ms_str)
        .set("batch.num.messages", &batch_size_str);
    if use_idempotence {
        cfg.set("enable.idempotence", "true").set("acks", "all");
    }
    cfg.create().expect("Producer creation error")
}

fn spawn_quote_publish_thread(
    brokers: String,
    main_routine_sender: mpsc::Sender<Task>,
    pushed_quote_deals_id: u64,
    batch_size: usize,
    linger_ms: u64,
    max_in_flight_requests_per_connection: u32,
    receiver: mpsc::Receiver<QuotePublishTaskInfo>,
    backlog: Arc<PublishBacklog>,
) {
    thread::Builder::new()
        .name("quote-publish".to_owned())
        .spawn(move || {
            let producer_rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("quote-pub-io")
                .enable_all()
                .build()
                .unwrap();

            producer_rt.block_on(async move {
                let producer = build_kafka_producer(
                    &brokers,
                    batch_size,
                    linger_ms,
                    max_in_flight_requests_per_connection,
                    false,
                );
                let mut pushed_quote_deals_id = pushed_quote_deals_id;

                loop {
                    let batch = collect_publish_batch(&receiver, batch_size);
                    let batch_len = batch.len();
                    let mut pending = Vec::with_capacity(batch_len);
                    let mut next_pushed_quote_deals_id = pushed_quote_deals_id;

                    for task in batch {
                        if task.deals_id > next_pushed_quote_deals_id {
                            pending.push((
                                task.deals_id,
                                enqueue_publish(&producer, &task.topic, &task.body),
                            ));
                            next_pushed_quote_deals_id = task.deals_id;
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
                        pushed_quote_deals_id = deals_id;
                        let task = QuotePublishProgressTask {
                            pushed_quote_deals_id,
                        };
                        main_routine_sender
                            .send(Task::QuoteProgressUpdateTask(task))
                            .expect("send quote progress update task failed");
                    }
                }
            });
        })
        .expect("spawn quote publish thread");
}

fn spawn_settle_publish_thread(
    brokers: String,
    main_routine_sender: mpsc::Sender<Task>,
    pushed_settle_message_ids: Vec<u64>,
    batch_size: usize,
    linger_ms: u64,
    max_in_flight_requests_per_connection: u32,
    receiver: mpsc::Receiver<SettlePublishTaskInfo>,
    backlog: Arc<PublishBacklog>,
) {
    thread::Builder::new()
        .name("settle-publish".to_owned())
        .spawn(move || {
            let settle_io_workers = std::thread::available_parallelism()
                .map(|n| n.get().clamp(2, 8))
                .unwrap_or(4);

            let producer_rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(settle_io_workers)
                .thread_name("settle-pub-io")
                .enable_all()
                .build()
                .unwrap();

            producer_rt.block_on(async move {
                let producer = Arc::new(build_kafka_producer(
                    &brokers,
                    batch_size,
                    linger_ms,
                    max_in_flight_requests_per_connection,
                    true,
                ));
                let mut partition_senders = Vec::with_capacity(pushed_settle_message_ids.len());

                for (group_id, pushed_settle_message_id) in
                    pushed_settle_message_ids.into_iter().enumerate()
                {
                    let (partition_sender, mut partition_receiver) =
                        tokio_mpsc::unbounded_channel::<SettlePublishTaskInfo>();
                    let producer = producer.clone();
                    let main_routine_sender = main_routine_sender.clone();
                    let backlog = backlog.clone();

                    tokio::spawn(async move {
                        let mut pushed_settle_message_id = pushed_settle_message_id;

                        while let Some(task) = partition_receiver.recv().await {
                            if task.settle_message_id <= pushed_settle_message_id {
                                backlog.dec_settle(group_id);
                                continue;
                            }

                            let settle_message_id = task.settle_message_id;
                            let delivery =
                                enqueue_settle_publish(&producer, group_id as i32, &task.body);

                            match delivery.await {
                                Ok(Ok(_)) => {}
                                Ok(Err((e, _))) => panic!(
                                    "settle publish delivery failed partition={}: {}",
                                    group_id, e
                                ),
                                Err(e) => panic!(
                                    "settle publish delivery canceled partition={}: {}",
                                    group_id, e
                                ),
                            }

                            backlog.dec_settle(group_id);
                            pushed_settle_message_id = settle_message_id;
                            let task = SettlePublishProgressTask {
                                group_id,
                                pushed_settle_message_id: settle_message_id,
                            };
                            main_routine_sender
                                .send(Task::SettleProgressUpdateTask(task))
                                .expect("send settle progress update task failed");
                        }
                    });

                    partition_senders.push(partition_sender);
                }

                loop {
                    let batch = collect_publish_batch(&receiver, batch_size);
                    let batch_len = batch.len();

                    for task in batch {
                        let group_id = task.group_id as usize;
                        let Some(sender) = partition_senders.get(group_id) else {
                            panic!("settle publish group_id out of range: {}", group_id);
                        };
                        if let Err(e) = sender.send(task) {
                            backlog.dec_settle(group_id);
                            panic!(
                                "settle partition worker stopped partition={} settle_message_id={}",
                                group_id, e.0.settle_message_id
                            );
                        }
                    }

                    if batch_len > 1 {
                        info!(
                            "settle dispatch batch: queued={} partitions={} linger_ms={}",
                            batch_len,
                            partition_senders.len(),
                            linger_ms
                        );
                    }
                }
            });
        })
        .expect("spawn settle publish thread");
}

impl Publish {
    pub fn new(
        brokers: String,
        main_routine_sender: mpsc::Sender<Task>,
        pushed_quote_deals_id: u64,
        pushed_settle_message_ids: Vec<u64>,
        output_publish: PublishDriverCfg,
    ) -> Publish {
        let PublishDriverCfg {
            quote_batch_size,
            quote_linger_ms,
            quote_max_in_flight_requests_per_connection,
            settle_batch_size,
            settle_linger_ms,
            settle_max_in_flight_requests_per_connection,
        } = output_publish;

        let (quote_sender, quote_receiver) = mpsc::channel();
        let (settle_sender, settle_receiver) = mpsc::channel();
        let settle_group_count = pushed_settle_message_ids.len() as u32;
        let backlog = Arc::new(PublishBacklog::new(settle_group_count as usize));

        spawn_quote_publish_thread(
            brokers.clone(),
            main_routine_sender.clone(),
            pushed_quote_deals_id,
            quote_batch_size,
            quote_linger_ms,
            quote_max_in_flight_requests_per_connection,
            quote_receiver,
            backlog.clone(),
        );
        spawn_settle_publish_thread(
            brokers,
            main_routine_sender,
            pushed_settle_message_ids,
            settle_batch_size,
            settle_linger_ms,
            settle_max_in_flight_requests_per_connection,
            settle_receiver,
            backlog.clone(),
        );

        Publish {
            quote_sender,
            settle_sender,
            backlog,
        }
    }

    pub fn publish_cancel_order(&self, m: &mut Market, extern_id: u64, order: &Rc<Order>) {
        let settle_message_id = m.next_settle_message_id(order.user_id);
        let body = SettlePublishBody::CancelOrder(SettleCancelOrderMsg {
            message_type: SETTLE_MSG_TYPE_CANCEL_ORDER,
            market: m.name.clone(),
            msgid: m.message_id,
            settle_message_id,
            txid: extern_id,
            order: kafka_order_payload(order),
        });

        let group_id = Market::settle_group_id(order.user_id) as u32;

        debug!("settle partition={} {:?}", group_id, body);

        let message = SettlePublishTaskInfo {
            group_id,
            settle_message_id,
            body,
        };

        self.backlog.inc_settle(group_id as usize);
        let res = self.settle_sender.send(message);
        match res {
            Ok(_) => {}
            Err(e) => {
                self.backlog.dec_settle(group_id as usize);
                panic!("publish_cancel_order failed.{}", e);
            }
        }
    }

    pub fn publish_error(
        &self,
        m: &mut Market,
        extern_id: u64,
        user_id: u32,
        params: &MqParams,
        code: u32,
    ) {
        m.message_id += 1;

        let settle_message_id = m.next_settle_message_id(user_id);
        let body = SettlePublishBody::Error(SettleErrorMsg {
            message_type: SETTLE_MSG_TYPE_ERROR,
            market: m.name.clone(),
            msgid: m.message_id,
            settle_message_id,
            txid: extern_id,
            params: params.clone(),
            code,
        });

        let group_id = Market::settle_group_id(user_id) as u32;

        debug!("settle partition={} {:?}", group_id, body);

        let message = SettlePublishTaskInfo {
            group_id,
            settle_message_id,
            body,
        };

        self.backlog.inc_settle(group_id as usize);
        let res = self.settle_sender.send(message);
        match res {
            Ok(_) => {}
            Err(e) => {
                self.backlog.dec_settle(group_id as usize);
                panic!("publish_error failed.{}", e);
            }
        }
    }
}

impl MatchPublisher for Publish {
    fn publish_put_order(&self, m: &mut Market, extern_id: u64, order: &Rc<Order>) {
        let settle_message_id = m.next_settle_message_id(order.user_id);
        let body = SettlePublishBody::PutOrder(SettlePutOrderMsg {
            message_type: SETTLE_MSG_TYPE_PUT_ORDER,
            market: m.name.clone(),
            msgid: m.message_id,
            settle_message_id,
            txid: extern_id,
            order: kafka_order_payload(order),
        });

        let group_id = Market::settle_group_id(order.user_id) as u32;

        debug!("settle partition={} {:?}", group_id, body);

        let message = SettlePublishTaskInfo {
            group_id,
            settle_message_id,
            body,
        };

        self.backlog.inc_settle(group_id as usize);
        let res = self.settle_sender.send(message);
        match res {
            Ok(_) => {}
            Err(e) => {
                self.backlog.dec_settle(group_id as usize);
                panic!("publish_put_order failed.{}", e);
            }
        }
    }

    fn publish_deal(
        &self,
        m: &mut Market,
        extern_id: u64,
        tm: i64,
        user_id: u32,
        rival_user_id: u32,
        order_id: u64,
        role: u32,
        price: &Decimal,
        amount: &Decimal,
        deal: &Decimal,
        fee: &Decimal,
        rival_fee: &Decimal,
    ) {
        let settle_message_id = m.next_settle_message_id(user_id);
        let body = SettlePublishBody::Deals(SettleDealsMsg {
            message_type: SETTLE_MSG_TYPE_DEALS,
            market: m.name.clone(),
            msgid: m.message_id,
            settle_message_id,
            txid: extern_id,
            deals: DealsNested {
                time: tm,
                user_id,
                rival_user_id,
                order_id,
                deal_id: m.deals_id,
                role,
                price: price.to_string(),
                amount: amount.to_string(),
                deal: deal.to_string(),
                fee: fee.to_string(),
                rival_fee: rival_fee.to_string(),
            },
        });

        let group_id = Market::settle_group_id(user_id) as u32;

        debug!("settle partition={} {:?}", group_id, body);

        let message = SettlePublishTaskInfo {
            group_id,
            settle_message_id,
            body,
        };

        self.backlog.inc_settle(group_id as usize);
        let res = self.settle_sender.send(message);
        match res {
            Ok(_) => {}
            Err(e) => {
                self.backlog.dec_settle(group_id as usize);
                panic!("publish_deal failed.{}", e);
            }
        }
    }

    fn publish_quote_deal(
        &self,
        m: &Market,
        tm: i64,
        price: &Decimal,
        amount: &Decimal,
        side: u32,
    ) {
        let body = QuoteDealsKafkaMsg {
            message_type: QUOTE_MSG_TYPE_DEAL,
            market: m.name.clone(),
            info: QuoteDealInfoKafka {
                time: tm,
                side,
                deal_id: m.deals_id,
                price: price.to_string(),
                amount: amount.to_string(),
            },
        };

        let topic = format!("quote_deals.{}", m.name);

        debug!("{} {:?}", topic, body);

        let message = QuotePublishTaskInfo {
            deals_id: m.deals_id,
            topic,
            body,
        };

        self.backlog.inc_quote();
        let res = self.quote_sender.send(message);
        match res {
            Ok(_) => {}
            Err(e) => {
                self.backlog.dec_quote();
                panic!("publish_quote_deal failed.{}", e);
            }
        }
    }
}
