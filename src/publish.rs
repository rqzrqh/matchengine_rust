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
    time::Duration,
};

use rdkafka::config::ClientConfig;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

/// Kafka publish tuning: quote vs settle each have batching and producer options
/// (`output_publish.quote` / `output_publish.settle` in YAML).
#[derive(Debug, Clone)]
pub struct PublishDriverCfg {
    pub quote_topic: String,
    pub quote_batch_size: usize,
    pub quote_linger_ms: u64,
    pub quote_max_in_flight_requests_per_connection: u32,
    pub settle_batch_size: usize,
    pub settle_linger_ms: u64,
    pub settle_max_in_flight_requests_per_connection: u32,
    pub settle_thread_count: usize,
}

pub struct Publish {
    quote_sender: mpsc::Sender<QuotePublishTaskInfo>,
    settle_senders: Vec<mpsc::Sender<SettlePublishTaskInfo>>,
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
    group_id: usize,
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

fn warm_kafka_producer_metadata(producer: &FutureProducer, topic: &str) {
    match producer
        .client()
        .fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(5)))
    {
        Ok(metadata) => {
            let partitions = metadata
                .topics()
                .iter()
                .find(|x| x.name() == topic)
                .map(|x| x.partitions().len())
                .unwrap_or(0);
            info!(
                "warmed Kafka producer metadata: topic={} partitions={}",
                topic, partitions
            );
        }
        Err(e) => warn!(
            "warm Kafka producer metadata failed: topic={} error={}",
            topic, e
        ),
    }
}

fn spawn_quote_publish_thread(
    brokers: String,
    main_routine_sender: mpsc::Sender<Task>,
    quote_topic: String,
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
            let producer_rt = tokio::runtime::Builder::new_current_thread()
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
                warm_kafka_producer_metadata(&producer, &quote_topic);
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
    producer: FutureProducer,
    main_routine_sender: mpsc::Sender<Task>,
    worker_id: usize,
    pushed_settle_message_ids: Vec<u64>,
    batch_size: usize,
    linger_ms: u64,
    receiver: mpsc::Receiver<SettlePublishTaskInfo>,
    backlog: Arc<PublishBacklog>,
) {
    thread::Builder::new()
        .name(format!("settle-publish-{}", worker_id))
        .spawn(move || {
            let producer_rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            producer_rt.block_on(async move {
                let mut pushed_settle_message_ids = pushed_settle_message_ids;

                loop {
                    let batch = collect_publish_batch(&receiver, batch_size);
                    let batch_len = batch.len();
                    let mut pending = Vec::with_capacity(batch_len);

                    for task in batch {
                        let group_id = task.group_id;
                        if task.settle_message_id <= pushed_settle_message_ids[group_id] {
                            backlog.dec_settle(group_id);
                            continue;
                        }

                        let settle_message_id = task.settle_message_id;
                        pending.push((
                            group_id,
                            settle_message_id,
                            enqueue_settle_publish(&producer, group_id as i32, &task.body),
                        ));
                        pushed_settle_message_ids[group_id] = settle_message_id;
                    }

                    if pending.len() > 1 {
                        info!(
                            "settle batch flush: worker={} queued={} delivered_after_ack={} linger_ms={}",
                            worker_id,
                            batch_len,
                            pending.len(),
                            linger_ms
                        );
                    }

                    for (group_id, settle_message_id, delivery) in pending {
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
                        let task = SettlePublishProgressTask {
                            group_id,
                            pushed_settle_message_id: settle_message_id,
                        };
                        main_routine_sender
                            .send(Task::SettleProgressUpdateTask(task))
                            .expect("send settle progress update task failed");
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
            quote_topic,
            quote_batch_size,
            quote_linger_ms,
            quote_max_in_flight_requests_per_connection,
            settle_batch_size,
            settle_linger_ms,
            settle_max_in_flight_requests_per_connection,
            settle_thread_count,
        } = output_publish;

        let (quote_sender, quote_receiver) = mpsc::channel();
        let settle_group_count = pushed_settle_message_ids.len();
        assert!(settle_group_count > 0, "settle group count must be > 0");
        assert!(
            settle_thread_count > 0,
            "settle publish thread count must be > 0"
        );
        assert_eq!(
            settle_group_count % settle_thread_count,
            0,
            "settle group count must be an integer multiple of settle publish thread count"
        );
        let backlog = Arc::new(PublishBacklog::new(settle_group_count));
        let settle_producer = build_kafka_producer(
            &brokers,
            settle_batch_size,
            settle_linger_ms,
            settle_max_in_flight_requests_per_connection,
            true,
        );
        warm_kafka_producer_metadata(&settle_producer, "settle");
        let mut settle_senders = Vec::with_capacity(settle_group_count);

        spawn_quote_publish_thread(
            brokers.clone(),
            main_routine_sender.clone(),
            quote_topic,
            pushed_quote_deals_id,
            quote_batch_size,
            quote_linger_ms,
            quote_max_in_flight_requests_per_connection,
            quote_receiver,
            backlog.clone(),
        );
        let groups_per_thread = settle_group_count / settle_thread_count;
        let mut worker_senders = Vec::with_capacity(settle_thread_count);
        for worker_id in 0..settle_thread_count {
            let (settle_sender, settle_receiver) = mpsc::channel();
            spawn_settle_publish_thread(
                settle_producer.clone(),
                main_routine_sender.clone(),
                worker_id,
                pushed_settle_message_ids.clone(),
                settle_batch_size,
                settle_linger_ms,
                settle_receiver,
                backlog.clone(),
            );
            worker_senders.push(settle_sender);
        }
        for group_id in 0..settle_group_count {
            let worker_id = group_id / groups_per_thread;
            settle_senders.push(worker_senders[worker_id].clone());
        }

        Publish {
            quote_sender,
            settle_senders,
            backlog,
        }
    }

    fn send_settle_publish_task(
        &self,
        group_id: usize,
        message: SettlePublishTaskInfo,
        context: &str,
    ) {
        self.backlog.inc_settle(group_id);
        let Some(sender) = self.settle_senders.get(group_id) else {
            self.backlog.dec_settle(group_id);
            panic!("settle publish group_id out of range: {}", group_id);
        };

        if let Err(e) = sender.send(message) {
            self.backlog.dec_settle(group_id);
            panic!("{} failed.{}", context, e);
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
            group_id: group_id as usize,
            settle_message_id,
            body,
        };

        self.send_settle_publish_task(group_id as usize, message, "publish_cancel_order");
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
            group_id: group_id as usize,
            settle_message_id,
            body,
        };

        self.send_settle_publish_task(group_id as usize, message, "publish_error");
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
            group_id: group_id as usize,
            settle_message_id,
            body,
        };

        self.send_settle_publish_task(group_id as usize, message, "publish_put_order");
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
            group_id: group_id as usize,
            settle_message_id,
            body,
        };

        self.send_settle_publish_task(group_id as usize, message, "publish_deal");
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
