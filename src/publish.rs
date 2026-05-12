use crate::market::*;
use crate::payload_encoding::encode_msgpack_named;
use crate::task::*;
use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rust_decimal::prelude::*;
use serde::Serialize;
use std::collections::{BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use rdkafka::config::ClientConfig;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

/// Kafka publish tuning: quote vs settle each have batching and producer options
/// (`output_publish.quote` / `output_publish.settle` in YAML).
#[derive(Debug, Clone)]
pub struct KafkaProducerDriverCfg {
    pub batch_num_messages: usize,
    pub linger_ms: u64,
    pub max_in_flight_requests_per_connection: u32,
    pub queue_buffering_max_messages: usize,
    pub queue_buffering_max_kbytes: usize,
    pub compression_type: String,
    pub delivery_timeout_ms: u64,
    pub statistics_interval_ms: u64,
}

#[derive(Debug, Clone)]
pub struct PublishDriverCfg {
    pub quote_topic: String,
    pub quote: KafkaProducerDriverCfg,
    pub quote_channel_capacity: usize,
    pub quote_drain_batch_size: usize,
    pub quote_max_outstanding: usize,
    pub settle: KafkaProducerDriverCfg,
    pub settle_channel_capacity: usize,
    pub settle_drain_batch_size: usize,
    pub settle_max_outstanding: usize,
    pub settle_worker_max_outstanding: usize,
    pub settle_per_group_send_burst: usize,
    pub settle_thread_count: usize,
    pub progress_flush_interval: Duration,
}

pub struct Publish {
    quote_sender: mpsc::SyncSender<QuotePublishTaskInfo>,
    quote_topic: String,
    settle_senders: Vec<mpsc::SyncSender<SettlePublishTaskInfo>>,
    settle_worker_by_group: Vec<usize>,
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
    quote_channel_capacity: usize,
    quote_max_outstanding: usize,
    settle_channel_capacity_per_worker: usize,
    settle_max_outstanding_per_group: usize,
    settle_worker_max_outstanding: usize,
    quote_pending: AtomicUsize,
    settle_pending: AtomicUsize,
    settle_group_pending: Vec<AtomicUsize>,
    quote_channel_full_count: AtomicUsize,
    quote_channel_blocked_nanos: AtomicU64,
    quote_channel_max_blocked_nanos: AtomicU64,
    settle_worker_channel_full_count: Vec<AtomicUsize>,
    settle_worker_channel_blocked_nanos: Vec<AtomicU64>,
    settle_worker_channel_max_blocked_nanos: Vec<AtomicU64>,
    quote_in_flight: AtomicUsize,
    quote_queue_full_count: AtomicUsize,
    settle_worker_outstanding: Vec<AtomicUsize>,
    settle_worker_queue_full_count: Vec<AtomicUsize>,
}

impl PublishBacklog {
    fn new(
        settle_group_count: usize,
        settle_worker_count: usize,
        quote_channel_capacity: usize,
        quote_max_outstanding: usize,
        settle_channel_capacity_per_worker: usize,
        settle_max_outstanding_per_group: usize,
        settle_worker_max_outstanding: usize,
    ) -> Self {
        Self {
            quote_channel_capacity,
            quote_max_outstanding,
            settle_channel_capacity_per_worker,
            settle_max_outstanding_per_group,
            settle_worker_max_outstanding,
            quote_pending: AtomicUsize::new(0),
            settle_pending: AtomicUsize::new(0),
            settle_group_pending: (0..settle_group_count)
                .map(|_| AtomicUsize::new(0))
                .collect(),
            quote_channel_full_count: AtomicUsize::new(0),
            quote_channel_blocked_nanos: AtomicU64::new(0),
            quote_channel_max_blocked_nanos: AtomicU64::new(0),
            settle_worker_channel_full_count: (0..settle_worker_count)
                .map(|_| AtomicUsize::new(0))
                .collect(),
            settle_worker_channel_blocked_nanos: (0..settle_worker_count)
                .map(|_| AtomicU64::new(0))
                .collect(),
            settle_worker_channel_max_blocked_nanos: (0..settle_worker_count)
                .map(|_| AtomicU64::new(0))
                .collect(),
            quote_in_flight: AtomicUsize::new(0),
            quote_queue_full_count: AtomicUsize::new(0),
            settle_worker_outstanding: (0..settle_worker_count)
                .map(|_| AtomicUsize::new(0))
                .collect(),
            settle_worker_queue_full_count: (0..settle_worker_count)
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

    fn inc_quote_in_flight(&self) {
        self.quote_in_flight.fetch_add(1, Ordering::Relaxed);
    }

    fn dec_quote_in_flight(&self) {
        let prev = self.quote_in_flight.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(prev > 0, "quote in-flight underflow");
    }

    fn inc_quote_queue_full(&self) {
        self.quote_queue_full_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_quote_channel_blocked(&self, blocked: Duration) {
        let nanos = duration_nanos_u64(blocked);
        self.quote_channel_full_count
            .fetch_add(1, Ordering::Relaxed);
        self.quote_channel_blocked_nanos
            .fetch_add(nanos, Ordering::Relaxed);
        record_atomic_max(&self.quote_channel_max_blocked_nanos, nanos);
    }

    fn record_settle_channel_blocked(&self, worker_id: usize, blocked: Duration) {
        let nanos = duration_nanos_u64(blocked);
        self.settle_worker_channel_full_count[worker_id].fetch_add(1, Ordering::Relaxed);
        self.settle_worker_channel_blocked_nanos[worker_id].fetch_add(nanos, Ordering::Relaxed);
        record_atomic_max(
            &self.settle_worker_channel_max_blocked_nanos[worker_id],
            nanos,
        );
    }

    fn add_settle_worker_outstanding(&self, worker_id: usize, count: usize) {
        self.settle_worker_outstanding[worker_id].fetch_add(count, Ordering::Relaxed);
    }

    fn dec_settle_worker_outstanding(&self, worker_id: usize) {
        let prev = self.settle_worker_outstanding[worker_id].fetch_sub(1, Ordering::Relaxed);
        debug_assert!(prev > 0, "settle worker outstanding underflow");
    }

    fn inc_settle_worker_queue_full(&self, worker_id: usize) {
        self.settle_worker_queue_full_count[worker_id].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> PublishStatusSnapshot {
        PublishStatusSnapshot {
            quote_channel_capacity: self.quote_channel_capacity,
            quote_max_outstanding: self.quote_max_outstanding,
            settle_channel_capacity_per_worker: self.settle_channel_capacity_per_worker,
            settle_max_outstanding_per_group: self.settle_max_outstanding_per_group,
            settle_worker_max_outstanding: self.settle_worker_max_outstanding,
            quote_pending: self.quote_pending.load(Ordering::Relaxed),
            settle_pending: self.settle_pending.load(Ordering::Relaxed),
            settle_group_pending: self
                .settle_group_pending
                .iter()
                .map(|pending| pending.load(Ordering::Relaxed))
                .collect(),
            quote_channel_full_count: self.quote_channel_full_count.load(Ordering::Relaxed),
            quote_channel_blocked_nanos: self.quote_channel_blocked_nanos.load(Ordering::Relaxed),
            quote_channel_max_blocked_nanos: self
                .quote_channel_max_blocked_nanos
                .load(Ordering::Relaxed),
            settle_worker_channel_full_count: self
                .settle_worker_channel_full_count
                .iter()
                .map(|count| count.load(Ordering::Relaxed))
                .collect(),
            settle_worker_channel_blocked_nanos: self
                .settle_worker_channel_blocked_nanos
                .iter()
                .map(|nanos| nanos.load(Ordering::Relaxed))
                .collect(),
            settle_worker_channel_max_blocked_nanos: self
                .settle_worker_channel_max_blocked_nanos
                .iter()
                .map(|nanos| nanos.load(Ordering::Relaxed))
                .collect(),
            quote_in_flight: self.quote_in_flight.load(Ordering::Relaxed),
            quote_queue_full_count: self.quote_queue_full_count.load(Ordering::Relaxed),
            settle_worker_outstanding: self
                .settle_worker_outstanding
                .iter()
                .map(|outstanding| outstanding.load(Ordering::Relaxed))
                .collect(),
            settle_worker_queue_full_count: self
                .settle_worker_queue_full_count
                .iter()
                .map(|count| count.load(Ordering::Relaxed))
                .collect(),
        }
    }
}

pub struct PublishStatusSnapshot {
    pub quote_channel_capacity: usize,
    pub quote_max_outstanding: usize,
    pub settle_channel_capacity_per_worker: usize,
    pub settle_max_outstanding_per_group: usize,
    pub settle_worker_max_outstanding: usize,
    pub quote_pending: usize,
    pub settle_pending: usize,
    pub settle_group_pending: Vec<usize>,
    pub quote_channel_full_count: usize,
    pub quote_channel_blocked_nanos: u64,
    pub quote_channel_max_blocked_nanos: u64,
    pub settle_worker_channel_full_count: Vec<usize>,
    pub settle_worker_channel_blocked_nanos: Vec<u64>,
    pub settle_worker_channel_max_blocked_nanos: Vec<u64>,
    pub quote_in_flight: usize,
    pub quote_queue_full_count: usize,
    pub settle_worker_outstanding: Vec<usize>,
    pub settle_worker_queue_full_count: Vec<usize>,
}

fn duration_nanos_u64(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn record_atomic_max(target: &AtomicU64, value: u64) {
    let mut observed = target.load(Ordering::Relaxed);
    while value > observed {
        match target.compare_exchange_weak(observed, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(current) => observed = current,
        }
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
    body: QuoteDealsKafkaMsg,
}

struct SettlePublishTaskInfo {
    group_id: usize,
    settle_message_id: u64,
    body: SettlePublishBody,
}

struct CompletedQuoteDelivery {
    deals_id: u64,
}

struct PendingQuoteDeliveryFuture {
    deals_id: u64,
    delivery: DeliveryFuture,
}

impl Future for PendingQuoteDeliveryFuture {
    type Output = CompletedQuoteDelivery;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.delivery).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(Ok(_))) => Poll::Ready(CompletedQuoteDelivery {
                deals_id: self.deals_id,
            }),
            Poll::Ready(Ok(Err((e, _)))) => panic!("quote publish delivery failed: {}", e),
            Poll::Ready(Err(e)) => panic!("quote publish delivery canceled: {}", e),
        }
    }
}

enum StartQueuedQuoteTaskResult {
    Started,
    NoQueuedTask,
    NoCapacity,
    QueueFull,
}

struct QuoteState {
    queued: VecDeque<QuotePublishTaskInfo>,
    in_flight: usize,
    pushed_quote_deals_id: u64,
    scheduled_quote_deals_id: u64,
    acked_quote_deals_ids: BTreeSet<u64>,
}

impl QuoteState {
    fn new(pushed_quote_deals_id: u64) -> Self {
        Self {
            queued: VecDeque::new(),
            in_flight: 0,
            pushed_quote_deals_id,
            scheduled_quote_deals_id: pushed_quote_deals_id,
            acked_quote_deals_ids: BTreeSet::new(),
        }
    }

    fn advance_pushed_quote_deals_id(&mut self, deals_id: u64) -> Option<u64> {
        assert!(
            self.in_flight > 0,
            "quote delivery completed without in-flight task"
        );
        self.in_flight -= 1;

        if deals_id <= self.pushed_quote_deals_id {
            panic!(
                "quote delivery id must be above pushed cursor: delivery_id={} pushed_id={}",
                deals_id, self.pushed_quote_deals_id
            );
        }

        if deals_id != self.pushed_quote_deals_id + 1 {
            self.acked_quote_deals_ids.insert(deals_id);
            return None;
        }

        self.pushed_quote_deals_id = deals_id;
        while self
            .acked_quote_deals_ids
            .remove(&(self.pushed_quote_deals_id + 1))
        {
            self.pushed_quote_deals_id += 1;
        }

        Some(self.pushed_quote_deals_id)
    }
}

struct CompletedSettleDelivery {
    group_id: usize,
    settle_message_id: u64,
}

struct PendingSettleDeliveryFuture {
    group_id: usize,
    settle_message_id: u64,
    delivery: DeliveryFuture,
}

impl Future for PendingSettleDeliveryFuture {
    type Output = CompletedSettleDelivery;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.delivery).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(Ok(_))) => Poll::Ready(CompletedSettleDelivery {
                group_id: self.group_id,
                settle_message_id: self.settle_message_id,
            }),
            Poll::Ready(Ok(Err((e, _)))) => panic!(
                "settle publish delivery failed partition={}: {}",
                self.group_id, e
            ),
            Poll::Ready(Err(e)) => panic!(
                "settle publish delivery canceled partition={}: {}",
                self.group_id, e
            ),
        }
    }
}

enum StartReadySettleTaskResult {
    Started { queue_full: bool },
    NoReadyTask,
    QueueFull,
}

struct SettleBatchStart {
    started: usize,
    queue_full: bool,
}

struct SettleGroupState {
    // A settle group maps 1:1 to a Kafka settle partition. Keep all ordering,
    // batching, and outstanding-window state local to the owning worker thread
    // so other partitions can continue independently.
    queued: VecDeque<SettlePublishTaskInfo>,
    in_flight: usize,
    max_outstanding: usize,
    pushed_settle_message_id: u64,
    scheduled_settle_message_id: u64,
    acked_settle_message_ids: BTreeSet<u64>,
    ready_queued: bool,
}

impl SettleGroupState {
    fn new(pushed_settle_message_id: u64, max_outstanding: usize) -> Self {
        Self {
            queued: VecDeque::new(),
            in_flight: 0,
            max_outstanding,
            pushed_settle_message_id,
            scheduled_settle_message_id: pushed_settle_message_id,
            acked_settle_message_ids: BTreeSet::new(),
            ready_queued: false,
        }
    }

    fn has_outstanding_capacity(&self) -> bool {
        self.in_flight < self.max_outstanding
    }

    fn advance_pushed_settle_message_id(&mut self, settle_message_id: u64) -> Option<u64> {
        assert!(
            self.in_flight > 0,
            "settle delivery completed without in-flight task"
        );
        self.in_flight -= 1;

        if settle_message_id <= self.pushed_settle_message_id {
            panic!(
                "settle delivery id must be above pushed cursor: delivery_id={} pushed_id={}",
                settle_message_id, self.pushed_settle_message_id
            );
        }

        if settle_message_id != self.pushed_settle_message_id + 1 {
            self.acked_settle_message_ids.insert(settle_message_id);
            return None;
        }

        self.pushed_settle_message_id = settle_message_id;
        while self
            .acked_settle_message_ids
            .remove(&(self.pushed_settle_message_id + 1))
        {
            self.pushed_settle_message_id += 1;
        }

        Some(self.pushed_settle_message_id)
    }
}

struct QuoteProgressReporter {
    pending_pushed_quote_deals_id: Option<u64>,
    next_flush_at: Instant,
    flush_interval: Duration,
}

impl QuoteProgressReporter {
    fn new(flush_interval: Duration) -> Self {
        let now = Instant::now();
        Self {
            pending_pushed_quote_deals_id: None,
            next_flush_at: now + flush_interval,
            flush_interval,
        }
    }

    fn record(&mut self, pushed_quote_deals_id: u64) {
        self.pending_pushed_quote_deals_id = Some(pushed_quote_deals_id);
    }

    fn has_pending(&self) -> bool {
        self.pending_pushed_quote_deals_id.is_some()
    }

    fn time_until_due(&self) -> Duration {
        self.next_flush_at.saturating_duration_since(Instant::now())
    }

    fn flush_if_due(&mut self, main_routine_sender: &MainTaskSender) {
        if self.has_pending() && Instant::now() >= self.next_flush_at {
            self.flush(main_routine_sender);
        }
    }

    fn flush(&mut self, main_routine_sender: &MainTaskSender) {
        let Some(pushed_quote_deals_id) = self.pending_pushed_quote_deals_id.take() else {
            return;
        };
        let task = QuotePublishProgressTask {
            pushed_quote_deals_id,
        };
        main_routine_sender
            .send(Task::QuoteProgressUpdateTask(task))
            .expect("send quote progress update task failed");
        self.next_flush_at = Instant::now() + self.flush_interval;
    }
}

struct SettleProgressReporter {
    pending_pushed_settle_message_ids: Vec<Option<u64>>,
    dirty_groups: Vec<usize>,
    next_flush_at: Instant,
    flush_interval: Duration,
}

impl SettleProgressReporter {
    fn new(settle_group_count: usize, flush_interval: Duration) -> Self {
        let now = Instant::now();
        Self {
            pending_pushed_settle_message_ids: vec![None; settle_group_count],
            dirty_groups: Vec::new(),
            next_flush_at: now + flush_interval,
            flush_interval,
        }
    }

    fn record(&mut self, group_id: usize, pushed_settle_message_id: u64) {
        if self.pending_pushed_settle_message_ids[group_id].is_none() {
            self.dirty_groups.push(group_id);
        }
        self.pending_pushed_settle_message_ids[group_id] = Some(pushed_settle_message_id);
    }

    fn has_pending(&self) -> bool {
        !self.dirty_groups.is_empty()
    }

    fn time_until_due(&self) -> Duration {
        self.next_flush_at.saturating_duration_since(Instant::now())
    }

    fn flush_if_due(&mut self, main_routine_sender: &MainTaskSender) {
        if self.has_pending() && Instant::now() >= self.next_flush_at {
            self.flush(main_routine_sender);
        }
    }

    fn flush(&mut self, main_routine_sender: &MainTaskSender) {
        if !self.has_pending() {
            return;
        }

        let dirty_groups = std::mem::take(&mut self.dirty_groups);
        let mut progresses = Vec::with_capacity(dirty_groups.len());
        for group_id in dirty_groups {
            let Some(pushed_settle_message_id) =
                self.pending_pushed_settle_message_ids[group_id].take()
            else {
                continue;
            };
            progresses.push(SettlePublishProgressTask {
                group_id,
                pushed_settle_message_id,
            });
        }

        if !progresses.is_empty() {
            let task = SettlePublishProgressBatchTask { progresses };
            main_routine_sender
                .send(Task::SettleProgressBatchUpdateTask(task))
                .expect("send settle progress batch update task failed");
        }
        self.next_flush_at = Instant::now() + self.flush_interval;
    }
}

fn recv_quote_task(
    receiver: &mpsc::Receiver<QuotePublishTaskInfo>,
    progress: &mut QuoteProgressReporter,
    main_routine_sender: &MainTaskSender,
) -> QuotePublishTaskInfo {
    loop {
        if !progress.has_pending() {
            return receiver.recv().unwrap();
        }

        match receiver.recv_timeout(progress.time_until_due()) {
            Ok(task) => return task,
            Err(mpsc::RecvTimeoutError::Timeout) => progress.flush(main_routine_sender),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("publish task channel disconnected")
            }
        }
    }
}

fn recv_settle_task(
    receiver: &mpsc::Receiver<SettlePublishTaskInfo>,
    progress: &mut SettleProgressReporter,
    main_routine_sender: &MainTaskSender,
) -> SettlePublishTaskInfo {
    loop {
        if !progress.has_pending() {
            return receiver.recv().unwrap();
        }

        match receiver.recv_timeout(progress.time_until_due()) {
            Ok(task) => return task,
            Err(mpsc::RecvTimeoutError::Timeout) => progress.flush(main_routine_sender),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("publish task channel disconnected")
            }
        }
    }
}

fn queue_quote_task(quote: &mut QuoteState, backlog: &PublishBacklog, task: QuotePublishTaskInfo) {
    if task.deals_id <= quote.scheduled_quote_deals_id {
        backlog.dec_quote();
        return;
    }

    assert!(
        task.deals_id > quote.scheduled_quote_deals_id,
        "quote publish task must be strictly increasing: task_id={} scheduled_id={}",
        task.deals_id,
        quote.scheduled_quote_deals_id
    );
    quote.scheduled_quote_deals_id = task.deals_id;
    quote.queued.push_back(task);
}

fn queue_settle_task(
    groups: &mut [SettleGroupState],
    ready_groups: &mut VecDeque<usize>,
    task: SettlePublishTaskInfo,
) {
    let group_id = task.group_id;
    let group = &mut groups[group_id];
    assert!(
        task.settle_message_id > group.scheduled_settle_message_id,
        "settle publish task must be strictly increasing: group={} task_id={} scheduled_id={}",
        group_id,
        task.settle_message_id,
        group.scheduled_settle_message_id
    );

    group.scheduled_settle_message_id = task.settle_message_id;
    if group.queued.is_empty() && !group.ready_queued {
        ready_groups.push_back(group_id);
        group.ready_queued = true;
    }
    group.queued.push_back(task);
}

fn start_queued_quote_task(
    producer: &FutureProducer,
    topic: &str,
    quote: &mut QuoteState,
    deliveries: &mut FuturesUnordered<PendingQuoteDeliveryFuture>,
    max_outstanding: usize,
    backlog: &PublishBacklog,
) -> StartQueuedQuoteTaskResult {
    if quote.in_flight >= max_outstanding {
        return StartQueuedQuoteTaskResult::NoCapacity;
    }

    let Some(task) = quote.queued.pop_front() else {
        return StartQueuedQuoteTaskResult::NoQueuedTask;
    };
    let deals_id = task.deals_id;
    let delivery = match enqueue_publish(producer, topic, &task.body) {
        Ok(delivery) => delivery,
        Err(KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)) => {
            backlog.inc_quote_queue_full();
            quote.queued.push_front(task);
            return StartQueuedQuoteTaskResult::QueueFull;
        }
        Err(e) => panic!("quote publish enqueue failed topic={} error={}", topic, e),
    };

    quote.in_flight += 1;
    backlog.inc_quote_in_flight();
    deliveries.push(PendingQuoteDeliveryFuture { deals_id, delivery });
    StartQueuedQuoteTaskResult::Started
}

fn start_queued_quote_tasks(
    producer: &FutureProducer,
    topic: &str,
    quote: &mut QuoteState,
    deliveries: &mut FuturesUnordered<PendingQuoteDeliveryFuture>,
    max_outstanding: usize,
    backlog: &PublishBacklog,
) -> (usize, bool) {
    let mut started = 0;

    loop {
        match start_queued_quote_task(producer, topic, quote, deliveries, max_outstanding, backlog)
        {
            StartQueuedQuoteTaskResult::Started => started += 1,
            StartQueuedQuoteTaskResult::NoQueuedTask | StartQueuedQuoteTaskResult::NoCapacity => {
                return (started, false);
            }
            StartQueuedQuoteTaskResult::QueueFull => return (started, true),
        }
    }
}

async fn await_quote_delivery(
    deliveries: &mut FuturesUnordered<PendingQuoteDeliveryFuture>,
    quote: &mut QuoteState,
    main_routine_sender: &MainTaskSender,
    backlog: &PublishBacklog,
    progress: &mut QuoteProgressReporter,
) -> bool {
    let completed = if progress.has_pending() {
        tokio::select! {
            completed = deliveries.next() => completed,
            _ = tokio::time::sleep(progress.time_until_due()) => {
                progress.flush(main_routine_sender);
                return true;
            }
        }
    } else {
        deliveries.next().await
    };

    let Some(CompletedQuoteDelivery { deals_id }) = completed else {
        return false;
    };

    backlog.dec_quote();
    backlog.dec_quote_in_flight();
    let Some(pushed_quote_deals_id) = quote.advance_pushed_quote_deals_id(deals_id) else {
        return true;
    };

    progress.record(pushed_quote_deals_id);
    progress.flush_if_due(main_routine_sender);
    true
}

fn start_settle_group_batch(
    producer: &FutureProducer,
    worker_id: usize,
    group_id: usize,
    group: &mut SettleGroupState,
    deliveries: &mut FuturesUnordered<PendingSettleDeliveryFuture>,
    backlog: &PublishBacklog,
    send_burst: usize,
    worker_remaining: usize,
) -> Result<SettleBatchStart, KafkaError> {
    let mut started = 0;

    while started < send_burst && started < worker_remaining {
        if !group.has_outstanding_capacity() {
            break;
        }

        let Some(task) = group.queued.pop_front() else {
            break;
        };
        let settle_message_id = task.settle_message_id;
        let delivery = match enqueue_settle_publish(producer, group_id as i32, &task.body) {
            Ok(delivery) => delivery,
            Err(e) => {
                if matches!(
                    e,
                    KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
                ) {
                    backlog.inc_settle_worker_queue_full(worker_id);
                }
                group.queued.push_front(task);
                return if started > 0 {
                    Ok(SettleBatchStart {
                        started,
                        queue_full: matches!(
                            e,
                            KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
                        ),
                    })
                } else {
                    Err(e)
                };
            }
        };

        started += 1;
        group.in_flight += 1;
        deliveries.push(PendingSettleDeliveryFuture {
            group_id,
            settle_message_id,
            delivery,
        });
    }

    Ok(SettleBatchStart {
        started,
        queue_full: false,
    })
}

fn try_start_ready_settle_task(
    producer: &FutureProducer,
    worker_id: usize,
    groups: &mut [SettleGroupState],
    ready_groups: &mut VecDeque<usize>,
    deliveries: &mut FuturesUnordered<PendingSettleDeliveryFuture>,
    backlog: &PublishBacklog,
    send_burst: usize,
    max_outstanding_per_worker: usize,
) -> StartReadySettleTaskResult {
    if deliveries.len() >= max_outstanding_per_worker {
        return StartReadySettleTaskResult::NoReadyTask;
    }

    while let Some(group_id) = ready_groups.pop_front() {
        let group = &mut groups[group_id];
        group.ready_queued = false;
        if group.queued.is_empty() {
            continue;
        }
        if !group.has_outstanding_capacity() {
            continue;
        }

        let worker_remaining = max_outstanding_per_worker - deliveries.len();
        match start_settle_group_batch(
            producer,
            worker_id,
            group_id,
            group,
            deliveries,
            backlog,
            send_burst,
            worker_remaining,
        ) {
            Ok(SettleBatchStart { started: 0, .. }) => {}
            Ok(batch) => {
                backlog.add_settle_worker_outstanding(worker_id, batch.started);
                if !group.queued.is_empty() {
                    group.ready_queued = true;
                    if batch.queue_full {
                        ready_groups.push_front(group_id);
                    } else if group.has_outstanding_capacity() {
                        ready_groups.push_back(group_id);
                    } else {
                        group.ready_queued = false;
                    }
                }
                return StartReadySettleTaskResult::Started {
                    queue_full: batch.queue_full,
                };
            }
            Err(KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)) => {
                ready_groups.push_front(group_id);
                group.ready_queued = true;
                return StartReadySettleTaskResult::QueueFull;
            }
            Err(e) => panic!(
                "settle publish enqueue failed partition={} error={}",
                group_id, e
            ),
        }
    }

    StartReadySettleTaskResult::NoReadyTask
}

fn start_ready_settle_tasks(
    producer: &FutureProducer,
    worker_id: usize,
    groups: &mut [SettleGroupState],
    ready_groups: &mut VecDeque<usize>,
    deliveries: &mut FuturesUnordered<PendingSettleDeliveryFuture>,
    backlog: &PublishBacklog,
    send_burst: usize,
    max_outstanding_per_worker: usize,
) -> (usize, bool) {
    let mut started = 0;

    loop {
        match try_start_ready_settle_task(
            producer,
            worker_id,
            groups,
            ready_groups,
            deliveries,
            backlog,
            send_burst,
            max_outstanding_per_worker,
        ) {
            StartReadySettleTaskResult::Started { queue_full } => {
                started += 1;
                if queue_full {
                    return (started, true);
                }
            }
            StartReadySettleTaskResult::NoReadyTask => return (started, false),
            StartReadySettleTaskResult::QueueFull => return (started, true),
        }
    }
}

async fn await_settle_delivery(
    worker_id: usize,
    deliveries: &mut FuturesUnordered<PendingSettleDeliveryFuture>,
    groups: &mut [SettleGroupState],
    ready_groups: &mut VecDeque<usize>,
    main_routine_sender: &MainTaskSender,
    backlog: &PublishBacklog,
    progress: &mut SettleProgressReporter,
) -> bool {
    let completed = if progress.has_pending() {
        tokio::select! {
            completed = deliveries.next() => completed,
            _ = tokio::time::sleep(progress.time_until_due()) => {
                progress.flush(main_routine_sender);
                return true;
            }
        }
    } else {
        deliveries.next().await
    };

    let Some(CompletedSettleDelivery {
        group_id,
        settle_message_id,
    }) = completed
    else {
        return false;
    };

    backlog.dec_settle(group_id);
    backlog.dec_settle_worker_outstanding(worker_id);
    let group = &mut groups[group_id];
    let pushed_settle_message_id = group.advance_pushed_settle_message_id(settle_message_id);
    if !group.queued.is_empty() && !group.ready_queued {
        ready_groups.push_back(group_id);
        group.ready_queued = true;
    }

    let Some(pushed_settle_message_id) = pushed_settle_message_id else {
        return true;
    };

    progress.record(group_id, pushed_settle_message_id);
    progress.flush_if_due(main_routine_sender);
    true
}

fn enqueue_publish(
    producer: &FutureProducer,
    topic: &str,
    body: &QuoteDealsKafkaMsg,
) -> Result<DeliveryFuture, KafkaError> {
    let payload = encode_msgpack_named(body);
    let record = FutureRecord::<(), Vec<u8>>::to(topic).payload(&payload);
    producer.send_result(record).map_err(|(e, _record)| e)
}

fn enqueue_settle_publish(
    producer: &FutureProducer,
    partition: i32,
    body: &SettlePublishBody,
) -> Result<DeliveryFuture, KafkaError> {
    let payload = encode_msgpack_named(body);
    let record = FutureRecord::<(), Vec<u8>>::to("settle")
        .partition(partition)
        .payload(&payload);
    producer.send_result(record).map_err(|(e, _record)| e)
}

fn build_kafka_producer(
    brokers: &str,
    client_id: &str,
    producer_cfg: &KafkaProducerDriverCfg,
    use_idempotence: bool,
) -> FutureProducer {
    let linger_ms_str = producer_cfg.linger_ms.to_string();
    let batch_size_str = producer_cfg.batch_num_messages.to_string();
    let in_flight_str = producer_cfg
        .max_in_flight_requests_per_connection
        .to_string();
    let queue_messages_str = producer_cfg.queue_buffering_max_messages.to_string();
    let queue_kbytes_str = producer_cfg.queue_buffering_max_kbytes.to_string();
    let delivery_timeout_str = producer_cfg.delivery_timeout_ms.to_string();
    let statistics_interval_str = producer_cfg.statistics_interval_ms.to_string();

    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers)
        .set("client.id", client_id)
        .set("delivery.timeout.ms", &delivery_timeout_str)
        .set("statistics.interval.ms", &statistics_interval_str)
        .set("queue.buffering.max.messages", &queue_messages_str)
        .set("queue.buffering.max.kbytes", &queue_kbytes_str)
        .set("compression.type", producer_cfg.compression_type.as_str())
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
    market_name: String,
    main_routine_sender: MainTaskSender,
    quote_topic: String,
    pushed_quote_deals_id: u64,
    producer_cfg: KafkaProducerDriverCfg,
    drain_batch_size: usize,
    max_outstanding: usize,
    progress_flush_interval: Duration,
    receiver: mpsc::Receiver<QuotePublishTaskInfo>,
    backlog: Arc<PublishBacklog>,
) {
    thread::Builder::new()
        .name("quote-pub-1of1".to_owned())
        .spawn(move || {
            let producer_rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            producer_rt.block_on(async move {
                let client_id = format!("matchengine.{}.quote-producer.1of1", market_name);
                let producer = build_kafka_producer(&brokers, &client_id, &producer_cfg, false);
                warm_kafka_producer_metadata(&producer, &quote_topic);
                let mut quote = QuoteState::new(pushed_quote_deals_id);
                let mut deliveries = FuturesUnordered::new();
                let mut progress = QuoteProgressReporter::new(progress_flush_interval);

                loop {
                    if deliveries.is_empty() && quote.queued.is_empty() {
                        progress.flush(&main_routine_sender);
                        let task = recv_quote_task(&receiver, &mut progress, &main_routine_sender);
                        queue_quote_task(&mut quote, backlog.as_ref(), task);
                    }

                    for _ in 0..drain_batch_size {
                        match receiver.try_recv() {
                            Ok(task) => queue_quote_task(&mut quote, backlog.as_ref(), task),
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(mpsc::TryRecvError::Disconnected) => {
                                panic!("publish task channel disconnected")
                            }
                        }
                    }

                    let (started, queue_full) = start_queued_quote_tasks(
                        &producer,
                        &quote_topic,
                        &mut quote,
                        &mut deliveries,
                        max_outstanding,
                        backlog.as_ref(),
                    );
                    if started > 1 {
                        debug!(
                            "quote pipeline fill: queued={} outstanding={} linger_ms={}",
                            started,
                            deliveries.len(),
                            producer_cfg.linger_ms
                        );
                    }

                    if queue_full {
                        if !deliveries.is_empty() {
                            await_quote_delivery(
                                &mut deliveries,
                                &mut quote,
                                &main_routine_sender,
                                backlog.as_ref(),
                                &mut progress,
                            )
                            .await;
                        } else {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        continue;
                    }

                    if started > 0 {
                        continue;
                    }

                    if !deliveries.is_empty() {
                        await_quote_delivery(
                            &mut deliveries,
                            &mut quote,
                            &main_routine_sender,
                            backlog.as_ref(),
                            &mut progress,
                        )
                        .await;
                    }
                }
            });
        })
        .expect("spawn quote publish thread");
}

fn spawn_settle_publish_thread(
    producer: FutureProducer,
    main_routine_sender: MainTaskSender,
    worker_id: usize,
    worker_count: usize,
    pushed_settle_message_ids: Vec<u64>,
    drain_batch_size: usize,
    max_outstanding_per_group: usize,
    max_outstanding_per_worker: usize,
    per_group_send_burst: usize,
    producer_linger_ms: u64,
    progress_flush_interval: Duration,
    receiver: mpsc::Receiver<SettlePublishTaskInfo>,
    backlog: Arc<PublishBacklog>,
) {
    thread::Builder::new()
        .name(format!("settle-pub-{}of{}", worker_id + 1, worker_count))
        .spawn(move || {
            let producer_rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            producer_rt.block_on(async move {
                let mut groups: Vec<_> = pushed_settle_message_ids
                    .into_iter()
                    .map(|pushed_settle_message_id| {
                        SettleGroupState::new(pushed_settle_message_id, max_outstanding_per_group)
                    })
                    .collect();
                let mut ready_groups = VecDeque::new();
                let mut deliveries = FuturesUnordered::new();
                let mut progress = SettleProgressReporter::new(groups.len(), progress_flush_interval);

                loop {
                    if deliveries.is_empty() && ready_groups.is_empty() {
                        progress.flush(&main_routine_sender);
                        let task =
                            recv_settle_task(&receiver, &mut progress, &main_routine_sender);
                        queue_settle_task(&mut groups, &mut ready_groups, task);
                    }

                    for _ in 0..drain_batch_size {
                        match receiver.try_recv() {
                            Ok(task) => queue_settle_task(&mut groups, &mut ready_groups, task),
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(mpsc::TryRecvError::Disconnected) => {
                                panic!("publish task channel disconnected")
                            }
                        }
                    }

                    let (started, queue_full) = start_ready_settle_tasks(
                        &producer,
                        worker_id,
                        &mut groups,
                        &mut ready_groups,
                        &mut deliveries,
                        backlog.as_ref(),
                        per_group_send_burst,
                        max_outstanding_per_worker,
                    );
                    if started > 1 {
                        debug!(
                            "settle pipeline fill: worker={} batches={} local_outstanding={} per_group_max_outstanding={} worker_max_outstanding={} linger_ms={}",
                            worker_id,
                            started,
                            deliveries.len(),
                            max_outstanding_per_group,
                            max_outstanding_per_worker,
                            producer_linger_ms
                        );
                    }

                    if queue_full {
                        if !deliveries.is_empty() {
                            await_settle_delivery(
                                worker_id,
                                &mut deliveries,
                                &mut groups,
                                &mut ready_groups,
                                &main_routine_sender,
                                backlog.as_ref(),
                                &mut progress,
                            )
                            .await;
                        } else {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        continue;
                    }

                    if started > 0 {
                        continue;
                    }

                    if !deliveries.is_empty() {
                        await_settle_delivery(
                            worker_id,
                            &mut deliveries,
                            &mut groups,
                            &mut ready_groups,
                            &main_routine_sender,
                            backlog.as_ref(),
                            &mut progress,
                        )
                        .await;
                    }
                }
            });
        })
        .expect("spawn settle publish thread");
}

impl Publish {
    pub fn new(
        brokers: String,
        market_name: String,
        main_routine_sender: MainTaskSender,
        pushed_quote_deals_id: u64,
        pushed_settle_message_ids: Vec<u64>,
        output_publish: PublishDriverCfg,
    ) -> Publish {
        let PublishDriverCfg {
            quote_topic,
            quote,
            quote_channel_capacity,
            quote_drain_batch_size,
            quote_max_outstanding,
            settle,
            settle_channel_capacity,
            settle_drain_batch_size,
            settle_max_outstanding,
            settle_worker_max_outstanding,
            settle_per_group_send_burst,
            settle_thread_count,
            progress_flush_interval,
        } = output_publish;

        let (quote_sender, quote_receiver) = mpsc::sync_channel(quote_channel_capacity);
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
        assert!(
            settle_max_outstanding > 0,
            "settle max_outstanding must be > 0"
        );
        assert!(
            settle_worker_max_outstanding > 0,
            "settle worker_max_outstanding must be > 0"
        );
        assert!(
            quote_channel_capacity > 0,
            "quote channel_capacity must be > 0"
        );
        assert!(
            settle_channel_capacity > 0,
            "settle channel_capacity must be > 0"
        );
        let backlog = Arc::new(PublishBacklog::new(
            settle_group_count,
            settle_thread_count,
            quote_channel_capacity,
            quote_max_outstanding,
            settle_channel_capacity,
            settle_max_outstanding,
            settle_worker_max_outstanding,
        ));
        let mut settle_senders = Vec::with_capacity(settle_group_count);

        spawn_quote_publish_thread(
            brokers.clone(),
            market_name.clone(),
            main_routine_sender.clone(),
            quote_topic.clone(),
            pushed_quote_deals_id,
            quote,
            quote_drain_batch_size,
            quote_max_outstanding,
            progress_flush_interval,
            quote_receiver,
            backlog.clone(),
        );
        let groups_per_thread = settle_group_count / settle_thread_count;
        let mut worker_senders = Vec::with_capacity(settle_thread_count);
        let mut settle_worker_by_group = Vec::with_capacity(settle_group_count);
        for worker_id in 0..settle_thread_count {
            let (settle_sender, settle_receiver) = mpsc::sync_channel(settle_channel_capacity);
            let client_id = format!(
                "matchengine.{}.settle-producer.{}of{}",
                market_name,
                worker_id + 1,
                settle_thread_count
            );
            let settle_producer = build_kafka_producer(&brokers, &client_id, &settle, true);
            warm_kafka_producer_metadata(&settle_producer, "settle");
            spawn_settle_publish_thread(
                settle_producer,
                main_routine_sender.clone(),
                worker_id,
                settle_thread_count,
                pushed_settle_message_ids.clone(),
                settle_drain_batch_size,
                settle_max_outstanding,
                settle_worker_max_outstanding,
                settle_per_group_send_burst,
                settle.linger_ms,
                progress_flush_interval,
                settle_receiver,
                backlog.clone(),
            );
            worker_senders.push(settle_sender);
        }
        for group_id in 0..settle_group_count {
            let worker_id = group_id / groups_per_thread;
            settle_senders.push(worker_senders[worker_id].clone());
            settle_worker_by_group.push(worker_id);
        }

        Publish {
            quote_sender,
            quote_topic,
            settle_senders,
            settle_worker_by_group,
            backlog,
        }
    }

    pub fn status_snapshot(&self) -> PublishStatusSnapshot {
        self.backlog.snapshot()
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
        let worker_id = self.settle_worker_by_group[group_id];

        match sender.try_send(message) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(message)) => {
                let blocked_start = Instant::now();
                if let Err(e) = sender.send(message) {
                    self.backlog.dec_settle(group_id);
                    panic!("{} failed.{}", context, e);
                }
                self.backlog
                    .record_settle_channel_blocked(worker_id, blocked_start.elapsed());
            }
            Err(mpsc::TrySendError::Disconnected(message)) => {
                drop(message);
                self.backlog.dec_settle(group_id);
                panic!("{} failed.publish task channel disconnected", context);
            }
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

        debug!("{} {:?}", self.quote_topic, body);

        let message = QuotePublishTaskInfo {
            deals_id: m.deals_id,
            body,
        };

        self.backlog.inc_quote();
        match self.quote_sender.try_send(message) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(message)) => {
                let blocked_start = Instant::now();
                if let Err(e) = self.quote_sender.send(message) {
                    self.backlog.dec_quote();
                    panic!("publish_quote_deal failed.{}", e);
                }
                self.backlog
                    .record_quote_channel_blocked(blocked_start.elapsed());
            }
            Err(mpsc::TrySendError::Disconnected(message)) => {
                drop(message);
                self.backlog.dec_quote();
                panic!("publish_quote_deal failed.publish task channel disconnected");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SettleGroupState, SettleProgressReporter};
    use crate::task::{MainTaskSender, Task};
    use std::time::Duration;

    #[test]
    fn settle_group_progress_advances_contiguous_acks() {
        let mut group = SettleGroupState::new(10, 16);
        group.in_flight = 1;

        assert_eq!(group.advance_pushed_settle_message_id(11), Some(11));
        assert_eq!(group.pushed_settle_message_id, 11);
        assert!(group.acked_settle_message_ids.is_empty());
    }

    #[test]
    fn settle_group_progress_holds_gap_until_missing_ack_arrives() {
        let mut group = SettleGroupState::new(10, 16);
        group.in_flight = 3;

        assert_eq!(group.advance_pushed_settle_message_id(12), None);
        assert_eq!(group.pushed_settle_message_id, 10);

        assert_eq!(group.advance_pushed_settle_message_id(13), None);
        assert_eq!(group.pushed_settle_message_id, 10);

        assert_eq!(group.advance_pushed_settle_message_id(11), Some(13));
        assert_eq!(group.pushed_settle_message_id, 13);
        assert!(group.acked_settle_message_ids.is_empty());
    }

    #[test]
    fn settle_progress_reporter_flushes_dirty_groups_once() {
        let (sender, receiver) = MainTaskSender::channel(8);
        let mut progress = SettleProgressReporter::new(4, Duration::from_millis(1));

        progress.record(1, 10);
        progress.record(1, 12);
        progress.record(3, 7);
        progress.flush(&sender);

        let Task::SettleProgressBatchUpdateTask(task) = receiver.recv().unwrap() else {
            panic!("expected settle progress batch update");
        };
        assert_eq!(task.progresses.len(), 2);
        assert_eq!(task.progresses[0].group_id, 1);
        assert_eq!(task.progresses[0].pushed_settle_message_id, 12);
        assert_eq!(task.progresses[1].group_id, 3);
        assert_eq!(task.progresses[1].pushed_settle_message_id, 7);
        assert!(!progress.has_pending());

        progress.flush(&sender);
        assert!(receiver.try_recv().is_err());
    }
}
