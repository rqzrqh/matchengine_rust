use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use tokio::sync::oneshot;

pub const MQ_METHOD_ORDER_PUT_LIMIT: u32 = 1;
pub const MQ_METHOD_ORDER_PUT_MARKET: u32 = 2;
pub const MQ_METHOD_ORDER_CANCEL: u32 = 3;

pub const QUOTE_MSG_TYPE_DEAL: u32 = 1;

pub const SETTLE_MSG_TYPE_PUT_ORDER: u32 = 1;
pub const SETTLE_MSG_TYPE_CANCEL_ORDER: u32 = 2;
pub const SETTLE_MSG_TYPE_ERROR: u32 = 3;
pub const SETTLE_MSG_TYPE_DEALS: u32 = 4;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum MqParams {
    PutLimit(PutLimitParams),
    PutMarket(PutMarketParams),
    Cancel(CancelParams),
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PutLimitParams {
    pub user_id: u32,
    pub side: u32,
    pub amount: String,
    pub price: String,
    pub taker_fee_rate: String,
    pub maker_fee_rate: String,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PutMarketParams {
    pub user_id: u32,
    pub side: u32,
    pub amount: String,
    pub taker_fee_rate: String,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CancelParams {
    pub user_id: u32,
    pub order_id: u64,
}

#[derive(Debug, serde::Deserialize)]
pub struct MqPayload {
    pub method: u32,
    pub id: u64,
    pub params: MqParams,
    pub input_sequence_id: u64,
}

/// Kafka MessagePack payload unpacked and envelope-validated on the `offer-consumer` thread; main thread
/// applies `input_sequence_id` checks and matcher updates.
pub struct KafkaMqTask {
    pub offset: i64,
    pub payload: Result<MqPayload, String>,
}

#[derive(Debug)]
pub enum HttpOp {
    MarketSummary {
        market: String,
    },
    MarketStatus {
        market: String,
    },
    OrderDetail {
        market: String,
        order_id: u64,
    },
    OrderBook {
        market: String,
        side: u32,
        offset: u32,
        limit: u32,
    },
    UserOrdersPending {
        market: String,
        user_id: u32,
        offset: u32,
        limit: u32,
    },
}

#[derive(Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}

pub struct HttpRequestTask {
    pub op: HttpOp,
    pub rsp: oneshot::Sender<HttpResponse>,
}

pub struct SqlDumpTask {
    pub tm: i64,
}

pub struct QuotePublishProgressTask {
    pub pushed_quote_deals_id: u64,
}

pub struct SettlePublishProgressTask {
    pub group_id: usize,
    pub pushed_settle_message_id: u64,
}

pub struct SettlePublishProgressBatchTask {
    pub progresses: Vec<SettlePublishProgressTask>,
}

pub enum Task {
    MqTask(KafkaMqTask),
    HttpRequest(HttpRequestTask),
    DumpTask(SqlDumpTask),
    QuoteProgressUpdateTask(QuotePublishProgressTask),
    SettleProgressBatchUpdateTask(SettlePublishProgressBatchTask),
    Terminate,
}

#[derive(Copy, Clone)]
enum MainTaskQueueKind {
    Offer,
    Http,
    Dump,
    QuoteProgress,
    SettleProgress,
    Terminate,
}

impl Task {
    fn queue_kind(&self) -> MainTaskQueueKind {
        match self {
            Task::MqTask(_) => MainTaskQueueKind::Offer,
            Task::HttpRequest(_) => MainTaskQueueKind::Http,
            Task::DumpTask(_) => MainTaskQueueKind::Dump,
            Task::QuoteProgressUpdateTask(_) => MainTaskQueueKind::QuoteProgress,
            Task::SettleProgressBatchUpdateTask(_) => MainTaskQueueKind::SettleProgress,
            Task::Terminate => MainTaskQueueKind::Terminate,
        }
    }
}

#[derive(Clone)]
pub struct MainTaskSender {
    sender: mpsc::SyncSender<Task>,
    status: Arc<MainTaskQueueStatus>,
}

impl MainTaskSender {
    pub fn channel(capacity: usize) -> (Self, mpsc::Receiver<Task>) {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        let status = Arc::new(MainTaskQueueStatus::new(capacity));
        (Self { sender, status }, receiver)
    }

    pub fn send(&self, task: Task) -> Result<(), mpsc::SendError<Task>> {
        let kind = task.queue_kind();
        self.status.record_send_attempt(kind);
        match self.sender.try_send(task) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(task)) => {
                self.status.record_queue_full();
                self.sender.send(task).inspect_err(|_| {
                    self.status.record_send_failed(kind);
                })
            }
            Err(mpsc::TrySendError::Disconnected(task)) => {
                self.status.record_send_failed(kind);
                Err(mpsc::SendError(task))
            }
        }
    }

    pub fn mark_dequeued(&self, task: &Task) {
        self.status.record_dequeued(task.queue_kind());
    }

    pub fn status_snapshot(&self) -> MainTaskQueueSnapshot {
        self.status.snapshot()
    }
}

struct MainTaskQueueStatus {
    capacity: usize,
    pending_or_blocked: AtomicUsize,
    max_pending_or_blocked: AtomicUsize,
    queue_full_count: AtomicUsize,
    offer_pending_or_blocked: AtomicUsize,
    http_pending_or_blocked: AtomicUsize,
    dump_pending_or_blocked: AtomicUsize,
    quote_progress_pending_or_blocked: AtomicUsize,
    settle_progress_pending_or_blocked: AtomicUsize,
    terminate_pending_or_blocked: AtomicUsize,
}

impl MainTaskQueueStatus {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            pending_or_blocked: AtomicUsize::new(0),
            max_pending_or_blocked: AtomicUsize::new(0),
            queue_full_count: AtomicUsize::new(0),
            offer_pending_or_blocked: AtomicUsize::new(0),
            http_pending_or_blocked: AtomicUsize::new(0),
            dump_pending_or_blocked: AtomicUsize::new(0),
            quote_progress_pending_or_blocked: AtomicUsize::new(0),
            settle_progress_pending_or_blocked: AtomicUsize::new(0),
            terminate_pending_or_blocked: AtomicUsize::new(0),
        }
    }

    fn counter_for_kind(&self, kind: MainTaskQueueKind) -> &AtomicUsize {
        match kind {
            MainTaskQueueKind::Offer => &self.offer_pending_or_blocked,
            MainTaskQueueKind::Http => &self.http_pending_or_blocked,
            MainTaskQueueKind::Dump => &self.dump_pending_or_blocked,
            MainTaskQueueKind::QuoteProgress => &self.quote_progress_pending_or_blocked,
            MainTaskQueueKind::SettleProgress => &self.settle_progress_pending_or_blocked,
            MainTaskQueueKind::Terminate => &self.terminate_pending_or_blocked,
        }
    }

    fn record_send_attempt(&self, kind: MainTaskQueueKind) {
        let pending = self.pending_or_blocked.fetch_add(1, Ordering::Relaxed) + 1;
        self.counter_for_kind(kind).fetch_add(1, Ordering::Relaxed);
        let mut observed = self.max_pending_or_blocked.load(Ordering::Relaxed);
        while pending > observed {
            match self.max_pending_or_blocked.compare_exchange_weak(
                observed,
                pending,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => observed = current,
            }
        }
    }

    fn record_queue_full(&self) {
        self.queue_full_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_send_failed(&self, kind: MainTaskQueueKind) {
        self.pending_or_blocked.fetch_sub(1, Ordering::Relaxed);
        self.counter_for_kind(kind).fetch_sub(1, Ordering::Relaxed);
    }

    fn record_dequeued(&self, kind: MainTaskQueueKind) {
        let prev = self.pending_or_blocked.fetch_sub(1, Ordering::Relaxed);
        let kind_prev = self.counter_for_kind(kind).fetch_sub(1, Ordering::Relaxed);
        debug_assert!(prev > 0, "main task queue backlog underflow");
        debug_assert!(kind_prev > 0, "main task queue kind backlog underflow");
    }

    fn snapshot(&self) -> MainTaskQueueSnapshot {
        MainTaskQueueSnapshot {
            capacity: self.capacity,
            pending_or_blocked: self.pending_or_blocked.load(Ordering::Relaxed),
            max_pending_or_blocked: self.max_pending_or_blocked.load(Ordering::Relaxed),
            queue_full_count: self.queue_full_count.load(Ordering::Relaxed),
            offer_pending_or_blocked: self.offer_pending_or_blocked.load(Ordering::Relaxed),
            http_pending_or_blocked: self.http_pending_or_blocked.load(Ordering::Relaxed),
            dump_pending_or_blocked: self.dump_pending_or_blocked.load(Ordering::Relaxed),
            quote_progress_pending_or_blocked: self
                .quote_progress_pending_or_blocked
                .load(Ordering::Relaxed),
            settle_progress_pending_or_blocked: self
                .settle_progress_pending_or_blocked
                .load(Ordering::Relaxed),
            terminate_pending_or_blocked: self.terminate_pending_or_blocked.load(Ordering::Relaxed),
        }
    }
}

pub struct MainTaskQueueSnapshot {
    pub capacity: usize,
    pub pending_or_blocked: usize,
    pub max_pending_or_blocked: usize,
    pub queue_full_count: usize,
    pub offer_pending_or_blocked: usize,
    pub http_pending_or_blocked: usize,
    pub dump_pending_or_blocked: usize,
    pub quote_progress_pending_or_blocked: usize,
    pub settle_progress_pending_or_blocked: usize,
    pub terminate_pending_or_blocked: usize,
}
