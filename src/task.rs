use tokio::sync::oneshot;

/// Kafka payload parsed and envelope-validated on the `kafka-consumer` thread; main thread
/// applies `input_sequence_id` checks and matcher updates.
pub struct KafkaMqTask {
    pub offset: i64,
    pub payload: Result<json::JsonValue, String>,
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

pub enum Task {
    MqTask(KafkaMqTask),
    HttpRequest(HttpRequestTask),
    DumpTask(SqlDumpTask),
    QuoteProgressUpdateTask(QuotePublishProgressTask),
    SettleProgressUpdateTask(SettlePublishProgressTask),
    Terminate,
}
