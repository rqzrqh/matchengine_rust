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

/// Kafka MessagePack payload unpacked and envelope-validated on the `kafka-consumer` thread; main thread
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

pub enum Task {
    MqTask(KafkaMqTask),
    HttpRequest(HttpRequestTask),
    DumpTask(SqlDumpTask),
    QuoteProgressUpdateTask(QuotePublishProgressTask),
    SettleProgressUpdateTask(SettlePublishProgressTask),
    Terminate,
}
