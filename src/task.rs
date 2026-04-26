use tokio::sync::oneshot;

pub struct KafkaMqTask {
    pub offset: i64,
    pub data: String,
}

#[derive(Debug)]
pub enum RestOp {
    MarketSummary { market: String },
    MarketStatus { market: String },
    OrderDetail { market: String, order_id: u64 },
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
pub struct RestResponse {
    pub status: u16,
    pub body: String,
}

pub struct RestQueryTask {
    pub op: RestOp,
    pub rsp: oneshot::Sender<RestResponse>,
}

pub struct SqlDumpTask {
    pub tm: i64,
}

pub struct PublishProgressTask {
    pub quote_deals_id: u64,
    pub settle_message_ids: Vec<u64>,
}

pub enum Task {
    MqTask(KafkaMqTask),
    RestQuery(RestQueryTask),
    DumpTask(SqlDumpTask),
    ProgressUpdateTask(PublishProgressTask),
    Terminate,
}
