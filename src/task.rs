use tokio::sync::oneshot;

pub struct KafkaMqTask {
    pub offset: i64,
    pub data: String,
}

pub struct HttpQueryTask {
    pub content: String,
    pub rsp: oneshot::Sender<String>,
}

pub struct SqlDumpTask {
    pub tm: i64,
}

pub enum Task {
    MqTask(KafkaMqTask),
    QueryTask(HttpQueryTask),
    DumpTask(SqlDumpTask),
    Terminate,
}