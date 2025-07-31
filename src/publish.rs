use crate::market::*;
use std::{thread, str, sync::mpsc, time::Duration};
use std::rc::Rc;
use rust_decimal::prelude::*;
use json::*;
use kafka::producer::{Producer, Record, RequiredAcks};
use crate::task::*;

pub struct Publish {
    sender: mpsc::Sender<Box<OutputPublishTask>>,
    settle_group_count:u32,
}

struct QuotePublishTaskInfo {
    deals_id:u64,
    topic:String,
    data:JsonValue,
}

struct SettlePublishTaskInfo {
    group_id:u32,
    message_id:u64,
    topic:String,
    data:JsonValue,
}

enum OutputPublishTask {
    QuotePublishTask(QuotePublishTaskInfo),
    SettlePublishTask(SettlePublishTaskInfo),
}

impl Publish {
    pub fn new(brokers: String, main_routine_sender: mpsc::Sender<Task>, quote_deals_id: u64, settle_message_ids: Vec<u64>) -> Publish {
        let (sender, receiver) = mpsc::channel();
        let settle_group_count = settle_message_ids.len() as u32;

        thread::spawn(move || {
            let mut producer = Producer::from_hosts(vec!(brokers.to_owned()))
                    .with_ack_timeout(Duration::from_secs(1))
                    .with_required_acks(RequiredAcks::One)
                    .create()
                    .unwrap();
            let mut quote_deals_id = quote_deals_id;
            let mut settle_message_ids:Vec<u64> = settle_message_ids.clone();

            loop {
                let task:Box<OutputPublishTask> = receiver.recv().unwrap();
                let x = task.as_ref();
                match x {
                    OutputPublishTask::QuotePublishTask(tsk) => {
                        if tsk.deals_id > quote_deals_id {
                            let topic = &tsk.topic;
                            let data = &tsk.data;
                            producer.send(&Record::from_value(&topic, data.to_string().as_bytes())).unwrap();
                            quote_deals_id = tsk.deals_id;

                            let task = PublishProgressTask{
                                quote_deals_id: quote_deals_id,
                                settle_message_ids: settle_message_ids.clone(),
                            };
                            main_routine_sender.send(Task::ProgressUpdateTask(task)).expect("send progress update task failed");
                        }
                    },
                    OutputPublishTask::SettlePublishTask(tsk) => {

                        let group_id = tsk.group_id as usize;

                        if tsk.message_id > settle_message_ids[group_id] {
                            let topic = &tsk.topic;
                            let data = &tsk.data;
                            producer.send(&Record::from_value(&topic, data.to_string().as_bytes())).unwrap();
                            settle_message_ids[group_id] = tsk.message_id;

                            let task = PublishProgressTask{
                                quote_deals_id: quote_deals_id,
                                settle_message_ids: settle_message_ids.clone(),
                            };
                            main_routine_sender.send(Task::ProgressUpdateTask(task)).expect("send progress update task failed");
                        }
                    }
                }
            }
        });

        Publish {
            sender: sender,
            settle_group_count: settle_group_count,
        }
    }

    pub fn publish_put_order(&self, m: &Market, extern_id: u64, order: &Rc<Order>) {

        let mut object = JsonValue::new_object();
        object["type"] = "put_order".into();
        object["market"] = m.name.clone().into();
        object["msgid"] = m.message_id.into();
        object["txid"] = extern_id.into();
        object["order"] = order.to_json();

        let group_id = order.user_id%self.settle_group_count;
        let topic = format!("settle.{}", group_id);

        info!("{} {}", topic, object);

        let message = SettlePublishTaskInfo {
            group_id:group_id,
            message_id:m.message_id,
            topic: topic,
            data: object,
        };

        self.sender.send(Box::new(OutputPublishTask::SettlePublishTask(message))).expect("send failed");
    }

    pub fn publish_cancel_order(&self, m: &Market, extern_id: u64, order: &Rc<Order>) {

        let mut object = JsonValue::new_object();
        object["type"] = "cancel_order".into();
        object["market"] = m.name.clone().into();
        object["msgid"] = m.message_id.into();
        object["txid"] = extern_id.into();
        object["order"] = order.to_json();

        let group_id = order.user_id%self.settle_group_count;
        let topic = format!("settle.{}", group_id);

        info!("{} {}", topic, object);

        let message = SettlePublishTaskInfo {
            group_id:group_id,
            message_id:m.message_id,
            topic: topic,
            data: object,
        };

        self.sender.send(Box::new(OutputPublishTask::SettlePublishTask(message))).expect("send failed");
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
        let topic = format!("settle.{}", group_id);

        info!("{} {}", topic, object);

        let message = SettlePublishTaskInfo {
            group_id:group_id,
            message_id:m.message_id,
            topic: topic,
            data: object,
        };

        self.sender.send(Box::new(OutputPublishTask::SettlePublishTask(message))).expect("send failed");
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
        let topic = format!("settle.{}", group_id);

        info!("{} {}", topic, object);

        let message = SettlePublishTaskInfo {
            group_id:group_id,
            message_id:m.message_id,
            topic: topic,
            data: object,
        };

        self.sender.send(Box::new(OutputPublishTask::SettlePublishTask(message))).expect("send failed");
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

        info!("{} {}", topic, object);

        let message = QuotePublishTaskInfo {
            deals_id: m.deals_id,
            topic: topic,
            data: object,
        };

        self.sender.send(Box::new(OutputPublishTask::QuotePublishTask(message))).expect("send failed");
    }
}
