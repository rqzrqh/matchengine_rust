use crate::market::*;
use std::sync::mpsc;
use std::rc::Rc;
use rust_decimal::prelude::*;
use json::*;

pub static USER_GROUP_SIZE:u32 = 64;

pub struct PublishInfo {
    pub topic: String,
    pub data: JsonValue,
}

pub struct Publish {
    sender: mpsc::Sender<Box<PublishInfo>>,
}

impl Publish {
    pub fn new(sender: mpsc::Sender<Box<PublishInfo>>) -> Publish {
        Publish {
            sender: sender,
        }
    }

    pub fn publish_order(&self, m: &mut Market, extern_id: u64, order: &Rc<Order>) {

        m.message_id += 1;

        let mut object = JsonValue::new_object();
        object["type"] = "order".into();
        object["market"] = m.name.clone().into();
        object["msgid"] = m.message_id.into();
        object["txid"] = extern_id.into();
        object["order"] = order.to_json();

        let topic = format!("settle.{}", order.user_id%USER_GROUP_SIZE);

        info!("{} {}", topic, object);

        let message = PublishInfo {
            topic: topic,
            data: object,
        };

        self.sender.send(Box::new(message)).expect("send failed");
    }

    pub fn publish_cancel_order(&self, m: &mut Market, extern_id: u64, order: &Rc<Order>) {

        m.message_id += 1;

        let mut object = JsonValue::new_object();
        object["type"] = "cancel_order".into();
        object["market"] = m.name.clone().into();
        object["msgid"] = m.message_id.into();
        object["txid"] = extern_id.into();
        object["order"] = order.to_json();

        let topic = format!("settle.{}", order.user_id%USER_GROUP_SIZE);

        info!("{} {}", topic, object);

        let message = PublishInfo {
            topic: topic,
            data: object,
        };

        self.sender.send(Box::new(message)).expect("send failed");
    }

    pub fn publish_deal(&self, m: &mut Market, extern_id: u64, tm: i64, user_id: u32, rival_user_id: u32, order_id: u64, role: u32,
            price: &Decimal, amount: &Decimal, deal: &Decimal, fee: &Decimal, rival_fee: &Decimal) {

        m.message_id += 1;

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

        let topic = format!("settle.{}", user_id%USER_GROUP_SIZE);

        info!("{} {}", topic, object);

        let message = PublishInfo {
            topic: topic,
            data: object,
        };

        self.sender.send(Box::new(message)).expect("send failed");
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

        let topic = format!("settle.{}", user_id%USER_GROUP_SIZE);

        info!("{} {}", topic, object);

        let message = PublishInfo {
            topic: topic,
            data: object,
        };

        self.sender.send(Box::new(message)).expect("send failed");
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

        let message = PublishInfo {
            topic: topic,
            data: object,
        };

        self.sender.send(Box::new(message)).expect("send failed");
    }
}
