use std::collections::HashMap;
use std::rc::Rc;
use std::cell::Cell;
use json::*;
use skiplist::OrderedSkipList;
use std::cmp::Ordering;
use rust_decimal::prelude::*;

pub static MARKET_ORDER_TYPE_LIMIT:u32 = 1;
pub static MARKET_ORDER_TYPE_MARKET:u32 = 2;

pub static MARKET_ORDER_SIDE_ASK:u32 = 1;
pub static MARKET_ORDER_SIDE_BID:u32 = 2;

pub static MARKET_ROLE_MAKER:u32 = 1;
pub static MARKET_ROLE_TAKER:u32 = 2;

pub struct Order {
    pub id: u64,
    pub order_type: u32,
    pub side: u32,
    pub create_time: i64,
    pub update_time: Cell<i64>,
    pub user_id: u32,
    pub price: Decimal,
    pub amount: Cell<Decimal>,
    pub taker_fee_rate: Decimal,
    pub maker_fee_rate: Decimal,
    pub left: Cell<Decimal>,
    pub deal_stock: Cell<Decimal>,
    pub deal_money: Cell<Decimal>,
    pub deal_fee: Cell<Decimal>,
}

impl Order {
    pub fn to_json(&self) -> JsonValue {
        let mut object = JsonValue::new_object();

        object["id"] = self.id.into();
        object["type"] = self.order_type.into();
        object["side"] = self.side.into();
        object["create_time"] = self.create_time.into();
        object["update_time"] = self.update_time.get().into();
        object["user_id"] = self.user_id.into();
        object["price"] = self.price.to_string().into();
        object["amount"] = self.amount.get().to_string().into();
        object["taker_fee_rate"] = self.taker_fee_rate.to_string().into();
        object["maker_fee_rate"] = self.maker_fee_rate.to_string().into();
        object["left"] = self.left.get().to_string().into();
        object["deal_stock"] = self.deal_stock.get().to_string().into();
        object["deal_money"] = self.deal_money.get().to_string().into();
        object["deal_fee"] = self.deal_fee.get().to_string().into();

        object
    }
}

impl PartialOrd for Order {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {

        if self.id == other.id {
            Some(Ordering::Equal)
        } else {
            let mut ret = Some(Ordering::Equal);

            if self.side == MARKET_ORDER_SIDE_ASK {
                ret = self.price.partial_cmp(&other.price);
            } else {
                ret = other.price.partial_cmp(&self.price);
            }

            if ret == Some(Ordering::Equal) {
                Some(self.id.cmp(&other.id))
            } else {
                ret
            }
        }
    }
}

impl PartialEq for Order { 
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

pub struct Market {
    pub oper_id: u64,
    pub order_id: u64,
    pub deals_id: u64,
    pub message_id: u64,
    pub input_offset: i64,

    pub name: String,

    pub stock_prec: u32,
    pub money_prec: u32,
    pub fee_rate_prec: u32,
    pub min_amount: Decimal,
    orders: HashMap<u64, Rc<Order>>,
    users: HashMap<u32, OrderedSkipList<Rc<Order>>>,
    pub asks: OrderedSkipList<Rc<Order>>,
    pub bids: OrderedSkipList<Rc<Order>>,

    pub stock_amount: Decimal,
    pub money_amount: Decimal,
}

impl Market {

    pub fn new(name:&String, stock_prec: u32, money_prec: u32, fee_rate: u32, min_amount: &Decimal) -> Market {
        Market {
            oper_id: 0,
            order_id: 0,
            deals_id: 0,
            message_id: 0,
            input_offset: 0,
            name : name.clone(),
            stock_prec: stock_prec,
            money_prec: money_prec,
            fee_rate_prec: fee_rate,
            min_amount: min_amount.clone(),

            orders: HashMap::new(),
            users: HashMap::new(),
            asks: OrderedSkipList::new(),
            bids: OrderedSkipList::new(),
            stock_amount: Decimal::ZERO,
            money_amount: Decimal::ZERO,
        }
    }

    pub fn check_user_order_limit(&self, user_id: u32) -> bool {
        match self.users.get(&user_id) {
            Some(order_list) => {
                if order_list.len() > 10 {
                    false
                } else {
                    true
                }
            }
            None => true,
        }
    }

    // panic
    pub fn put_order(&mut self, order: Rc<Order>) {
        if order.order_type != MARKET_ORDER_TYPE_LIMIT {
            return;
        }

        if order.side == MARKET_ORDER_SIDE_ASK {
            self.stock_amount = self.stock_amount + order.left.get();
        } else {
            self.money_amount = self.money_amount + (order.price*order.left.get());
        }

        self.orders.insert(order.id, order.clone());
        self.users.entry(order.user_id).or_insert_with(|| OrderedSkipList::new()).insert(order.clone());

        if order.side == MARKET_ORDER_SIDE_ASK {
            self.asks.insert(order);
        } else {
            // other calc?
            self.bids.insert(order);
        }
    }

    pub fn order_finish(&mut self, order: &Rc<Order>) {

        let side = order.side;

        if side == MARKET_ORDER_SIDE_ASK {
            self.asks.remove(&order);
        } else {
            self.bids.remove(&order);
        }

        self.orders.remove(&order.id);
        let lst = self.users.get_mut(&order.user_id).unwrap();
        lst.remove(&order);
        if lst.len() == 0 {
            self.users.remove(&order.user_id);
        }
    }

    pub fn get_order(&self, order_id: &u64) -> Option<&Rc<Order>> {
        self.orders.get(order_id)
    }

    pub fn get_user_order_list(&self, user_id: &u32) -> Option<&OrderedSkipList<Rc<Order>>> {
        self.users.get(user_id)
    }
}
