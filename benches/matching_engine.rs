use std::rc::Rc;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use matchengine_rust::engine::{market_put_limit_order, market_put_market_order};
use matchengine_rust::market::{
    MARKET_ORDER_SIDE_ASK, MARKET_ORDER_SIDE_BID, Market, Order, USER_SETTLE_GROUP_SIZE,
};
use matchengine_rust::publish::MatchPublisher;
use rust_decimal::Decimal;
use std::hint::black_box;

const CORE_BENCH_ORDER_COUNT: u64 = 10_000;

struct NoopPublisher;

impl MatchPublisher for NoopPublisher {
    fn publish_put_order(&self, m: &mut Market, _extern_id: u64, order: &Rc<Order>) {
        black_box(m.next_settle_message_id(order.user_id));
    }

    fn publish_deal(
        &self,
        m: &mut Market,
        _extern_id: u64,
        _tm: i64,
        user_id: u32,
        _rival_user_id: u32,
        _order_id: u64,
        _role: u32,
        _price: &Decimal,
        _amount: &Decimal,
        _deal: &Decimal,
        _fee: &Decimal,
        _rival_fee: &Decimal,
    ) {
        black_box(m.next_settle_message_id(user_id));
    }

    fn publish_quote_deal(
        &self,
        _m: &Market,
        _tm: i64,
        _price: &Decimal,
        _amount: &Decimal,
        _side: u32,
    ) {
    }
}

fn new_market() -> Market {
    Market::new(&"eth_btc".to_string(), 4, 8, 4, &Decimal::new(1, 4))
}

fn amount() -> Decimal {
    Decimal::new(1, 0)
}

fn fee_rate() -> Decimal {
    Decimal::new(1, 3)
}

fn seed_ask_book(publisher: &NoopPublisher, order_count: u64) -> Market {
    let mut market = new_market();
    for i in 0..order_count {
        let price = Decimal::new(100_0000 + (i % 100) as i64, 4);
        market_put_limit_order(
            publisher,
            &mut market,
            i + 1,
            (i % (u32::MAX as u64)) as u32,
            MARKET_ORDER_SIDE_ASK,
            amount(),
            price,
            fee_rate(),
            fee_rate(),
        )
        .unwrap();
    }
    market
}

fn bench_build_limit_book(c: &mut Criterion) {
    let publisher = NoopPublisher;
    let mut group = c.benchmark_group("matching_engine");
    group.throughput(Throughput::Elements(CORE_BENCH_ORDER_COUNT));
    group.bench_function("resting_limit_order_core_throughput", |b| {
        b.iter(|| {
            let mut market = new_market();
            for i in 0..CORE_BENCH_ORDER_COUNT {
                let side = if i % 2 == 0 {
                    MARKET_ORDER_SIDE_ASK
                } else {
                    MARKET_ORDER_SIDE_BID
                };
                let price = if side == MARKET_ORDER_SIDE_ASK {
                    Decimal::new(110_0000 + (i % 100) as i64, 4)
                } else {
                    Decimal::new(90_0000 - (i % 100) as i64, 4)
                };

                market_put_limit_order(
                    &publisher,
                    &mut market,
                    black_box(i + 1),
                    black_box((i % (u32::MAX as u64)) as u32),
                    side,
                    amount(),
                    price,
                    fee_rate(),
                    fee_rate(),
                )
                .unwrap();
            }
            black_box(market);
        });
    });
    group.finish();
}

fn bench_cross_limit_orders(c: &mut Criterion) {
    let publisher = NoopPublisher;
    let mut group = c.benchmark_group("matching_engine");
    group.throughput(Throughput::Elements(CORE_BENCH_ORDER_COUNT));
    group.bench_function("cross_limit_order_core_throughput", |b| {
        b.iter_batched(
            || seed_ask_book(&publisher, CORE_BENCH_ORDER_COUNT),
            |mut market| {
                for i in 0..CORE_BENCH_ORDER_COUNT {
                    market_put_limit_order(
                        &publisher,
                        &mut market,
                        black_box(CORE_BENCH_ORDER_COUNT + i + 1),
                        black_box((CORE_BENCH_ORDER_COUNT + i) as u32),
                        MARKET_ORDER_SIDE_BID,
                        amount(),
                        Decimal::new(100_0100, 4),
                        fee_rate(),
                        fee_rate(),
                    )
                    .unwrap();
                }
                black_box(market);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_market_order_sweep(c: &mut Criterion) {
    let publisher = NoopPublisher;
    let mut group = c.benchmark_group("matching_engine");
    group.throughput(Throughput::Elements(CORE_BENCH_ORDER_COUNT));
    group.bench_function("market_order_sweep_core_deal_throughput", |b| {
        b.iter_batched(
            || seed_ask_book(&publisher, CORE_BENCH_ORDER_COUNT),
            |mut market| {
                market_put_market_order(
                    &publisher,
                    &mut market,
                    black_box(CORE_BENCH_ORDER_COUNT + 1),
                    black_box((CORE_BENCH_ORDER_COUNT + 1) as u32),
                    MARKET_ORDER_SIDE_BID,
                    Decimal::new(100_0100 * CORE_BENCH_ORDER_COUNT as i64, 4),
                    fee_rate(),
                )
                .unwrap();
                black_box(market);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn assert_settle_group_count() {
    assert_eq!(USER_SETTLE_GROUP_SIZE, 64);
}

fn criterion_benchmark(c: &mut Criterion) {
    assert_settle_group_count();
    bench_build_limit_book(c);
    bench_cross_limit_orders(c);
    bench_market_order_sweep(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
