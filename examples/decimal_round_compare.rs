//! Compare `Decimal::rescale` vs `round_dp_with_strategy` (padding vs rounding).
use rust_decimal::prelude::*;
use rust_decimal::RoundingStrategy;
use std::str::FromStr;

fn main() {
    let mut a = Decimal::from_str("1.2").unwrap();
    let r = a.round_dp_with_strategy(4, RoundingStrategy::MidpointAwayFromZero);
    a.rescale(4);
    println!("1.2  round_dp(4)={} scale {} | rescale(4)={} scale {}", r, r.scale(), a, a.scale());

    let mut c = Decimal::from_str("1.2345").unwrap();
    let r2 = c.round_dp_with_strategy(2, RoundingStrategy::MidpointAwayFromZero);
    c.rescale(2);
    println!("1.2345 round_dp(2)={} | rescale(2)={}", r2, c);

    let mut big = Decimal::from_str("11.76470588235294").unwrap();
    big.rescale(28);
    println!("rescale to 28: {} scale {}", big, big.scale());
}
