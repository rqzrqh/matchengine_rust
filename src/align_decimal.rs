//! Quantize helpers so shrinking fractional digits uses **toward-zero** truncation (same sign as
//! libmpdecimal `ROUND_DOWN` on non-negative values), while staying on `rust_decimal`.
//!
//! `Decimal::rescale(dp)` uses midpoint-away-from-zero when **shrinking** scale. Truncating toward
//! zero avoids overspending quote on market buys. When **growing** scale (padding zeros),
//! `rescale` matches the usual fixed-point padding.

use rust_decimal::Decimal;
use rust_decimal::MathematicalOps;
use rust_decimal::RoundingStrategy;

/// If the value has more than `dp` fractional digits, truncate toward zero to `dp` places.
/// Otherwise pad with zeros to `dp` places (same as [`Decimal::rescale`]).
#[inline]
pub fn rescale_down(d: &mut Decimal, dp: u32) {
    if d.scale() > dp {
        *d = d.round_dp_with_strategy(dp, RoundingStrategy::ToZero);
    } else {
        d.rescale(dp);
    }
}

/// Quote budget `left` (money), ask `price` (money per stock unit); returns tradable stock amount
/// at `stock_prec`, matching the market-bid sizing in `engine`.
#[inline]
pub fn market_buy_base_amount(left: Decimal, price: Decimal, stock_prec: u32) -> Decimal {
    let ten = Decimal::new(10, 0);
    let min = ten.powi(-(stock_prec as i64));
    let mut amount = left / price;
    rescale_down(&mut amount, stock_prec);
    loop {
        if amount * price > left {
            amount -= min;
        } else {
            break;
        }
    }
    amount
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn market_buy_base_amount_vectors() {
        let cases = [
            ("10", "2", 2u32, "5"),
            ("100", "3", 2u32, "33.33"),
            ("100", "3", 0u32, "33"),
            ("1", "0.07", 8u32, "14.28571428"),
        ];
        for (left, price, prec, want) in cases {
            let got = market_buy_base_amount(d(left), d(price), prec);
            assert_eq!(got, d(want), "left={left} price={price} stock_prec={prec}");
        }
    }
}
