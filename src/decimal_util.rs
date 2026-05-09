use rust_decimal::Decimal;
use std::str::FromStr;

pub fn parse_decimal_with_max_scale(raw: &str, prec: u32, field: &str) -> Result<Decimal, String> {
    let parsed = Decimal::from_str(raw)
        .map_err(|e| format!("{} decimal parse failed {}: {}", field, raw, e))?;
    let normalized = parsed.normalize();

    if normalized.scale() > prec {
        return Err(format!(
            "{} precision overflow: value {} exceeds scale {}",
            field, raw, prec
        ));
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_extra_trailing_zeroes_within_effective_scale() {
        let parsed = parse_decimal_with_max_scale("0.30", 1, "left").unwrap();

        assert_eq!(parsed, Decimal::from_str("0.3").unwrap());
        assert_eq!(parsed.scale(), 1);
    }

    #[test]
    fn parse_rejects_values_with_too_many_significant_decimal_places() {
        let err = parse_decimal_with_max_scale("0.31", 1, "left").unwrap_err();

        assert_eq!(err, "left precision overflow: value 0.31 exceeds scale 1");
    }
}
