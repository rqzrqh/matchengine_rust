use rust_decimal::Decimal;
use std::str::FromStr;

pub fn parse_decimal_with_max_scale(raw: &str, prec: u32, field: &str) -> Result<Decimal, String> {
    let parsed = Decimal::from_str(raw)
        .map_err(|e| format!("{} decimal parse failed {}: {}", field, raw, e))?;

    if parsed.scale() > prec {
        return Err(format!(
            "{} precision overflow: value {} exceeds scale {}",
            field, raw, prec
        ));
    }

    Ok(parsed)
}
