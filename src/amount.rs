//! Credit amounts as integer base units, with human-credit conversion.
//!
//! CE denominates money in integer **base units** — `1 credit = CREDIT (10^18) base units`,
//! wei-style — never floating point. The HTTP API carries amounts as decimal *strings* (they
//! exceed JavaScript's 2^53 safe-integer limit), so [`Amount`] (de)serializes as a string.

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Base units per credit (10^18).
pub const CREDIT: i128 = 1_000_000_000_000_000_000;

/// A signed credit amount in base units. Used for both balances (which may be negative
/// during sync) and amounts (which are non-negative). The whole supply (2.1e28 base units)
/// fits in `i128` with room to spare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Amount(pub i128);

impl Amount {
    pub const ZERO: Amount = Amount(0);

    /// `n` whole credits.
    pub fn from_credits(n: u64) -> Amount {
        Amount(n as i128 * CREDIT)
    }

    /// Raw base units.
    pub fn from_base(base: i128) -> Amount {
        Amount(base)
    }

    /// The amount in base units.
    pub fn base(self) -> i128 {
        self.0
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Parse a human credit decimal string (`"1000"`, `"1.5"`, `"0.000000000000000001"`).
    /// Up to 18 decimal places.
    pub fn parse_credits(s: &str) -> anyhow::Result<Amount> {
        let s = s.trim();
        let neg = s.starts_with('-');
        let body = s.strip_prefix('-').unwrap_or(s);
        let (whole_str, frac_str) = body.split_once('.').unwrap_or((body, ""));
        if frac_str.len() > 18 {
            anyhow::bail!("amount '{s}' has more than 18 decimal places");
        }
        let whole: i128 = if whole_str.is_empty() {
            0
        } else {
            whole_str.parse().map_err(|_| anyhow::anyhow!("invalid amount '{s}'"))?
        };
        let frac: i128 = format!("{frac_str:0<18}")
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid amount '{s}'"))?;
        let base = whole * CREDIT + frac;
        Ok(Amount(if neg { -base } else { base }))
    }

    /// Format as a human credit decimal string, trimming trailing fractional zeros.
    pub fn credits(self) -> String {
        let sign = if self.0 < 0 { "-" } else { "" };
        let v = self.0.unsigned_abs();
        let whole = v / CREDIT as u128;
        let frac = v % CREDIT as u128;
        if frac == 0 {
            format!("{sign}{whole}")
        } else {
            let frac_str = format!("{frac:018}");
            format!("{sign}{whole}.{}", frac_str.trim_end_matches('0'))
        }
    }
}

impl fmt::Display for Amount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} credits", self.credits())
    }
}

// Wire form: a decimal string of base units (precision-safe across JSON).
impl Serialize for Amount {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for Amount {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.trim().parse::<i128>().map(Amount).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_format_round_trip() {
        for s in ["0", "1", "1000", "1.5", "0.25", "0.000000000000000001", "21000000000"] {
            let a = Amount::parse_credits(s).unwrap();
            assert_eq!(a.credits(), s, "round-trip {s}");
        }
    }

    #[test]
    fn from_credits_and_base() {
        assert_eq!(Amount::from_credits(1).base(), CREDIT);
        assert_eq!(Amount::from_credits(1000).credits(), "1000");
        assert_eq!(Amount(CREDIT / 2).credits(), "0.5");
    }

    #[test]
    fn parse_rejects_too_many_decimals() {
        assert!(Amount::parse_credits("0.0000000000000000001").is_err());
        assert!(Amount::parse_credits("xyz").is_err());
    }

    #[test]
    fn json_is_a_base_unit_string() {
        let a = Amount::from_credits(1);
        let j = serde_json::to_string(&a).unwrap();
        assert_eq!(j, "\"1000000000000000000\"");
        let back: Amount = serde_json::from_str(&j).unwrap();
        assert_eq!(back, a);
    }
}
