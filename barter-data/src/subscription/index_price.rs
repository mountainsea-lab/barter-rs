use super::SubscriptionKind;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Barter [`SubscriptionKind`] marker that yields [`IndexPrice`] events.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub struct IndexPrices;

impl SubscriptionKind for IndexPrices {
    type Event = IndexPrice;

    fn as_str(&self) -> &'static str {
        "index_prices"
    }
}

impl std::fmt::Display for IndexPrices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter index price model.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct IndexPrice {
    pub event_time: DateTime<Utc>,
    pub index_price: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn index_prices_kind_formats_as_expected() {
        assert_eq!(IndexPrices.as_str(), "index_prices");
        assert_eq!(IndexPrices.to_string(), "index_prices");
    }

    #[test]
    fn index_price_model_uses_decimal_fields() {
        let time = Utc::now();
        let actual = IndexPrice {
            event_time: time,
            index_price: dec!(11791.23456789),
        };

        assert_eq!(actual.event_time, time);
        assert_eq!(actual.index_price, dec!(11791.23456789));
    }
}
