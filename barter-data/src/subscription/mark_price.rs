use super::SubscriptionKind;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Barter [`SubscriptionKind`] marker that yields [`MarkPrice`] events.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub struct MarkPrices;

impl SubscriptionKind for MarkPrices {
    type Event = MarkPrice;

    fn as_str(&self) -> &'static str {
        "mark_prices"
    }
}

impl std::fmt::Display for MarkPrices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter mark price model.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct MarkPrice {
    pub event_time: DateTime<Utc>,
    pub mark_price: Decimal,
    pub index_price: Decimal,
    pub estimated_settle_price: Option<Decimal>,
    pub last_funding_rate: Option<Decimal>,
    pub interest_rate: Option<Decimal>,
    pub next_funding_time: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn mark_prices_kind_formats_as_expected() {
        assert_eq!(MarkPrices.as_str(), "mark_prices");
        assert_eq!(MarkPrices.to_string(), "mark_prices");
    }

    #[test]
    fn mark_price_model_uses_decimal_fields() {
        let time = Utc::now();
        let actual = MarkPrice {
            event_time: time,
            mark_price: dec!(11793.63104562),
            index_price: dec!(11791.23456789),
            estimated_settle_price: Some(dec!(11790.11111111)),
            last_funding_rate: Some(dec!(0.00010000)),
            interest_rate: Some(dec!(0.00010000)),
            next_funding_time: Some(time),
        };

        assert_eq!(actual.event_time, time);
        assert_eq!(actual.mark_price, dec!(11793.63104562));
        assert_eq!(actual.index_price, dec!(11791.23456789));
        assert_eq!(actual.estimated_settle_price, Some(dec!(11790.11111111)));
        assert_eq!(actual.last_funding_rate, Some(dec!(0.00010000)));
        assert_eq!(actual.interest_rate, Some(dec!(0.00010000)));
        assert_eq!(actual.next_funding_time, Some(time));
    }
}
