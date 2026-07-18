use super::SubscriptionKind;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Barter [`SubscriptionKind`] marker that yields [`FundingRate`] events.
///
/// Funding rates are REST-first data in the current architecture. This marker
/// identifies the normalized output type, but does not imply WebSocket stream
/// support or `DynamicStreams` wiring.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub struct FundingRates;

impl SubscriptionKind for FundingRates {
    type Event = FundingRate;

    fn as_str(&self) -> &'static str {
        "funding_rates"
    }
}

impl std::fmt::Display for FundingRates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter funding rate model.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct FundingRate {
    pub funding_time: DateTime<Utc>,
    pub funding_rate: Decimal,
    pub mark_price: Option<Decimal>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    #[test]
    fn funding_rates_kind_formats_as_expected() {
        assert_eq!(FundingRates.as_str(), "funding_rates");
        assert_eq!(FundingRates.to_string(), "funding_rates");
    }

    #[test]
    fn funding_rate_model_uses_decimal_fields() {
        let time = Utc::now();
        let actual = FundingRate {
            funding_time: time,
            funding_rate: dec!(0.00010000),
            mark_price: Some(dec!(65000.25)),
        };

        assert_eq!(actual.funding_time, time);
        assert_eq!(actual.funding_rate, dec!(0.00010000));
        assert_eq!(actual.mark_price, Some(dec!(65000.25)));
    }
}
