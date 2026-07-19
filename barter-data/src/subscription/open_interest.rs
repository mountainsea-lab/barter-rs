use super::SubscriptionKind;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Barter [`SubscriptionKind`] marker that yields [`OpenInterest`] events.
///
/// Open interest is REST-first data in the current architecture. This marker
/// identifies the normalized output type, but does not imply WebSocket stream
/// support or `DynamicStreams` wiring.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub struct OpenInterests;

impl SubscriptionKind for OpenInterests {
    type Event = OpenInterest;

    fn as_str(&self) -> &'static str {
        "open_interests"
    }
}

impl std::fmt::Display for OpenInterests {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter open interest model.
///
/// `open_interest` is the quantity reported by the source exchange for the
/// futures symbol. For Binance USD-M perpetual contracts this should be treated
/// as base-asset quantity, not quote notional.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct OpenInterest {
    pub event_time: DateTime<Utc>,
    pub open_interest: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    #[test]
    fn open_interests_kind_formats_as_expected() {
        assert_eq!(OpenInterests.as_str(), "open_interests");
        assert_eq!(OpenInterests.to_string(), "open_interests");
    }

    #[test]
    fn open_interest_model_uses_decimal_quantity() {
        let time = Utc::now();
        let actual = OpenInterest {
            event_time: time,
            open_interest: dec!(10659.509),
        };

        assert_eq!(actual.event_time, time);
        assert_eq!(actual.open_interest, dec!(10659.509));
    }
}
