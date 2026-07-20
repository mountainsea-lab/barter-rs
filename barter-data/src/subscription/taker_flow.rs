use super::SubscriptionKind;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Barter [`SubscriptionKind`] marker that yields [`TakerFlow`] events.
///
/// Taker flow is REST-first data in the current architecture. This marker
/// identifies the normalized output type, but does not imply WebSocket stream
/// support or `DynamicStreams` wiring.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub struct TakerFlows;

impl SubscriptionKind for TakerFlows {
    type Event = TakerFlow;

    fn as_str(&self) -> &'static str {
        "taker_flows"
    }
}

impl std::fmt::Display for TakerFlows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter taker buy/sell volume model.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct TakerFlow {
    pub period_start: DateTime<Utc>,
    pub buy_volume: Decimal,
    pub sell_volume: Decimal,
    pub buy_sell_ratio: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    #[test]
    fn taker_flows_kind_formats_as_expected() {
        assert_eq!(TakerFlows.as_str(), "taker_flows");
        assert_eq!(TakerFlows.to_string(), "taker_flows");
    }

    #[test]
    fn taker_flow_model_uses_decimal_fields() {
        let time = Utc::now();
        let actual = TakerFlow {
            period_start: time,
            buy_volume: dec!(387.3300),
            sell_volume: dec!(270.0700),
            buy_sell_ratio: dec!(1.4342),
        };

        assert_eq!(actual.period_start, time);
        assert_eq!(actual.buy_volume, dec!(387.3300));
        assert_eq!(actual.sell_volume, dec!(270.0700));
        assert_eq!(actual.buy_sell_ratio, dec!(1.4342));
    }
}
