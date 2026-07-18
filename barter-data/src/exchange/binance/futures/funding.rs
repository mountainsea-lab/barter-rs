use crate::{
    event::{MarketEvent, MarketIter},
    subscription::funding::FundingRate,
};
use barter_instrument::exchange::ExchangeId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// [`crate::exchange::binance::futures::BinanceFuturesUsd`] HTTP funding rate url.
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#get-funding-rate-history>
pub const HTTP_FUNDING_RATE_URL_BINANCE_FUTURES_USD: &str =
    "https://fapi.binance.com/fapi/v1/fundingRate";

/// Binance USD-M Futures funding rate REST row.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesFundingRate {
    #[serde(alias = "symbol")]
    pub symbol: String,
    #[serde(
        alias = "fundingRate",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub funding_rate: Decimal,
    #[serde(
        alias = "fundingTime",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub funding_time: DateTime<Utc>,
    #[serde(
        alias = "markPrice",
        default,
        deserialize_with = "deserialize_optional_decimal_string"
    )]
    pub mark_price: Option<Decimal>,
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceFuturesFundingRate)>
    for MarketIter<InstrumentKey, FundingRate>
{
    fn from(
        (exchange_id, instrument, input): (ExchangeId, InstrumentKey, BinanceFuturesFundingRate),
    ) -> Self {
        Self(vec![Ok(MarketEvent {
            time_exchange: input.funding_time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: FundingRate {
                funding_time: input.funding_time,
                funding_rate: input.funding_rate,
                mark_price: input.mark_price,
            },
        })])
    }
}

fn deserialize_optional_decimal_string<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    value
        .filter(|value| !value.is_empty())
        .map(|value| value.parse::<Decimal>().map_err(serde::de::Error::custom))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::serde::de::datetime_utc_from_epoch_duration;
    use rust_decimal_macros::dec;
    use std::time::Duration;

    fn fixture() -> &'static str {
        r#"
        [
            {
                "symbol": "BTCUSDT",
                "fundingRate": "0.00010000",
                "fundingTime": 1749340800000,
                "markPrice": "65000.25000000"
            }
        ]
        "#
    }

    #[test]
    fn binance_futures_funding_rate_deserialises() {
        let actual = serde_json::from_str::<Vec<BinanceFuturesFundingRate>>(fixture()).unwrap();
        let row = actual.into_iter().next().unwrap();

        assert_eq!(row.symbol, "BTCUSDT");
        assert_eq!(row.funding_rate, dec!(0.00010000));
        assert_eq!(row.mark_price, Some(dec!(65000.25000000)));
        assert_eq!(
            row.funding_time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1749340800000))
        );
    }

    #[test]
    fn binance_futures_funding_rate_converts_to_market_event() {
        let raw = serde_json::from_str::<Vec<BinanceFuturesFundingRate>>(fixture())
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let funding_time = raw.funding_time;

        let iter = MarketIter::<&'static str, FundingRate>::from((
            ExchangeId::BinanceFuturesUsd,
            "btc-usdt-perp",
            raw,
        ));

        let event = iter.0.into_iter().next().unwrap().unwrap();
        assert_eq!(event.exchange, ExchangeId::BinanceFuturesUsd);
        assert_eq!(event.instrument, "btc-usdt-perp");
        assert_eq!(event.time_exchange, funding_time);
        assert_eq!(event.kind.funding_time, funding_time);
        assert_eq!(event.kind.funding_rate, dec!(0.00010000));
        assert_eq!(event.kind.mark_price, Some(dec!(65000.25000000)));
    }
}
