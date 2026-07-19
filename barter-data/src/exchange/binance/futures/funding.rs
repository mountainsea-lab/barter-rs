use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::binance::{futures::BinanceFuturesUsd, market::BinanceMarket},
    instrument::InstrumentData,
    subscription::{Subscription, funding::FundingRate},
};
use barter_instrument::exchange::ExchangeId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::future::Future;

/// [`crate::exchange::binance::futures::BinanceFuturesUsd`] HTTP funding rate url.
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#get-funding-rate-history>
pub const HTTP_FUNDING_RATE_URL_BINANCE_FUTURES_USD: &str =
    "https://fapi.binance.com/fapi/v1/fundingRate";

/// Query parameters for Binance USD-M Futures funding rate history.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct BinanceFuturesFundingRateRequest {
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub limit: Option<u16>,
}

impl BinanceFuturesFundingRateRequest {
    pub fn latest() -> Self {
        Self {
            start_time: None,
            end_time: None,
            limit: Some(1),
        }
    }
}

pub fn funding_rate_url(symbol: &str, request: BinanceFuturesFundingRateRequest) -> String {
    let mut url = format!(
        "{}?symbol={}",
        HTTP_FUNDING_RATE_URL_BINANCE_FUTURES_USD, symbol
    );

    if let Some(start_time) = request.start_time {
        url.push_str(&format!("&startTime={start_time}"));
    }

    if let Some(end_time) = request.end_time {
        url.push_str(&format!("&endTime={end_time}"));
    }

    if let Some(limit) = request.limit {
        url.push_str(&format!("&limit={limit}"));
    }

    url
}

#[derive(Debug)]
pub struct BinanceFuturesUsdFundingRateFetcher;

impl BinanceFuturesUsdFundingRateFetcher {
    pub fn fetch_latest<Instrument>(
        subscriptions: &[Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::funding::FundingRates,
        >],
    ) -> impl Future<
        Output = Result<
            Vec<MarketEvent<Instrument::Key, FundingRate>>,
            barter_integration::error::SocketError,
        >,
    > + Send
    where
        Instrument: InstrumentData,
        Instrument::Key: Clone,
        Subscription<BinanceFuturesUsd, Instrument, crate::subscription::funding::FundingRates>:
            Identifier<BinanceMarket>,
    {
        Self::fetch(subscriptions, BinanceFuturesFundingRateRequest::latest())
    }

    pub fn fetch<Instrument>(
        subscriptions: &[Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::funding::FundingRates,
        >],
        request: BinanceFuturesFundingRateRequest,
    ) -> impl Future<
        Output = Result<
            Vec<MarketEvent<Instrument::Key, FundingRate>>,
            barter_integration::error::SocketError,
        >,
    > + Send
    where
        Instrument: InstrumentData,
        Instrument::Key: Clone,
        Subscription<BinanceFuturesUsd, Instrument, crate::subscription::funding::FundingRates>:
            Identifier<BinanceMarket>,
    {
        use futures_util::future::try_join_all;

        let funding_futures = subscriptions.iter().map(move |sub| {
            let symbol = sub.id();
            let funding_url = funding_rate_url(symbol.as_ref(), request);

            async move {
                let rows = reqwest::get(funding_url)
                    .await
                    .map_err(barter_integration::error::SocketError::Http)?
                    .error_for_status()
                    .map_err(barter_integration::error::SocketError::Http)?
                    .json::<Vec<BinanceFuturesFundingRate>>()
                    .await
                    .map_err(barter_integration::error::SocketError::Http)?;

                Ok::<_, barter_integration::error::SocketError>(
                    rows.into_iter()
                        .map(|row| {
                            let funding_time = row.funding_time;
                            MarketEvent {
                                time_exchange: funding_time,
                                time_received: Utc::now(),
                                exchange: ExchangeId::BinanceFuturesUsd,
                                instrument: sub.instrument.key().clone(),
                                kind: FundingRate {
                                    funding_time,
                                    funding_rate: row.funding_rate,
                                    mark_price: row.mark_price,
                                },
                            }
                        })
                        .collect::<Vec<_>>(),
                )
            }
        });

        async move {
            let nested = try_join_all(funding_futures).await?;
            Ok(nested.into_iter().flatten().collect())
        }
    }
}

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

    #[test]
    fn funding_rate_url_builds_required_symbol_only_query() {
        let actual = funding_rate_url("BTCUSDT", BinanceFuturesFundingRateRequest::default());

        assert_eq!(
            actual,
            "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT"
        );
    }

    #[test]
    fn funding_rate_url_builds_full_query() {
        let actual = funding_rate_url(
            "ETHUSDT",
            BinanceFuturesFundingRateRequest {
                start_time: Some(1749340800000),
                end_time: Some(1749369600000),
                limit: Some(100),
            },
        );

        assert_eq!(
            actual,
            "https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&startTime=1749340800000&endTime=1749369600000&limit=100"
        );
    }

    #[test]
    fn latest_request_uses_limit_one() {
        let actual = funding_rate_url("BTCUSDT", BinanceFuturesFundingRateRequest::latest());

        assert_eq!(
            actual,
            "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=1"
        );
    }
}
