use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::binance::{futures::BinanceFuturesUsd, market::BinanceMarket},
    instrument::InstrumentData,
    subscription::{Subscription, taker_flow::TakerFlow},
};
use barter_instrument::exchange::ExchangeId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::future::Future;

/// [`crate::exchange::binance::futures::BinanceFuturesUsd`] HTTP taker long/short ratio url.
pub const HTTP_TAKER_LONG_SHORT_RATIO_URL_BINANCE_FUTURES_USD: &str =
    "https://fapi.binance.com/futures/data/takerlongshortRatio";

pub fn taker_flow_url(symbol: &str, period: &str, limit: Option<u16>) -> String {
    let mut url = reqwest::Url::parse(HTTP_TAKER_LONG_SHORT_RATIO_URL_BINANCE_FUTURES_USD)
        .expect("Binance Futures taker flow URL constant must be valid");
    {
        let mut query = url.query_pairs_mut();
        query
            .append_pair("symbol", symbol)
            .append_pair("period", period);
        if let Some(limit) = limit {
            query.append_pair("limit", &limit.to_string());
        }
    }
    url.to_string()
}

#[derive(Debug)]
pub struct BinanceFuturesUsdTakerFlowFetcher;

impl BinanceFuturesUsdTakerFlowFetcher {
    pub fn fetch_recent<Instrument>(
        subscriptions: &[Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::taker_flow::TakerFlows,
        >],
        period: &str,
        limit: Option<u16>,
    ) -> impl Future<
        Output = Result<
            Vec<MarketEvent<Instrument::Key, TakerFlow>>,
            barter_integration::error::SocketError,
        >,
    > + Send
    where
        Instrument: InstrumentData,
        Instrument::Key: Clone,
        Subscription<BinanceFuturesUsd, Instrument, crate::subscription::taker_flow::TakerFlows>:
            Identifier<BinanceMarket>,
    {
        Self::fetch(subscriptions, period, limit)
    }

    pub fn fetch<Instrument>(
        subscriptions: &[Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::taker_flow::TakerFlows,
        >],
        period: &str,
        limit: Option<u16>,
    ) -> impl Future<
        Output = Result<
            Vec<MarketEvent<Instrument::Key, TakerFlow>>,
            barter_integration::error::SocketError,
        >,
    > + Send
    where
        Instrument: InstrumentData,
        Instrument::Key: Clone,
        Subscription<BinanceFuturesUsd, Instrument, crate::subscription::taker_flow::TakerFlows>:
            Identifier<BinanceMarket>,
    {
        use futures_util::future::try_join_all;

        let period = period.to_owned();
        let taker_flow_futures = subscriptions.iter().map(move |sub| {
            let symbol = sub.id();
            let url = taker_flow_url(symbol.as_ref(), &period, limit);

            async move {
                let rows = fetch_taker_flow_url(url).await?;
                let instrument = sub.instrument.key().clone();

                Ok::<_, barter_integration::error::SocketError>(
                    rows.into_iter()
                        .map(|row| {
                            let period_start = row.timestamp;
                            MarketEvent {
                                time_exchange: period_start,
                                time_received: Utc::now(),
                                exchange: ExchangeId::BinanceFuturesUsd,
                                instrument: instrument.clone(),
                                kind: TakerFlow::from(row),
                            }
                        })
                        .collect::<Vec<_>>(),
                )
            }
        });

        async move {
            let nested = try_join_all(taker_flow_futures).await?;
            Ok(nested.into_iter().flatten().collect())
        }
    }
}

async fn fetch_taker_flow_url(
    url: String,
) -> Result<Vec<BinanceFuturesTakerFlowRest>, barter_integration::error::SocketError> {
    reqwest::get(url)
        .await
        .map_err(barter_integration::error::SocketError::Http)?
        .error_for_status()
        .map_err(barter_integration::error::SocketError::Http)?
        .json::<Vec<BinanceFuturesTakerFlowRest>>()
        .await
        .map_err(barter_integration::error::SocketError::Http)
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesTakerFlowRest {
    #[serde(
        alias = "buySellRatio",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub buy_sell_ratio: Decimal,
    #[serde(
        alias = "buyVol",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub buy_volume: Decimal,
    #[serde(
        alias = "sellVol",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub sell_volume: Decimal,
    #[serde(
        alias = "timestamp",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub timestamp: DateTime<Utc>,
}

impl From<BinanceFuturesTakerFlowRest> for TakerFlow {
    fn from(value: BinanceFuturesTakerFlowRest) -> Self {
        Self {
            period_start: value.timestamp,
            buy_volume: value.buy_volume,
            sell_volume: value.sell_volume,
            buy_sell_ratio: value.buy_sell_ratio,
        }
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceFuturesTakerFlowRest)>
    for MarketIter<InstrumentKey, TakerFlow>
{
    fn from(
        (exchange, instrument, value): (ExchangeId, InstrumentKey, BinanceFuturesTakerFlowRest),
    ) -> Self {
        let period_start = value.timestamp;
        Self(vec![Ok(MarketEvent {
            time_exchange: period_start,
            time_received: Utc::now(),
            exchange,
            instrument,
            kind: TakerFlow::from(value),
        })])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::serde::de::datetime_utc_from_epoch_duration;
    use rust_decimal_macros::dec;
    use std::time::Duration;

    fn rest_fixture() -> &'static str {
        r#"
        [
          {
            "buySellRatio": "1.4342",
            "buyVol": "387.3300",
            "sellVol": "270.0700",
            "timestamp": 1585614900000
          }
        ]
        "#
    }

    #[test]
    fn taker_flow_url_includes_required_parameters_and_limit() {
        assert_eq!(
            taker_flow_url("BTCUSDT", "5m", Some(30)),
            "https://fapi.binance.com/futures/data/takerlongshortRatio?symbol=BTCUSDT&period=5m&limit=30"
        );
        assert_eq!(
            taker_flow_url("BTCUSDT", "1h", None),
            "https://fapi.binance.com/futures/data/takerlongshortRatio?symbol=BTCUSDT&period=1h"
        );
    }

    #[test]
    fn taker_flow_url_percent_encodes_query_parameters() {
        assert_eq!(
            taker_flow_url("BTC&evil=1 USDT", "5m&limit=999", Some(30)),
            "https://fapi.binance.com/futures/data/takerlongshortRatio?symbol=BTC%26evil%3D1+USDT&period=5m%26limit%3D999&limit=30"
        );
    }

    #[test]
    fn binance_futures_taker_flow_rest_deserialises() {
        let rows =
            serde_json::from_str::<Vec<BinanceFuturesTakerFlowRest>>(rest_fixture()).unwrap();
        let actual = rows.into_iter().next().unwrap();

        assert_eq!(actual.buy_sell_ratio, dec!(1.4342));
        assert_eq!(actual.buy_volume, dec!(387.3300));
        assert_eq!(actual.sell_volume, dec!(270.0700));
        assert_eq!(
            actual.timestamp,
            datetime_utc_from_epoch_duration(Duration::from_millis(1585614900000))
        );
    }

    #[test]
    fn binance_futures_taker_flow_rest_converts_to_normalised_taker_flow() {
        let input = serde_json::from_str::<Vec<BinanceFuturesTakerFlowRest>>(rest_fixture())
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let actual = crate::subscription::taker_flow::TakerFlow::from(input);

        assert_eq!(actual.buy_sell_ratio, dec!(1.4342));
        assert_eq!(actual.buy_volume, dec!(387.3300));
        assert_eq!(actual.sell_volume, dec!(270.0700));
        assert_eq!(
            actual.period_start,
            datetime_utc_from_epoch_duration(Duration::from_millis(1585614900000))
        );
    }

    #[test]
    fn binance_futures_taker_flow_rest_converts_to_market_iter() {
        let input = serde_json::from_str::<Vec<BinanceFuturesTakerFlowRest>>(rest_fixture())
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let actual =
            crate::event::MarketIter::<&str, crate::subscription::taker_flow::TakerFlow>::from((
                barter_instrument::exchange::ExchangeId::BinanceFuturesUsd,
                "BTCUSDT",
                input,
            ));

        assert_eq!(actual.0.len(), 1);
        let event = actual.0.into_iter().next().unwrap().unwrap();
        assert_eq!(event.instrument, "BTCUSDT");
        assert_eq!(event.time_exchange, event.kind.period_start);
        assert_eq!(event.kind.buy_volume, dec!(387.3300));
        assert_eq!(event.kind.sell_volume, dec!(270.0700));
        assert_eq!(event.kind.buy_sell_ratio, dec!(1.4342));
    }

    #[tokio::test]
    async fn taker_flow_fetcher_surfaces_http_error_statuses_before_json_decoding() {
        use std::{
            io::{Read, Write},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());

        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 418 I'm a teapot\r\ncontent-type: application/json\r\ncontent-length: 8\r\n\r\nnot-json",
                )
                .unwrap();
        });

        let actual = fetch_taker_flow_url(url).await;

        server.join().unwrap();
        match actual {
            Err(barter_integration::error::SocketError::Http(error)) => {
                assert_eq!(error.status(), Some(reqwest::StatusCode::IM_A_TEAPOT));
            }
            other => panic!("expected HTTP status error before JSON decoding, got {other:?}"),
        }
    }
}
