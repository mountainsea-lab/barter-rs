use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::binance::{futures::BinanceFuturesUsd, market::BinanceMarket},
    instrument::InstrumentData,
    subscription::{Subscription, open_interest::OpenInterest},
};
use barter_instrument::exchange::ExchangeId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::future::Future;

/// [`crate::exchange::binance::futures::BinanceFuturesUsd`] HTTP open interest url.
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#open-interest>
pub const HTTP_OPEN_INTEREST_URL_BINANCE_FUTURES_USD: &str =
    "https://fapi.binance.com/fapi/v1/openInterest";

pub fn open_interest_url(symbol: &str) -> String {
    format!("{HTTP_OPEN_INTEREST_URL_BINANCE_FUTURES_USD}?symbol={symbol}")
}

#[derive(Debug)]
pub struct BinanceFuturesUsdOpenInterestFetcher;

impl BinanceFuturesUsdOpenInterestFetcher {
    pub fn fetch_latest<Instrument>(
        subscriptions: &[Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::open_interest::OpenInterests,
        >],
    ) -> impl Future<
        Output = Result<
            Vec<MarketEvent<Instrument::Key, OpenInterest>>,
            barter_integration::error::SocketError,
        >,
    > + Send
    where
        Instrument: InstrumentData,
        Instrument::Key: Clone,
        Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::open_interest::OpenInterests,
        >: Identifier<BinanceMarket>,
    {
        Self::fetch(subscriptions)
    }

    pub fn fetch<Instrument>(
        subscriptions: &[Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::open_interest::OpenInterests,
        >],
    ) -> impl Future<
        Output = Result<
            Vec<MarketEvent<Instrument::Key, OpenInterest>>,
            barter_integration::error::SocketError,
        >,
    > + Send
    where
        Instrument: InstrumentData,
        Instrument::Key: Clone,
        Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::open_interest::OpenInterests,
        >: Identifier<BinanceMarket>,
    {
        use futures_util::future::try_join_all;

        let open_interest_futures = subscriptions.iter().map(move |sub| {
            let symbol = sub.id();
            let url = open_interest_url(symbol.as_ref());

            async move {
                let row = fetch_open_interest_url(url).await?;
                let event_time = row.time;

                Ok::<_, barter_integration::error::SocketError>(MarketEvent {
                    time_exchange: event_time,
                    time_received: Utc::now(),
                    exchange: ExchangeId::BinanceFuturesUsd,
                    instrument: sub.instrument.key().clone(),
                    kind: OpenInterest::from(row),
                })
            }
        });

        async move { try_join_all(open_interest_futures).await }
    }
}

async fn fetch_open_interest_url(
    url: String,
) -> Result<BinanceFuturesOpenInterestRest, barter_integration::error::SocketError> {
    reqwest::get(url)
        .await
        .map_err(barter_integration::error::SocketError::Http)?
        .error_for_status()
        .map_err(barter_integration::error::SocketError::Http)?
        .json::<BinanceFuturesOpenInterestRest>()
        .await
        .map_err(barter_integration::error::SocketError::Http)
}

/// Binance USD-M Futures latest open interest REST response.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesOpenInterestRest {
    #[serde(alias = "symbol")]
    pub symbol: String,
    #[serde(
        alias = "openInterest",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub open_interest: Decimal,
    #[serde(
        alias = "time",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: DateTime<Utc>,
}

impl From<BinanceFuturesOpenInterestRest> for OpenInterest {
    fn from(value: BinanceFuturesOpenInterestRest) -> Self {
        Self {
            event_time: value.time,
            open_interest: value.open_interest,
        }
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceFuturesOpenInterestRest)>
    for MarketIter<InstrumentKey, OpenInterest>
{
    fn from(
        (exchange, instrument, value): (ExchangeId, InstrumentKey, BinanceFuturesOpenInterestRest),
    ) -> Self {
        let event_time = value.time;
        Self(vec![Ok(MarketEvent {
            time_exchange: event_time,
            time_received: Utc::now(),
            exchange,
            instrument,
            kind: OpenInterest::from(value),
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
        {
            "openInterest": "10659.509",
            "symbol": "BTCUSDT",
            "time": 1589437530011
        }
        "#
    }

    #[test]
    fn open_interest_url_includes_required_symbol() {
        assert_eq!(
            open_interest_url("BTCUSDT"),
            "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"
        );
    }

    #[test]
    fn binance_futures_open_interest_rest_deserialises() {
        let actual =
            serde_json::from_str::<BinanceFuturesOpenInterestRest>(rest_fixture()).unwrap();

        assert_eq!(actual.symbol, "BTCUSDT");
        assert_eq!(actual.open_interest, dec!(10659.509));
        assert_eq!(
            actual.time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1589437530011))
        );
    }

    #[test]
    fn binance_futures_open_interest_rest_converts_to_normalised_open_interest() {
        let input = serde_json::from_str::<BinanceFuturesOpenInterestRest>(rest_fixture()).unwrap();
        let actual = crate::subscription::open_interest::OpenInterest::from(input);

        assert_eq!(actual.open_interest, dec!(10659.509));
        assert_eq!(
            actual.event_time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1589437530011))
        );
    }

    #[test]
    fn binance_futures_open_interest_rest_converts_to_market_iter() {
        let input = serde_json::from_str::<BinanceFuturesOpenInterestRest>(rest_fixture()).unwrap();
        let actual = crate::event::MarketIter::<
            &str,
            crate::subscription::open_interest::OpenInterest,
        >::from((
            barter_instrument::exchange::ExchangeId::BinanceFuturesUsd,
            "BTCUSDT",
            input,
        ));

        assert_eq!(actual.0.len(), 1);
        let event = actual.0.into_iter().next().unwrap().unwrap();
        assert_eq!(
            event.exchange,
            barter_instrument::exchange::ExchangeId::BinanceFuturesUsd
        );
        assert_eq!(event.instrument, "BTCUSDT");
        assert_eq!(event.time_exchange, event.kind.event_time);
        assert_eq!(event.kind.open_interest, dec!(10659.509));
    }

    #[tokio::test]
    async fn open_interest_fetcher_surfaces_http_error_statuses_before_json_decoding() {
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

        let actual = fetch_open_interest_url(url).await;

        server.join().unwrap();
        match actual {
            Err(barter_integration::error::SocketError::Http(error)) => {
                assert_eq!(error.status(), Some(reqwest::StatusCode::IM_A_TEAPOT));
            }
            other => panic!("expected HTTP status error before JSON decoding, got {other:?}"),
        }
    }
}
