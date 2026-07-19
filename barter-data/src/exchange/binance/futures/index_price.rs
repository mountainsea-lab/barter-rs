use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::{
        ExchangeSub,
        binance::{futures::BinanceFuturesUsd, market::BinanceMarket},
    },
    instrument::InstrumentData,
    subscription::{Subscription, index_price::IndexPrice},
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::future::Future;

/// [`crate::exchange::binance::futures::BinanceFuturesUsd`] HTTP index price url.
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#mark-price>
pub const HTTP_INDEX_PRICE_URL_BINANCE_FUTURES_USD: &str =
    "https://fapi.binance.com/fapi/v1/premiumIndex";

pub fn index_price_url(symbol: &str) -> String {
    format!("{HTTP_INDEX_PRICE_URL_BINANCE_FUTURES_USD}?symbol={symbol}")
}

#[derive(Debug)]
pub struct BinanceFuturesUsdIndexPriceFetcher;

impl BinanceFuturesUsdIndexPriceFetcher {
    pub fn fetch_latest<Instrument>(
        subscriptions: &[Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::index_price::IndexPrices,
        >],
    ) -> impl Future<
        Output = Result<
            Vec<MarketEvent<Instrument::Key, IndexPrice>>,
            barter_integration::error::SocketError,
        >,
    > + Send
    where
        Instrument: InstrumentData,
        Instrument::Key: Clone,
        Subscription<BinanceFuturesUsd, Instrument, crate::subscription::index_price::IndexPrices>:
            Identifier<BinanceMarket>,
    {
        Self::fetch(subscriptions)
    }

    pub fn fetch<Instrument>(
        subscriptions: &[Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::index_price::IndexPrices,
        >],
    ) -> impl Future<
        Output = Result<
            Vec<MarketEvent<Instrument::Key, IndexPrice>>,
            barter_integration::error::SocketError,
        >,
    > + Send
    where
        Instrument: InstrumentData,
        Instrument::Key: Clone,
        Subscription<BinanceFuturesUsd, Instrument, crate::subscription::index_price::IndexPrices>:
            Identifier<BinanceMarket>,
    {
        use futures_util::future::try_join_all;

        let index_price_futures = subscriptions.iter().map(move |sub| {
            let symbol = sub.id();
            let url = index_price_url(symbol.as_ref());

            async move {
                let row = reqwest::get(url)
                    .await
                    .map_err(barter_integration::error::SocketError::Http)?
                    .error_for_status()
                    .map_err(barter_integration::error::SocketError::Http)?
                    .json::<BinanceFuturesIndexPriceRest>()
                    .await
                    .map_err(barter_integration::error::SocketError::Http)?;

                let event_time = row.time;
                Ok::<_, barter_integration::error::SocketError>(MarketEvent {
                    time_exchange: event_time,
                    time_received: Utc::now(),
                    exchange: ExchangeId::BinanceFuturesUsd,
                    instrument: sub.instrument.key().clone(),
                    kind: IndexPrice::from(row),
                })
            }
        });

        async move { try_join_all(index_price_futures).await }
    }
}

/// Binance USD-M Futures premium index REST response normalised as index price.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesIndexPriceRest {
    #[serde(alias = "symbol")]
    pub symbol: String,
    #[serde(
        alias = "indexPrice",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub index_price: Decimal,
    #[serde(
        alias = "time",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: DateTime<Utc>,
}

/// Binance USD-M Futures mark price WebSocket payload normalised as index price.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesIndexPriceWs {
    #[serde(alias = "e")]
    pub event_type: String,
    #[serde(
        alias = "E",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub event_time: DateTime<Utc>,
    #[serde(alias = "s", deserialize_with = "de_index_price_subscription_id")]
    pub subscription_id: SubscriptionId,
    #[serde(
        alias = "i",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub index_price: Decimal,
}

impl Identifier<Option<SubscriptionId>> for BinanceFuturesIndexPriceWs {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl From<BinanceFuturesIndexPriceRest> for IndexPrice {
    fn from(value: BinanceFuturesIndexPriceRest) -> Self {
        Self {
            event_time: value.time,
            index_price: value.index_price,
        }
    }
}

impl From<BinanceFuturesIndexPriceWs> for IndexPrice {
    fn from(value: BinanceFuturesIndexPriceWs) -> Self {
        Self {
            event_time: value.event_time,
            index_price: value.index_price,
        }
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceFuturesIndexPriceRest)>
    for MarketIter<InstrumentKey, IndexPrice>
{
    fn from(
        (exchange, instrument, value): (ExchangeId, InstrumentKey, BinanceFuturesIndexPriceRest),
    ) -> Self {
        let event_time = value.time;
        Self(vec![Ok(MarketEvent {
            time_exchange: event_time,
            time_received: Utc::now(),
            exchange,
            instrument,
            kind: IndexPrice::from(value),
        })])
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceFuturesIndexPriceWs)>
    for MarketIter<InstrumentKey, IndexPrice>
{
    fn from(
        (exchange, instrument, value): (ExchangeId, InstrumentKey, BinanceFuturesIndexPriceWs),
    ) -> Self {
        let event_time = value.event_time;
        Self(vec![Ok(MarketEvent {
            time_exchange: event_time,
            time_received: Utc::now(),
            exchange,
            instrument,
            kind: IndexPrice::from(value),
        })])
    }
}

/// Deserialize Binance mark price stream symbols as `@markPrice@1s|SYMBOL` subscription ids.
pub fn de_index_price_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer).map(|market| {
        ExchangeSub::from((
            crate::exchange::binance::BinanceChannel::MARK_PRICE_1S,
            market,
        ))
        .id()
    })
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
            "symbol": "BTCUSDT",
            "markPrice": "11793.63104562",
            "indexPrice": "11791.23456789",
            "estimatedSettlePrice": "11790.11111111",
            "lastFundingRate": "0.00010000",
            "interestRate": "0.00010000",
            "nextFundingTime": 1749369600000,
            "time": 1749340800000
        }
        "#
    }

    fn ws_fixture() -> &'static str {
        r#"
        {
            "e": "markPriceUpdate",
            "E": 1749340800000,
            "s": "BTCUSDT",
            "p": "11793.63104562",
            "i": "11791.23456789",
            "P": "11790.11111111",
            "r": "0.00010000",
            "T": 1749369600000
        }
        "#
    }

    #[test]
    fn index_price_url_includes_required_symbol() {
        assert_eq!(
            index_price_url("BTCUSDT"),
            "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"
        );
    }

    #[test]
    fn binance_futures_index_price_rest_deserialises() {
        let actual = serde_json::from_str::<BinanceFuturesIndexPriceRest>(rest_fixture()).unwrap();

        assert_eq!(actual.symbol, "BTCUSDT");
        assert_eq!(actual.index_price, dec!(11791.23456789));
        assert_eq!(
            actual.time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1749340800000))
        );
    }

    #[test]
    fn binance_futures_index_price_rest_converts_to_market_event() {
        let raw = serde_json::from_str::<BinanceFuturesIndexPriceRest>(rest_fixture()).unwrap();
        let event_time = raw.time;

        let iter = MarketIter::<&'static str, IndexPrice>::from((
            ExchangeId::BinanceFuturesUsd,
            "btc-usdt-perp",
            raw,
        ));

        let event = iter.0.into_iter().next().unwrap().unwrap();
        assert_eq!(event.exchange, ExchangeId::BinanceFuturesUsd);
        assert_eq!(event.instrument, "btc-usdt-perp");
        assert_eq!(event.time_exchange, event_time);
        assert_eq!(event.kind.event_time, event_time);
        assert_eq!(event.kind.index_price, dec!(11791.23456789));
    }

    #[test]
    fn index_price_fetcher_surfaces_http_error_statuses() {
        let source = include_str!("index_price.rs");

        assert!(
            source.matches(".error_for_status()").count() >= 2,
            "index price REST fetcher must call error_for_status() before JSON decoding"
        );
    }

    #[test]
    fn binance_futures_index_price_ws_deserialises_from_mark_price_payload() {
        let actual = serde_json::from_str::<BinanceFuturesIndexPriceWs>(ws_fixture()).unwrap();

        assert_eq!(actual.event_type, "markPriceUpdate");
        assert_eq!(
            actual.subscription_id,
            barter_integration::subscription::SubscriptionId::from("@markPrice@1s|BTCUSDT")
        );
        assert_eq!(actual.index_price, dec!(11791.23456789));
        assert_eq!(
            actual.event_time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1749340800000))
        );
    }

    #[test]
    fn binance_futures_index_price_ws_converts_to_market_event() {
        let raw = serde_json::from_str::<BinanceFuturesIndexPriceWs>(ws_fixture()).unwrap();
        let event_time = raw.event_time;

        let iter = MarketIter::<&'static str, IndexPrice>::from((
            ExchangeId::BinanceFuturesUsd,
            "btc-usdt-perp",
            raw,
        ));

        let event = iter.0.into_iter().next().unwrap().unwrap();
        assert_eq!(event.exchange, ExchangeId::BinanceFuturesUsd);
        assert_eq!(event.instrument, "btc-usdt-perp");
        assert_eq!(event.time_exchange, event_time);
        assert_eq!(event.kind.event_time, event_time);
        assert_eq!(event.kind.index_price, dec!(11791.23456789));
    }
}
