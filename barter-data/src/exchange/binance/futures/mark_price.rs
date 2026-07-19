use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::{
        ExchangeSub,
        binance::{futures::BinanceFuturesUsd, market::BinanceMarket},
    },
    instrument::InstrumentData,
    subscription::{Subscription, mark_price::MarkPrice},
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::future::Future;

/// [`crate::exchange::binance::futures::BinanceFuturesUsd`] HTTP mark price url.
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#mark-price>
pub const HTTP_MARK_PRICE_URL_BINANCE_FUTURES_USD: &str =
    "https://fapi.binance.com/fapi/v1/premiumIndex";

pub fn mark_price_url(symbol: &str) -> String {
    format!("{HTTP_MARK_PRICE_URL_BINANCE_FUTURES_USD}?symbol={symbol}")
}

#[derive(Debug)]
pub struct BinanceFuturesUsdMarkPriceFetcher;

impl BinanceFuturesUsdMarkPriceFetcher {
    pub fn fetch_latest<Instrument>(
        subscriptions: &[Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::mark_price::MarkPrices,
        >],
    ) -> impl Future<
        Output = Result<
            Vec<MarketEvent<Instrument::Key, MarkPrice>>,
            barter_integration::error::SocketError,
        >,
    > + Send
    where
        Instrument: InstrumentData,
        Instrument::Key: Clone,
        Subscription<BinanceFuturesUsd, Instrument, crate::subscription::mark_price::MarkPrices>:
            Identifier<BinanceMarket>,
    {
        Self::fetch(subscriptions)
    }

    pub fn fetch<Instrument>(
        subscriptions: &[Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::mark_price::MarkPrices,
        >],
    ) -> impl Future<
        Output = Result<
            Vec<MarketEvent<Instrument::Key, MarkPrice>>,
            barter_integration::error::SocketError,
        >,
    > + Send
    where
        Instrument: InstrumentData,
        Instrument::Key: Clone,
        Subscription<BinanceFuturesUsd, Instrument, crate::subscription::mark_price::MarkPrices>:
            Identifier<BinanceMarket>,
    {
        use futures_util::future::try_join_all;

        let mark_price_futures = subscriptions.iter().map(move |sub| {
            let symbol = sub.id();
            let url = mark_price_url(symbol.as_ref());

            async move {
                let row = reqwest::get(url)
                    .await
                    .map_err(barter_integration::error::SocketError::Http)?
                    .error_for_status()
                    .map_err(barter_integration::error::SocketError::Http)?
                    .json::<BinanceFuturesMarkPriceRest>()
                    .await
                    .map_err(barter_integration::error::SocketError::Http)?;

                let event_time = row.time;
                Ok::<_, barter_integration::error::SocketError>(MarketEvent {
                    time_exchange: event_time,
                    time_received: Utc::now(),
                    exchange: ExchangeId::BinanceFuturesUsd,
                    instrument: sub.instrument.key().clone(),
                    kind: MarkPrice::from(row),
                })
            }
        });

        async move { try_join_all(mark_price_futures).await }
    }
}

/// Binance USD-M Futures mark price REST response.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesMarkPriceRest {
    #[serde(alias = "symbol")]
    pub symbol: String,
    #[serde(
        alias = "markPrice",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub mark_price: Decimal,
    #[serde(
        alias = "indexPrice",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub index_price: Decimal,
    #[serde(
        alias = "estimatedSettlePrice",
        default,
        deserialize_with = "deserialize_optional_decimal_string"
    )]
    pub estimated_settle_price: Option<Decimal>,
    #[serde(
        alias = "lastFundingRate",
        default,
        deserialize_with = "deserialize_optional_decimal_string"
    )]
    pub last_funding_rate: Option<Decimal>,
    #[serde(
        alias = "interestRate",
        default,
        deserialize_with = "deserialize_optional_decimal_string"
    )]
    pub interest_rate: Option<Decimal>,
    #[serde(
        alias = "nextFundingTime",
        default,
        deserialize_with = "deserialize_optional_epoch_ms"
    )]
    pub next_funding_time: Option<DateTime<Utc>>,
    #[serde(
        alias = "time",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: DateTime<Utc>,
}

/// Binance USD-M Futures mark price WebSocket message.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesMarkPriceWs {
    #[serde(alias = "e")]
    pub event_type: String,
    #[serde(
        alias = "E",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub event_time: DateTime<Utc>,
    #[serde(alias = "s", deserialize_with = "de_mark_price_subscription_id")]
    pub subscription_id: SubscriptionId,
    #[serde(
        alias = "p",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub mark_price: Decimal,
    #[serde(
        alias = "i",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub index_price: Decimal,
    #[serde(
        alias = "P",
        default,
        deserialize_with = "deserialize_optional_decimal_string"
    )]
    pub estimated_settle_price: Option<Decimal>,
    #[serde(
        alias = "r",
        default,
        deserialize_with = "deserialize_optional_decimal_string"
    )]
    pub last_funding_rate: Option<Decimal>,
    #[serde(
        alias = "T",
        default,
        deserialize_with = "deserialize_optional_epoch_ms"
    )]
    pub next_funding_time: Option<DateTime<Utc>>,
}

impl Identifier<Option<SubscriptionId>> for BinanceFuturesMarkPriceWs {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl From<BinanceFuturesMarkPriceRest> for MarkPrice {
    fn from(input: BinanceFuturesMarkPriceRest) -> Self {
        Self {
            event_time: input.time,
            mark_price: input.mark_price,
            index_price: input.index_price,
            estimated_settle_price: input.estimated_settle_price,
            last_funding_rate: input.last_funding_rate,
            interest_rate: input.interest_rate,
            next_funding_time: input.next_funding_time,
        }
    }
}

impl From<BinanceFuturesMarkPriceWs> for MarkPrice {
    fn from(input: BinanceFuturesMarkPriceWs) -> Self {
        Self {
            event_time: input.event_time,
            mark_price: input.mark_price,
            index_price: input.index_price,
            estimated_settle_price: input.estimated_settle_price,
            last_funding_rate: input.last_funding_rate,
            interest_rate: None,
            next_funding_time: input.next_funding_time,
        }
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceFuturesMarkPriceRest)>
    for MarketIter<InstrumentKey, MarkPrice>
{
    fn from(
        (exchange_id, instrument, input): (ExchangeId, InstrumentKey, BinanceFuturesMarkPriceRest),
    ) -> Self {
        let time_exchange = input.time;
        Self(vec![Ok(MarketEvent {
            time_exchange,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: MarkPrice::from(input),
        })])
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceFuturesMarkPriceWs)>
    for MarketIter<InstrumentKey, MarkPrice>
{
    fn from(
        (exchange_id, instrument, input): (ExchangeId, InstrumentKey, BinanceFuturesMarkPriceWs),
    ) -> Self {
        let time_exchange = input.event_time;
        Self(vec![Ok(MarketEvent {
            time_exchange,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: MarkPrice::from(input),
        })])
    }
}

/// Deserialize Binance mark price symbol as the associated `@markPrice@1s|SYMBOL` subscription id.
pub fn de_mark_price_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
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

fn deserialize_optional_epoch_ms<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let value = Option::<u64>::deserialize(deserializer)?;
    Ok(value.map(|value| {
        barter_integration::serde::de::datetime_utc_from_epoch_duration(
            std::time::Duration::from_millis(value),
        )
    }))
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
    fn mark_price_url_includes_required_symbol() {
        assert_eq!(
            mark_price_url("BTCUSDT"),
            "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"
        );
    }

    #[test]
    fn mark_price_fetcher_surfaces_http_error_statuses() {
        let source = include_str!("mark_price.rs");

        assert!(
            source.matches(".error_for_status()").count() >= 2,
            "mark price REST fetcher must call error_for_status() before JSON decoding"
        );
    }

    #[test]
    fn binance_futures_mark_price_rest_deserialises() {
        let actual = serde_json::from_str::<BinanceFuturesMarkPriceRest>(rest_fixture()).unwrap();

        assert_eq!(actual.symbol, "BTCUSDT");
        assert_eq!(actual.mark_price, dec!(11793.63104562));
        assert_eq!(actual.index_price, dec!(11791.23456789));
        assert_eq!(actual.estimated_settle_price, Some(dec!(11790.11111111)));
        assert_eq!(actual.last_funding_rate, Some(dec!(0.00010000)));
        assert_eq!(actual.interest_rate, Some(dec!(0.00010000)));
        assert_eq!(
            actual.next_funding_time,
            Some(datetime_utc_from_epoch_duration(Duration::from_millis(
                1749369600000
            )))
        );
        assert_eq!(
            actual.time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1749340800000))
        );
    }

    #[test]
    fn binance_futures_mark_price_rest_converts_to_market_event() {
        let raw = serde_json::from_str::<BinanceFuturesMarkPriceRest>(rest_fixture()).unwrap();
        let event_time = raw.time;

        let iter = MarketIter::<&'static str, MarkPrice>::from((
            ExchangeId::BinanceFuturesUsd,
            "btc-usdt-perp",
            raw,
        ));

        let event = iter.0.into_iter().next().unwrap().unwrap();
        assert_eq!(event.exchange, ExchangeId::BinanceFuturesUsd);
        assert_eq!(event.instrument, "btc-usdt-perp");
        assert_eq!(event.time_exchange, event_time);
        assert_eq!(event.kind.event_time, event_time);
        assert_eq!(event.kind.mark_price, dec!(11793.63104562));
        assert_eq!(event.kind.index_price, dec!(11791.23456789));
        assert_eq!(event.kind.interest_rate, Some(dec!(0.00010000)));
    }

    #[test]
    fn binance_futures_mark_price_ws_deserialises() {
        let actual = serde_json::from_str::<BinanceFuturesMarkPriceWs>(ws_fixture()).unwrap();

        assert_eq!(actual.event_type, "markPriceUpdate");
        assert_eq!(
            actual.subscription_id,
            SubscriptionId::from("@markPrice@1s|BTCUSDT")
        );
        assert_eq!(actual.mark_price, dec!(11793.63104562));
        assert_eq!(actual.index_price, dec!(11791.23456789));
        assert_eq!(actual.estimated_settle_price, Some(dec!(11790.11111111)));
        assert_eq!(actual.last_funding_rate, Some(dec!(0.00010000)));
        assert_eq!(
            actual.event_time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1749340800000))
        );
    }

    #[test]
    fn binance_futures_mark_price_ws_converts_to_market_event() {
        let raw = serde_json::from_str::<BinanceFuturesMarkPriceWs>(ws_fixture()).unwrap();
        let event_time = raw.event_time;

        let iter = MarketIter::<&'static str, MarkPrice>::from((
            ExchangeId::BinanceFuturesUsd,
            "btc-usdt-perp",
            raw,
        ));

        let event = iter.0.into_iter().next().unwrap().unwrap();
        assert_eq!(event.exchange, ExchangeId::BinanceFuturesUsd);
        assert_eq!(event.instrument, "btc-usdt-perp");
        assert_eq!(event.time_exchange, event_time);
        assert_eq!(event.kind.event_time, event_time);
        assert_eq!(event.kind.mark_price, dec!(11793.63104562));
        assert_eq!(event.kind.index_price, dec!(11791.23456789));
        assert_eq!(event.kind.interest_rate, None);
    }
}
