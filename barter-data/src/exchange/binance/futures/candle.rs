use super::super::BinanceChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::candle::Candle,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Binance USD-M Futures kline WebSocket message.
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-streams>
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesKline {
    #[serde(alias = "E")]
    pub event_time: u64,
    #[serde(alias = "s")]
    pub symbol: String,
    #[serde(alias = "k")]
    pub kline: BinanceFuturesKlineData,
}

/// Binance USD-M Futures kline payload nested under the `k` field.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesKlineData {
    #[serde(alias = "s", deserialize_with = "de_candle_subscription_id")]
    pub subscription_id: SubscriptionId,
    #[serde(alias = "i")]
    pub interval: String,
    #[serde(
        alias = "T",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub close_time: DateTime<Utc>,
    #[serde(
        alias = "o",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub open: f64,
    #[serde(
        alias = "h",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub high: f64,
    #[serde(
        alias = "l",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub low: f64,
    #[serde(
        alias = "c",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub close: f64,
    #[serde(
        alias = "v",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub volume: f64,
    #[serde(alias = "n")]
    pub trade_count: u64,
}

impl Identifier<Option<SubscriptionId>> for BinanceFuturesKline {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.kline.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceFuturesKline)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, input): (ExchangeId, InstrumentKey, BinanceFuturesKline),
    ) -> Self {
        Self(vec![Ok(MarketEvent {
            time_exchange: input.kline.close_time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: Candle {
                close_time: input.kline.close_time,
                open: input.kline.open,
                high: input.kline.high,
                low: input.kline.low,
                close: input.kline.close,
                volume: input.kline.volume,
                trade_count: input.kline.trade_count,
            },
        })])
    }
}

/// Deserialize Binance kline symbol as the associated `@kline_1m|SYMBOL` subscription id.
pub fn de_candle_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|market| ExchangeSub::from((BinanceChannel::CANDLES_1M, market)).id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::serde::de::datetime_utc_from_epoch_duration;
    use std::time::Duration;

    fn fixture() -> &'static str {
        r#"
        {
            "e": "kline",
            "E": 1749354825200,
            "s": "BTCUSDT",
            "k": {
                "t": 1749354780000,
                "T": 1749354839999,
                "s": "BTCUSDT",
                "i": "1m",
                "f": 100,
                "L": 200,
                "o": "10000.10",
                "c": "10010.20",
                "h": "10020.30",
                "l": "9990.40",
                "v": "12.345",
                "n": 42,
                "x": false,
                "q": "123456.78",
                "V": "6.789",
                "Q": "67890.12",
                "B": "0"
            }
        }
        "#
    }

    #[test]
    fn test_binance_futures_kline_deserialises() {
        let actual = serde_json::from_str::<BinanceFuturesKline>(fixture()).unwrap();

        assert_eq!(actual.symbol, "BTCUSDT");
        assert_eq!(
            actual.kline.subscription_id,
            SubscriptionId::from("@kline_1m|BTCUSDT")
        );
        assert_eq!(actual.kline.interval, "1m");
        assert_eq!(
            actual.kline.close_time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1749354839999))
        );
        assert_eq!(actual.kline.open, 10000.10);
        assert_eq!(actual.kline.high, 10020.30);
        assert_eq!(actual.kline.low, 9990.40);
        assert_eq!(actual.kline.close, 10010.20);
        assert_eq!(actual.kline.volume, 12.345);
        assert_eq!(actual.kline.trade_count, 42);
    }

    #[test]
    fn test_binance_futures_kline_converts_to_candle_event() {
        let raw = serde_json::from_str::<BinanceFuturesKline>(fixture()).unwrap();
        let close_time = raw.kline.close_time;

        let iter = MarketIter::<&'static str, Candle>::from((
            ExchangeId::BinanceFuturesUsd,
            "btc-usdt-perp",
            raw,
        ));

        let event = iter.0.into_iter().next().unwrap().unwrap();
        assert_eq!(event.exchange, ExchangeId::BinanceFuturesUsd);
        assert_eq!(event.instrument, "btc-usdt-perp");
        assert_eq!(event.time_exchange, close_time);
        assert_eq!(event.kind.close_time, close_time);
        assert_eq!(event.kind.open, 10000.10);
        assert_eq!(event.kind.high, 10020.30);
        assert_eq!(event.kind.low, 9990.40);
        assert_eq!(event.kind.close, 10010.20);
        assert_eq!(event.kind.volume, 12.345);
        assert_eq!(event.kind.trade_count, 42);
    }
}
