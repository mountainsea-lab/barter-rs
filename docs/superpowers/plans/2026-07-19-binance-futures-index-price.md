# Binance USD-M Futures Index Price Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add first-class Binance USD-M Futures index price support to `barter-data` using Binance's real premium index REST payload and mark price WebSocket payload index fields.

**Architecture:** Add a normalized `subscription::index_price` module and `DataKind::IndexPrice`, then wire Binance REST and WebSocket implementations through a provider-specific `exchange::binance::futures::index_price` module. The Binance WebSocket transport intentionally uses `{symbol}@markPrice@1s` as the provider stream but emits only normalized `IndexPrice` events for `IndexPrices` subscriptions.

**Tech Stack:** Rust, `barter-data`, `barter-instrument`, `barter-integration`, `chrono::DateTime<Utc>`, `rust_decimal::Decimal`, serde, tokio/futures stream builder tests.

---

## File Structure

- Create `barter-data/src/subscription/index_price.rs`: normalized `IndexPrices` marker and `IndexPrice` event model.
- Modify `barter-data/src/subscription/mod.rs`: export `index_price`, add `SubKind::IndexPrices`, support matrix entries, serde coverage.
- Modify `barter-data/src/lib.rs`: add public exports for `IndexPrice` and `IndexPrices` using the same pattern as `MarkPrice` and `MarkPrices`.
- Modify `barter-data/src/event.rs`: import `IndexPrice`, add `DataKind::IndexPrice`, add `as_index_price`, `kind_name`, typed-to-generic conversions and tests.
- Create `barter-data/src/exchange/binance/futures/index_price.rs`: Binance REST fetcher, raw REST/WS structs, URL helper, conversions to normalized events, fixture tests.
- Modify `barter-data/src/exchange/binance/futures/mod.rs`: export `index_price`, add `StreamSelector<Instrument, IndexPrices>` using Binance mark price stream raw payload.
- Modify Binance subscription id implementations under `barter-data/src/exchange/binance/mod.rs` if the typed `IndexPrices` subscription needs the same `@markPrice@1s` provider channel mapping as `MarkPrices`.
- Modify `barter-data/src/streams/builder/dynamic/mod.rs`: add `index_prices` channel storage, initialization arm, selectors, and tests.
- Add examples under `barter-data/examples/`: one REST snapshot example and one WebSocket or dynamic stream example, following existing mark price example style.

## Task 1: Normalized Subscription Type and Support Matrix

**Files:**
- Create: `barter-data/src/subscription/index_price.rs`
- Modify: `barter-data/src/subscription/mod.rs`
- Test: existing unit tests in those files

- [ ] **Step 1: Write the failing normalized model test**

Add this new file first:

```rust
use super::SubscriptionKind;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub struct IndexPrices;

impl SubscriptionKind for IndexPrices {
    type Event = IndexPrice;

    fn as_str(&self) -> &'static str {
        "index_prices"
    }
}

impl std::fmt::Display for IndexPrices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct IndexPrice {
    pub event_time: DateTime<Utc>,
    pub index_price: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn index_prices_kind_formats_as_expected() {
        assert_eq!(IndexPrices.as_str(), "index_prices");
        assert_eq!(IndexPrices.to_string(), "index_prices");
    }

    #[test]
    fn index_price_model_uses_decimal_fields() {
        let time = Utc::now();
        let actual = IndexPrice {
            event_time: time,
            index_price: dec!(11791.23456789),
        };

        assert_eq!(actual.event_time, time);
        assert_eq!(actual.index_price, dec!(11791.23456789));
    }
}
```

Add failing tests in `barter-data/src/subscription/mod.rs` test module:

```rust
#[test]
fn sub_kind_index_prices_serde_uses_snake_case() {
    let encoded = serde_json::to_string(&SubKind::IndexPrices).unwrap();
    assert_eq!(encoded, "\"index_prices\"");

    let decoded = serde_json::from_str::<SubKind>("\"index_prices\"").unwrap();
    assert_eq!(decoded, SubKind::IndexPrices);
}

#[test]
fn binance_futures_usd_supports_perpetual_index_prices_only() {
    use barter_instrument::instrument::market_data::MarketDataInstrumentKind;

    assert!(exchange_supports_subscription_kind(
        ExchangeId::BinanceFuturesUsd,
        &MarketDataInstrumentKind::Perpetual,
        &SubKind::IndexPrices,
    ));
    assert!(!exchange_supports_subscription_kind(
        ExchangeId::BinanceFuturesUsd,
        &MarketDataInstrumentKind::Spot,
        &SubKind::IndexPrices,
    ));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task1 \
  rtk cargo test -p barter-data index_price -- --nocapture
```

Expected: compile failure or failing tests because `index_price` is not exported and `SubKind::IndexPrices` does not exist.

- [ ] **Step 3: Implement minimal subscription wiring**

Modify `barter-data/src/subscription/mod.rs`:

```rust
pub mod index_price;
```

Add the enum variant:

```rust
pub enum SubKind {
    PublicTrades,
    OrderBooksL1,
    OrderBooksL2,
    OrderBooksL3,
    Liquidations,
    Candles,
    MarkPrices,
    FundingRates,
    IndexPrices,
}
```

Update the support matrix function that handles `exchange_supports_subscription_kind` so Binance USD-M Futures perpetual instruments include:

```rust
SubKind::IndexPrices => true,
```

and spot or non-Binance branches remain unsupported unless they already explicitly support that kind.

- [ ] **Step 4: Verify Task 1 passes**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task1 \
  rtk cargo test -p barter-data index_price -- --nocapture
rtk git diff --check
```

Expected: all `index_price` focused tests pass and diff check exits 0.

- [ ] **Step 5: Commit Task 1**

```bash
rtk git add barter-data/src/subscription/index_price.rs barter-data/src/subscription/mod.rs barter-data/src/lib.rs
rtk git commit -m "feat(data): add index price subscription kind"
```

## Task 2: DataKind Event Integration

**Files:**
- Modify: `barter-data/src/event.rs`

- [ ] **Step 1: Write failing event integration tests**

Add tests to the `#[cfg(test)]` module in `barter-data/src/event.rs`:

```rust
#[test]
fn data_kind_index_price_has_expected_name_and_accessor() {
    use crate::subscription::index_price::IndexPrice;
    use rust_decimal_macros::dec;

    let time = Utc::now();
    let event = MarketEvent {
        time_exchange: time,
        time_received: time,
        exchange: ExchangeId::BinanceFuturesUsd,
        instrument: "btc-usdt-perp",
        kind: DataKind::IndexPrice(IndexPrice {
            event_time: time,
            index_price: dec!(11791.23456789),
        }),
    };

    assert_eq!(event.kind.kind_name(), "index_price");
    let typed = event.as_index_price().unwrap();
    assert_eq!(*typed.instrument, "btc-usdt-perp");
    assert_eq!(typed.kind.index_price, dec!(11791.23456789));
}

#[test]
fn market_event_index_price_converts_to_data_kind() {
    use crate::subscription::index_price::IndexPrice;
    use rust_decimal_macros::dec;

    let time = Utc::now();
    let typed = MarketEvent {
        time_exchange: time,
        time_received: time,
        exchange: ExchangeId::BinanceFuturesUsd,
        instrument: "btc-usdt-perp",
        kind: IndexPrice {
            event_time: time,
            index_price: dec!(11791.23456789),
        },
    };

    let generic = MarketEvent::<_, DataKind>::from(typed);
    assert_eq!(generic.kind.kind_name(), "index_price");
    assert!(matches!(generic.kind, DataKind::IndexPrice(_)));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task2 \
  rtk cargo test -p barter-data data_kind_index_price market_event_index_price -- --nocapture
```

If Cargo rejects multiple filters, run the two filters separately:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task2 \
  rtk cargo test -p barter-data data_kind_index_price -- --nocapture
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task2 \
  rtk cargo test -p barter-data market_event_index_price -- --nocapture
```

Expected: compile failure because `DataKind::IndexPrice` and `as_index_price` do not exist.

- [ ] **Step 3: Implement minimal DataKind integration**

Update imports in `barter-data/src/event.rs`:

```rust
index_price::IndexPrice,
```

Add accessor:

```rust
pub fn as_index_price(&self) -> Option<MarketEvent<&InstrumentKey, &IndexPrice>> {
    match &self.kind {
        DataKind::IndexPrice(index_price) => Some(self.as_event(index_price)),
        _ => None,
    }
}
```

Add variant:

```rust
IndexPrice(IndexPrice),
```

Add kind name:

```rust
DataKind::IndexPrice(_) => "index_price",
```

Add conversions:

```rust
impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, IndexPrice>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, IndexPrice>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, IndexPrice>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, IndexPrice>) -> Self {
        value.map_kind(IndexPrice::into)
    }
}
```

- [ ] **Step 4: Verify Task 2 passes**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task2 \
  rtk cargo test -p barter-data index_price -- --nocapture
rtk git diff --check
```

Expected: Task 1 and Task 2 index price tests pass.

- [ ] **Step 5: Commit Task 2**

```bash
rtk git add barter-data/src/event.rs
rtk git commit -m "feat(data): add index price data kind"
```

## Task 3: Binance REST Index Price Fetcher

**Files:**
- Create: `barter-data/src/exchange/binance/futures/index_price.rs`
- Modify: `barter-data/src/exchange/binance/futures/mod.rs`

- [ ] **Step 1: Write failing REST tests and skeleton module**

Create `barter-data/src/exchange/binance/futures/index_price.rs` with tests and minimal type names used by tests:

```rust
use crate::{
    event::{MarketEvent, MarketIter},
    exchange::Connector,
    subscription::index_price::IndexPrice,
};
use barter_instrument::exchange::ExchangeId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub fn index_price_url(symbol: &str) -> String {
    format!("https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}")
}

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
```

Add the full test module:

```rust
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
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task3 \
  rtk cargo test -p barter-data binance_futures_index_price_rest -- --nocapture
```

Expected: conversion test fails to compile because `MarketIter` conversion and fetcher do not exist.

- [ ] **Step 3: Implement minimal REST conversion and fetcher**

Add a fetcher and conversion, following funding and mark price patterns:

```rust
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct BinanceFuturesUsdIndexPriceFetcher;

impl BinanceFuturesUsdIndexPriceFetcher {
    pub async fn fetch<InstrumentKey>(
        subscriptions: &[crate::subscription::Subscription<
            crate::exchange::binance::futures::BinanceFuturesUsd,
            crate::instrument::MarketInstrumentData<InstrumentKey>,
            crate::subscription::index_price::IndexPrices,
        >],
    ) -> Result<Vec<MarketEvent<InstrumentKey, IndexPrice>>, barter_integration::error::SocketError>
    where
        InstrumentKey: Clone,
    {
        use futures_util::future::try_join_all;

        let futures = subscriptions.iter().map(move |sub| {
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

        try_join_all(futures).await
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
```

If exact `MarketInstrumentData` or fetcher trait bounds differ, copy the shape from `barter-data/src/exchange/binance/futures/mark_price.rs` and replace `MarkPrice`/`MarkPrices` with `IndexPrice`/`IndexPrices`.

- [ ] **Step 4: Export the module**

Modify `barter-data/src/exchange/binance/futures/mod.rs`:

```rust
pub mod index_price;
```

- [ ] **Step 5: Verify Task 3 passes**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task3 \
  rtk cargo test -p barter-data index_price -- --nocapture
rtk git diff --check
```

Expected: REST URL, REST fixture, conversion, and HTTP status structural tests pass.

- [ ] **Step 6: Commit Task 3**

```bash
rtk git add barter-data/src/exchange/binance/futures/index_price.rs barter-data/src/exchange/binance/futures/mod.rs
rtk git commit -m "feat(binance): add futures index price REST parser"
```

## Task 4: Binance WebSocket Mapping and StreamSelector

**Files:**
- Modify: `barter-data/src/exchange/binance/futures/index_price.rs`
- Modify: `barter-data/src/exchange/binance/futures/mod.rs`
- Modify: `barter-data/src/exchange/binance/mod.rs` if typed channel identifiers are implemented there

- [ ] **Step 1: Write failing WebSocket tests**

Add to `index_price.rs` tests:

```rust
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
fn binance_futures_index_price_ws_deserialises_from_mark_price_payload() {
    let actual = serde_json::from_str::<BinanceFuturesIndexPriceWs>(ws_fixture()).unwrap();

    assert_eq!(actual.event_type, "markPriceUpdate");
    assert_eq!(
        actual.subscription_id,
        SubscriptionId::from("@markPrice@1s|BTCUSDT")
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
```

Also add a compile-only selector test in `futures/mod.rs`:

```rust
#[test]
fn binance_futures_usd_selects_index_prices_from_mark_price_stream_payload() {
    fn assert_selector<Instrument>()
    where
        Instrument: crate::instrument::InstrumentData,
        BinanceFuturesUsd: StreamSelector<Instrument, crate::subscription::index_price::IndexPrices>,
    {
    }

    assert_selector::<crate::instrument::MarketInstrumentData<&'static str>>();
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task4 \
  rtk cargo test -p barter-data binance_futures_index_price_ws -- --nocapture
```

Expected: compile failure because `BinanceFuturesIndexPriceWs` and selector are missing.

- [ ] **Step 3: Implement WebSocket raw payload and conversion**

Add imports and type in `index_price.rs`:

```rust
use crate::{Identifier, exchange::SubscriptionId};

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

impl From<BinanceFuturesIndexPriceWs> for IndexPrice {
    fn from(value: BinanceFuturesIndexPriceWs) -> Self {
        Self {
            event_time: value.event_time,
            index_price: value.index_price,
        }
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceFuturesIndexPriceWs)>
    for MarketIter<InstrumentKey, IndexPrice>
{
    fn from((exchange, instrument, value): (ExchangeId, InstrumentKey, BinanceFuturesIndexPriceWs)) -> Self {
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

fn de_index_price_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer).map(|market| {
        crate::exchange::binance::BinanceSub::from((
            crate::exchange::binance::BinanceChannel::MARK_PRICE_1S,
            market,
        ))
        .id()
    })
}
```

Use the exact `BinanceSub` or `ExchangeSub` name present in `mark_price.rs` if it differs.

- [ ] **Step 4: Implement typed subscription channel mapping**

Where `Identifier<BinanceMarket>` is implemented for `MarkPrices`, add equivalent for `IndexPrices` using the same Binance channel constant:

```rust
impl<Instrument> Identifier<BinanceMarket>
    for Subscription<BinanceFuturesUsd, Instrument, IndexPrices>
where
    Instrument: InstrumentData,
{
    fn id(&self) -> BinanceMarket {
        BinanceMarket::new(&self.instrument, BinanceChannel::MARK_PRICE_1S)
    }
}
```

Adjust the constructor to match the real pattern in `barter-data/src/exchange/binance/mod.rs`.

- [ ] **Step 5: Implement StreamSelector**

Modify `barter-data/src/exchange/binance/futures/mod.rs` imports:

```rust
index_price::BinanceFuturesIndexPriceWs,
```

and:

```rust
index_price::IndexPrices,
```

Add selector:

```rust
impl<Instrument> StreamSelector<Instrument, IndexPrices> for BinanceFuturesUsd
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = BinanceWsStream<
        StatelessTransformer<Self, Instrument::Key, IndexPrices, BinanceFuturesIndexPriceWs>,
    >;
}
```

- [ ] **Step 6: Verify Task 4 passes**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task4 \
  rtk cargo test -p barter-data binance_futures_index_price_ws -- --nocapture
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task4 \
  rtk cargo test -p barter-data binance_futures_usd_selects_index_prices -- --nocapture
rtk git diff --check
```

Expected: WebSocket fixture and compile-only selector tests pass.

- [ ] **Step 7: Commit Task 4**

```bash
rtk git add barter-data/src/exchange/binance/futures/index_price.rs barter-data/src/exchange/binance/futures/mod.rs barter-data/src/exchange/binance/mod.rs
rtk git commit -m "feat(binance): stream futures index prices"
```

## Task 5: DynamicStreams Wiring

**Files:**
- Modify: `barter-data/src/streams/builder/dynamic/mod.rs`

- [ ] **Step 1: Write failing dynamic tests**

Add tests near existing `Channels::try_from` tests:

```rust
#[test]
fn channels_accept_binance_futures_perpetual_index_prices() {
    use crate::subscription::index_price::IndexPrices;
    use barter_instrument::instrument::market_data::MarketDataInstrumentKind;

    let subscriptions = vec![Subscription::new(
        ExchangeId::BinanceFuturesUsd,
        ("btc", "usdt", MarketDataInstrumentKind::Perpetual),
        SubKind::IndexPrices,
    )];

    let channels = Channels::try_from(&[subscriptions]).unwrap();
    assert!(channels.txs.index_prices.contains_key(ExchangeId::BinanceFuturesUsd as usize));
}

#[test]
fn channels_reject_binance_futures_spot_index_prices() {
    use barter_instrument::instrument::market_data::MarketDataInstrumentKind;

    let subscriptions = vec![Subscription::new(
        ExchangeId::BinanceFuturesUsd,
        ("btc", "usdt", MarketDataInstrumentKind::Spot),
        SubKind::IndexPrices,
    )];

    assert!(Channels::try_from(&[subscriptions]).is_err());
}
```

If `VecMap::contains_key` takes `usize`, use the cast shown. If existing tests use `get`, follow the existing style.

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task5 \
  rtk cargo test -p barter-data channels_accept_binance_futures_perpetual_index_prices -- --nocapture
```

Expected: compile failure because `index_prices` channel storage does not exist.

- [ ] **Step 3: Add channel storage and bounds**

Update imports:

```rust
index_price::{IndexPrice, IndexPrices},
```

Add to `DynamicStreams`:

```rust
pub index_prices:
    VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, IndexPrice>>>,
```

Add matching sender storage to the internal `Channels` structs:

```rust
index_prices: VecMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, IndexPrice>>>,
```

Add an `IndexPrices` Binance identifier bound to `DynamicStreams::init`:

```rust
Subscription<BinanceFuturesUsd, Instrument, IndexPrices>: Identifier<BinanceMarket>,
```

- [ ] **Step 4: Add initialization arm**

In the `match (exchange, sub_kind)` inside `DynamicStreams::init`, add:

```rust
(ExchangeId::BinanceFuturesUsd, SubKind::IndexPrices) => {
    init_market_stream(
        STREAM_RECONNECTION_POLICY,
        subs.into_iter()
            .map(|sub| {
                Subscription::new(
                    BinanceFuturesUsd::default(),
                    sub.instrument,
                    IndexPrices,
                )
            })
            .collect(),
    )
    .await
    .map(|stream| {
        tokio::spawn(stream.forward_to(
            txs.index_prices.get(&exchange).unwrap().clone(),
        ))
    })
}
```

- [ ] **Step 5: Add selectors**

Add methods following `select_mark_prices` style:

```rust
pub fn select_index_prices(
    &mut self,
    exchange: ExchangeId,
) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, IndexPrice>>> {
    self.index_prices.remove(exchange as usize)
}

pub fn select_all_index_prices(
    &mut self,
) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, IndexPrice>>> {
    self.index_prices.drain().map(|(_, rx)| rx).collect()
}
```

Update `select_all` if the existing method includes every streamable kind:

```rust
select_all.extend(self.select_all_index_prices().map(MarketStreamResult::from));
```

- [ ] **Step 6: Verify Task 5 passes**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task5 \
  rtk cargo test -p barter-data channels_ -- --nocapture
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-task5 \
  rtk cargo test -p barter-data index_price -- --nocapture
rtk git diff --check
```

Expected: channel tests and all index price focused tests pass.

- [ ] **Step 7: Commit Task 5**

```bash
rtk git add barter-data/src/streams/builder/dynamic/mod.rs
rtk git commit -m "feat(data): wire dynamic index price streams"
```

## Task 6: Examples, Full Verification, and Review

**Files:**
- Create or modify examples under `barter-data/examples/`
- No production code unless verification exposes a failing task

- [ ] **Step 1: Add examples**

Add `barter-data/examples/binance_futures_index_price_rest.rs`:

```rust
use barter_data::{
    exchange::binance::futures::index_price::BinanceFuturesUsdIndexPriceFetcher,
    subscription::{Subscription, index_price::IndexPrices},
};
use barter_instrument::instrument::market_data::MarketDataInstrumentKind;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriptions = vec![Subscription::from((
        barter_data::exchange::binance::futures::BinanceFuturesUsd::default(),
        "btc",
        "usdt",
        MarketDataInstrumentKind::Perpetual,
        IndexPrices,
    ))];

    let events = BinanceFuturesUsdIndexPriceFetcher::fetch(&subscriptions).await?;
    println!("{events:#?}");
    Ok(())
}
```

Add `barter-data/examples/binance_futures_index_price_stream.rs` by adapting the existing Binance Futures mark price stream example. It should subscribe to `IndexPrices` and document in a comment that Binance provider transport is `@markPrice@1s` while the normalized output is `IndexPrice`.

- [ ] **Step 2: Check examples compile**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-examples \
  rtk cargo check -p barter-data --examples
```

Expected: examples compile without live network calls.

- [ ] **Step 3: Run full focused verification**

Run:

```bash
rtk cargo fmt --check
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-final \
  rtk cargo test -p barter-data index_price -- --nocapture
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-final \
  rtk cargo test -p barter-data mark_price -- --nocapture
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-final \
  rtk cargo test -p barter-data channels_ -- --nocapture
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-final \
  rtk cargo check -p barter-data
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-final \
  rtk cargo check -p barter-data --examples
rtk git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 4: Commit examples**

```bash
rtk git add barter-data/examples barter-data/src
rtk git commit -m "docs(data): add index price examples"
```

- [ ] **Step 5: Request code review**

Use the code review workflow and ask reviewers to focus on:

```text
Review Binance USD-M Futures Index Price support. Check especially:
1. IndexPrices emits only IndexPrice events even though Binance provider stream is @markPrice@1s.
2. REST fetcher uses /fapi/v1/premiumIndex and error_for_status().
3. DynamicStreams support matrix accepts only BinanceFuturesUsd perpetual index_prices.
4. DataKind and typed/generic conversions are consistent with mark_price and funding_rate.
```

- [ ] **Step 6: Address Critical and Important review findings**

For each Critical or Important item:

1. Verify the finding against the code.
2. Add a failing regression test.
3. Implement the smallest fix.
4. Run the focused test.
5. Commit the fix.

- [ ] **Step 7: Final verification before completion**

Run:

```bash
rtk cargo fmt --check
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-final \
  rtk cargo test -p barter-data
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-final \
  rtk cargo check -p barter-data
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-index-price-final \
  rtk cargo check -p barter-data --examples
rtk git diff --check
rtk git status --short
```

Expected: all commands exit 0 and `git status --short` is empty after final commits.

## Self-Review

- Spec coverage: Tasks cover normalized model, `SubKind::IndexPrices`, serde, support matrix, `DataKind`, REST premium index extraction, WebSocket mark price payload extraction, DynamicStreams wiring, examples, verification, and code review.
- Non-goals preserved: no composite index constituents, no asset index streams, no index price klines, no all-market stream, no REST polling scheduler, no fake independent WebSocket channel.
- Red-flag scan: no unresolved open implementation markers are intentionally left in the plan. Any branch-specific wording that says to follow existing style names a concrete file to copy from.
- Type consistency: `IndexPrices` is the subscription marker, `IndexPrice` is the normalized event, `SubKind::IndexPrices` serializes as `index_prices`, and `DataKind::IndexPrice` reports `index_price`.
