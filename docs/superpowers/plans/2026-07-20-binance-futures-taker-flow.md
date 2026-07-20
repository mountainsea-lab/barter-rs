# Binance USD-M Futures Taker Flow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Binance USD-M Futures REST-first taker buy/sell volume collection for factor computation and strategy analysis.

**Architecture:** This adds a normalized REST-only `TakerFlow` data kind, wires it into `SubKind`/`DataKind`/support validation, explicitly rejects `DynamicStreams`, then adds a Binance USD-M `/futures/data/takerlongshortRatio` fetcher and no-credential example. The implementation follows existing funding and open-interest REST patterns and does not add WebSocket channel support.

**Tech Stack:** Rust, serde, chrono, rust_decimal, reqwest, tokio, barter-data subscription/event/exchange abstractions.

---

## Spec Reference

Implement `docs/superpowers/specs/2026-07-20-binance-futures-taker-flow-design.md`.

## File Structure

**Create:**
- `barter-data/src/subscription/taker_flow.rs`
  - Defines `TakerFlows` marker and normalized `TakerFlow` event model.
- `barter-data/src/exchange/binance/futures/taker_flow.rs`
  - Defines Binance REST URL builder, raw REST row, conversion, and fetcher.
- `barter-data/examples/binance_futures_taker_flow.rs`
  - No-credential example fetching recent BTCUSDT taker flow rows.

**Modify:**
- `barter-data/src/subscription/mod.rs`
  - Exports `taker_flow`, adds `SubKind::TakerFlows`, updates support matrix and serde tests.
- `barter-data/src/event.rs`
  - Adds `DataKind::TakerFlow` and `as_taker_flow` helper.
- `barter-data/src/streams/builder/dynamic/mod.rs`
  - Imports only enough to reject `SubKind::TakerFlows` through existing unsupported-sub-kind path and adds a test.
- `barter-data/src/exchange/binance/futures/mod.rs`
  - Exports `taker_flow` with a doc comment.

## Task 1: Add normalized TakerFlow model, SubKind, DataKind, and support matrix

**Files:**
- Create: `barter-data/src/subscription/taker_flow.rs`
- Modify: `barter-data/src/subscription/mod.rs`
- Modify: `barter-data/src/event.rs`

- [ ] **Step 1: Write failing normalized model and routing tests**

Add this new file `barter-data/src/subscription/taker_flow.rs`:

```rust
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
```

In `barter-data/src/subscription/mod.rs`, add these test expectations before implementation:

```rust
// In sub_kind_serde_uses_subscription_kind_strings cases:
(SubKind::TakerFlows, "taker_flows"),
```

Add this test near existing support matrix tests in `barter-data/src/subscription/mod.rs`:

```rust
#[test]
fn binance_futures_usd_supports_taker_flows_for_perpetuals_only() {
    use crate::subscription::taker_flow::TakerFlows;
    use barter_instrument::instrument::market_data::{
        MarketDataInstrument, kind::MarketDataInstrumentKind,
    };

    let valid = Subscription::from((
        ExchangeId::BinanceFuturesUsd,
        "btc",
        "usdt",
        MarketDataInstrumentKind::Perpetual,
        TakerFlows,
    ));
    assert!(valid.validate().is_ok());

    let invalid = Subscription::from((
        ExchangeId::BinanceFuturesUsd,
        "btc",
        "usdt",
        MarketDataInstrumentKind::Spot,
        TakerFlows,
    ));
    assert!(invalid.validate().is_err());

    assert!(exchange_supports_instrument_kind_sub_kind(
        &ExchangeId::BinanceFuturesUsd,
        &MarketDataInstrumentKind::Perpetual,
        SubKind::TakerFlows,
    ));
    assert!(!exchange_supports_instrument_kind_sub_kind(
        &ExchangeId::BinanceSpot,
        &MarketDataInstrumentKind::Spot,
        SubKind::TakerFlows,
    ));

    let _instrument = MarketDataInstrument::from((
        "btc",
        "usdt",
        MarketDataInstrumentKind::Perpetual,
    ));
}
```

In `barter-data/src/event.rs`, add this test near existing `DataKind` tests:

```rust
#[test]
fn data_kind_taker_flow_helper_returns_taker_flow_event() {
    use crate::subscription::taker_flow::TakerFlow;
    use rust_decimal_macros::dec;

    let time = Utc::now();
    let event = MarketEvent {
        time_exchange: time,
        time_received: time,
        exchange: ExchangeId::BinanceFuturesUsd,
        instrument: MarketDataInstrument::from((
            "btc",
            "usdt",
            barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind::Perpetual,
        )),
        kind: DataKind::TakerFlow(TakerFlow {
            period_start: time,
            buy_volume: dec!(387.3300),
            sell_volume: dec!(270.0700),
            buy_sell_ratio: dec!(1.4342),
        }),
    };

    let actual = event.as_taker_flow().expect("taker flow event");
    assert_eq!(actual.kind.buy_volume, dec!(387.3300));
    assert_eq!(actual.kind.sell_volume, dec!(270.0700));
    assert_eq!(actual.kind.buy_sell_ratio, dec!(1.4342));
}
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow /Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data taker_flow -- --nocapture
```

Expected: FAIL. Acceptable RED failures include missing `SubKind::TakerFlows`, missing `subscription::taker_flow` export, missing `DataKind::TakerFlow`, or missing `as_taker_flow`.

- [ ] **Step 3: Implement model, exports, support matrix, and DataKind**

In `barter-data/src/subscription/mod.rs`:

```rust
/// Taker flow [`SubscriptionKind`] and the associated Barter output data model.
pub mod taker_flow;
```

Add `TakerFlows` to `SubKind`:

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
    OpenInterests,
    TakerFlows,
}
```

Update Binance USD-M support matrix arm:

```rust
(
    BinanceFuturesUsd,
    Perpetual,
    PublicTrades | OrderBooksL1 | OrderBooksL2 | Liquidations | Candles | MarkPrices
    | FundingRates | IndexPrices | OpenInterests | TakerFlows,
) => true,
```

In `barter-data/src/event.rs`, import and add the kind:

```rust
use crate::subscription::taker_flow::TakerFlow;
```

```rust
pub enum DataKind {
    Trade(PublicTrade),
    OrderBookL1(OrderBookL1),
    OrderBook(OrderBookEvent),
    Candle(Candle),
    FundingRate(FundingRate),
    MarkPrice(MarkPrice),
    IndexPrice(IndexPrice),
    OpenInterest(OpenInterest),
    TakerFlow(TakerFlow),
    Liquidation(Liquidation),
}
```

Add helper:

```rust
pub fn as_taker_flow(&self) -> Option<MarketEvent<&InstrumentKey, &TakerFlow>> {
    match &self.kind {
        DataKind::TakerFlow(taker_flow) => Some(self.as_event(taker_flow)),
        _ => None,
    }
}
```

Update `kind_name()`:

```rust
DataKind::TakerFlow(_) => "taker_flow",
```

- [ ] **Step 4: Run focused tests to verify GREEN**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow /Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data taker_flow -- --nocapture
```

Expected: PASS for taker flow tests in subscription and event modules.

- [ ] **Step 5: Format, diff-check, and commit Task 1**

Run:

```bash
/Volumes/wdata/rust/.cargo/bin/cargo fmt --all -- --check
git diff --check
git status --short
```

Expected: fmt and diff-check exit 0. `git status --short` shows only Task 1 files.

Commit:

```bash
git add barter-data/src/subscription/taker_flow.rs barter-data/src/subscription/mod.rs barter-data/src/event.rs
git commit -m "feat(data): add taker flow model"
```

## Task 2: Add explicit DynamicStreams REST-only rejection

**Files:**
- Modify: `barter-data/src/streams/builder/dynamic/mod.rs`

- [ ] **Step 1: Write failing rejection test**

Add this test near `channels_reject_open_interests_runtime_without_rest_source`:

```rust
#[test]
fn channels_reject_taker_flows_runtime_without_rest_source() {
    let batches: Vec<Vec<Subscription<ExchangeId, MarketDataInstrument, SubKind>>> =
        vec![vec![Subscription::new(
            ExchangeId::BinanceFuturesUsd,
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Perpetual)),
            SubKind::TakerFlows,
        )]];

    let actual = Channels::try_from(&batches);

    match actual {
        Err(error) => assert_eq!(error, DataError::UnsupportedSubKind(SubKind::TakerFlows)),
        Ok(_) => panic!("TakerFlows dynamic channel allocation should be unsupported"),
    }
}
```

- [ ] **Step 2: Run test to verify RED or existing behavior**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow /Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data channels_reject_taker_flows_runtime_without_rest_source -- --nocapture
```

Expected: If Task 1 is complete and `Channels` falls through to `unsupported`, this may PASS immediately. If it fails to compile, implement Step 3. An immediate PASS is acceptable because the test locks the intended REST-only boundary.

- [ ] **Step 3: Ensure no channel allocation is added for TakerFlows**

Verify the `Channels::try_from` match ends with this catch-all and does not include a `SubKind::TakerFlows` allocation arm:

```rust
unsupported => return Err(DataError::UnsupportedSubKind(unsupported)),
```

Do not add `taker_flows` fields to `DynamicStreams`, `Txs`, or `Rxs`.

- [ ] **Step 4: Run focused tests**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow /Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data channels_reject_taker_flows_runtime_without_rest_source -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Format, diff-check, and commit Task 2**

Run:

```bash
/Volumes/wdata/rust/.cargo/bin/cargo fmt --all -- --check
git diff --check
git status --short
```

Commit:

```bash
git add barter-data/src/streams/builder/dynamic/mod.rs
git commit -m "feat(data): reject taker flow dynamic streams"
```

## Task 3: Add Binance USD-M Futures taker flow REST fetcher

**Files:**
- Create: `barter-data/src/exchange/binance/futures/taker_flow.rs`
- Modify: `barter-data/src/exchange/binance/futures/mod.rs`

- [ ] **Step 1: Write failing parser, conversion, URL, and HTTP tests**

Create `barter-data/src/exchange/binance/futures/taker_flow.rs` with tests first and stubs only if needed for compiler discovery:

```rust
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

pub const HTTP_TAKER_LONG_SHORT_RATIO_URL_BINANCE_FUTURES_USD: &str =
    "https://fapi.binance.com/futures/data/takerlongshortRatio";

pub fn taker_flow_url(symbol: &str, period: &str, limit: Option<u16>) -> String {
    match limit {
        Some(limit) => format!(
            "{HTTP_TAKER_LONG_SHORT_RATIO_URL_BINANCE_FUTURES_USD}?symbol={symbol}&period={period}&limit={limit}"
        ),
        None => format!(
            "{HTTP_TAKER_LONG_SHORT_RATIO_URL_BINANCE_FUTURES_USD}?symbol={symbol}&period={period}"
        ),
    }
}

#[derive(Debug)]
pub struct BinanceFuturesUsdTakerFlowFetcher;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesTakerFlowRest {
    #[serde(
        alias = "buySellRatio",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub buy_sell_ratio: Decimal,
    #[serde(alias = "buyVol", deserialize_with = "rust_decimal::serde::str::deserialize")]
    pub buy_volume: Decimal,
    #[serde(alias = "sellVol", deserialize_with = "rust_decimal::serde::str::deserialize")]
    pub sell_volume: Decimal,
    #[serde(
        alias = "timestamp",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub timestamp: DateTime<Utc>,
}
```

Add tests in the same file:

```rust
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
    fn binance_futures_taker_flow_rest_deserialises() {
        let rows = serde_json::from_str::<Vec<BinanceFuturesTakerFlowRest>>(rest_fixture()).unwrap();
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
        let actual = crate::event::MarketIter::<
            &str,
            crate::subscription::taker_flow::TakerFlow,
        >::from((
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
```

In `barter-data/src/exchange/binance/futures/mod.rs`, add before running RED:

```rust
/// Taker buy/sell volume REST types and fetcher.
pub mod taker_flow;
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow /Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data taker_flow -- --nocapture
```

Expected: FAIL due to missing `From<BinanceFuturesTakerFlowRest> for TakerFlow`, missing `MarketIter` conversion, or missing `fetch_taker_flow_url`.

- [ ] **Step 3: Implement conversions and fetcher**

Add to `barter-data/src/exchange/binance/futures/taker_flow.rs`:

```rust
impl BinanceFuturesUsdTakerFlowFetcher {
    pub fn fetch_recent<Instrument>(
        subscriptions: &[Subscription<BinanceFuturesUsd, Instrument, crate::subscription::taker_flow::TakerFlows>],
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
        subscriptions: &[Subscription<BinanceFuturesUsd, Instrument, crate::subscription::taker_flow::TakerFlows>],
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
```

Keep `fetch_taker_flow_url` private. Only expose the URL builder, raw type, and public fetcher.

- [ ] **Step 4: Run focused tests to verify GREEN**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow /Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data taker_flow -- --nocapture
```

Expected: PASS for all taker flow tests.

- [ ] **Step 5: Format, diff-check, and commit Task 3**

Run:

```bash
/Volumes/wdata/rust/.cargo/bin/cargo fmt --all -- --check
git diff --check
git status --short
```

Commit:

```bash
git add barter-data/src/exchange/binance/futures/taker_flow.rs barter-data/src/exchange/binance/futures/mod.rs
git commit -m "feat(data): fetch binance futures taker flow"
```

## Task 4: Add no-credential taker flow example

**Files:**
- Create: `barter-data/examples/binance_futures_taker_flow.rs`

- [ ] **Step 1: Run example check to verify RED**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow /Volumes/wdata/rust/.cargo/bin/cargo check -p barter-data --example binance_futures_taker_flow
```

Expected: FAIL because the example target does not exist.

- [ ] **Step 2: Add no-credential example**

Create `barter-data/examples/binance_futures_taker_flow.rs`:

```rust
use barter_data::{
    exchange::binance::futures::{
        BinanceFuturesUsd, taker_flow::BinanceFuturesUsdTakerFlowFetcher,
    },
    subscription::{Subscription, taker_flow::TakerFlows},
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let subscriptions = vec![Subscription::from((
        BinanceFuturesUsd::default(),
        "btc",
        "usdt",
        MarketDataInstrumentKind::Perpetual,
        TakerFlows,
    ))];

    // REST-only, no credentials required. Fetch recent Binance USD-M Futures
    // taker buy/sell volume rows for BTCUSDT 5 minute periods.
    let events = BinanceFuturesUsdTakerFlowFetcher::fetch_recent(&subscriptions, "5m", Some(5))
        .await
        .expect("fetch Binance USD-M Futures taker flow");

    tracing::info!(count = events.len(), "received Binance USD-M Futures taker flow rows");

    for event in events.iter().take(5) {
        tracing::info!(?event, "received Binance USD-M Futures taker flow");
    }
}
```

- [ ] **Step 3: Run example check to verify GREEN**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow /Volumes/wdata/rust/.cargo/bin/cargo check -p barter-data --example binance_futures_taker_flow
```

Expected: PASS.

- [ ] **Step 4: Run focused taker flow tests again**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow /Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data taker_flow -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Format, diff-check, and commit Task 4**

Run:

```bash
/Volumes/wdata/rust/.cargo/bin/cargo fmt --all -- --check
git diff --check
git status --short
```

Commit:

```bash
git add barter-data/examples/binance_futures_taker_flow.rs
git commit -m "feat(data): add futures taker flow example"
```

## Task 5: Final verification and review readiness

**Files:**
- No source changes expected unless verification finds an issue.

- [ ] **Step 1: Run final verification matrix**

Use disk-sensitive settings:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow-final /Volumes/wdata/rust/.cargo/bin/cargo fmt --all -- --check
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow-final /Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data -- --nocapture
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow-final /Volumes/wdata/rust/.cargo/bin/cargo check -p barter-data
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow-final /Volumes/wdata/rust/.cargo/bin/cargo check -p barter-data --example binance_futures_taker_flow
git diff --check
git status --short
```

Expected:
- `cargo fmt --check` exit 0.
- `cargo test -p barter-data` exit 0.
- `cargo check -p barter-data` exit 0.
- example check exit 0.
- `git diff --check` exit 0.
- `git status --short` empty.

- [ ] **Step 2: Clean only this task's temporary target directories**

Run:

```bash
rm -rf /tmp/barter-rs-target-taker-flow /tmp/barter-rs-target-taker-flow-final
```

Expected: command exits 0.

- [ ] **Step 3: Confirm clean status**

Run:

```bash
git status --short
```

Expected: no output.

- [ ] **Step 4: Prepare review summary**

Summarize:

```text
Implemented Binance USD-M Futures REST-first taker flow.
Scope: normalized TakerFlow/TakerFlows, SubKind/DataKind/support matrix, DynamicStreams rejection, REST fetcher, no-credential example.
Non-scope: aggTrade WebSocket, realtime aggregation, historical pagination, MDB bridge, factor engine.
Verification: include exact commands and pass/fail counts from Step 1.
```

- [ ] **Step 5: Request two-stage review**

Run spec compliance and quality review according to the user's established workflow. Reviewers should check:

```text
Spec compliance: Does implementation satisfy docs/superpowers/specs/2026-07-20-binance-futures-taker-flow-design.md without adding non-goals?
Quality: Are public APIs minimal, REST error handling correct, Decimal semantics preserved, tests meaningful, and DynamicStreams boundary explicit?
```

If review returns Critical or Important findings, fix them with TDD, run focused verification, amend or add a follow-up commit, and rerun review.
