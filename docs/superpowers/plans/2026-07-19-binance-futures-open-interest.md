# Binance Futures Open Interest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add first-class Binance USD-M Futures latest Open Interest support as a REST-first data type.

**Architecture:** Follow the existing `FundingRates` REST-first boundary and `IndexPrices` REST fetcher testing style. Add normalized `OpenInterest` types, wire them into `SubKind`/`DataKind`/support matrix, explicitly reject `DynamicStreams`, then add a Binance REST fetcher and example.

**Tech Stack:** Rust, `serde`, `chrono`, `rust_decimal`, `reqwest`, `futures_util::try_join_all`, existing `barter-data` subscription/event/exchange patterns.

---

## Spec Reference

Implementation must match `docs/superpowers/specs/2026-07-19-binance-futures-open-interest-design.md`.

## File Structure

Create:

- `barter-data/src/subscription/open_interest.rs`
  - Owns the `OpenInterests` marker and normalized `OpenInterest` event model.
- `barter-data/src/exchange/binance/futures/open_interest.rs`
  - Owns Binance USD-M REST URL construction, raw response parsing, raw-to-normalized conversion, and `BinanceFuturesUsdOpenInterestFetcher`.
- `barter-data/examples/binance_futures_open_interest.rs`
  - Demonstrates fetching latest open interest without credentials.

Modify:

- `barter-data/src/subscription/mod.rs`
  - Export `open_interest`, add `SubKind::OpenInterests`, serde support, support matrix, and support tests.
- `barter-data/src/event.rs`
  - Add `OpenInterest` to `DataKind`, `kind_name`, accessor, and `MarketEvent` conversion.
- `barter-data/src/exchange/binance/futures/mod.rs`
  - Export `open_interest` module only. Do not add a `StreamSelector`.
- `barter-data/src/streams/builder/dynamic/mod.rs`
  - Add explicit `OpenInterests` runtime rejection in tests and implementation matching `FundingRates` behavior if the compiler requires match exhaustiveness changes.

Do not add WebSocket channel selectors, `DynamicStreams` storage, or polling runtime.

---

### Task 1: Add Open Interest subscription model, SubKind, DataKind, and support matrix

**Files:**

- Create: `barter-data/src/subscription/open_interest.rs`
- Modify: `barter-data/src/subscription/mod.rs`
- Modify: `barter-data/src/event.rs`

- [ ] **Step 1: Write failing tests for the subscription model**

Create `barter-data/src/subscription/open_interest.rs` with tests first:

```rust
use super::SubscriptionKind;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Barter [`SubscriptionKind`] marker that yields [`OpenInterest`] events.
///
/// Open interest is REST-first data in the current architecture. This marker
/// identifies the normalized output type, but does not imply WebSocket stream
/// support or `DynamicStreams` wiring.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub struct OpenInterests;

impl SubscriptionKind for OpenInterests {
    type Event = OpenInterest;

    fn as_str(&self) -> &'static str {
        "open_interests"
    }
}

impl std::fmt::Display for OpenInterests {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter open interest model.
///
/// `open_interest` is the quantity reported by the source exchange for the
/// futures symbol. For Binance USD-M perpetual contracts this should be treated
/// as base-asset quantity, not quote notional.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct OpenInterest {
    pub event_time: DateTime<Utc>,
    pub open_interest: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    #[test]
    fn open_interests_kind_formats_as_expected() {
        assert_eq!(OpenInterests.as_str(), "open_interests");
        assert_eq!(OpenInterests.to_string(), "open_interests");
    }

    #[test]
    fn open_interest_model_uses_decimal_quantity() {
        let time = Utc::now();
        let actual = OpenInterest {
            event_time: time,
            open_interest: dec!(10659.509),
        };

        assert_eq!(actual.event_time, time);
        assert_eq!(actual.open_interest, dec!(10659.509));
    }
}
```

- [ ] **Step 2: Run model tests to verify they fail due to missing module export**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest rtk cargo test -p barter-data open_interest -- --nocapture
```

Expected: FAIL with an error showing `open_interest.rs` is not compiled or `super::SubscriptionKind` cannot be reached because `subscription::open_interest` has not been exported from `subscription/mod.rs`.

- [ ] **Step 3: Export the module and add failing SubKind/support tests**

Modify `barter-data/src/subscription/mod.rs` near the existing module exports:

```rust
/// Index price [`SubscriptionKind`] and the associated Barter output data model.
pub mod index_price;

/// Mark price [`SubscriptionKind`] and the associated Barter output data model.
pub mod mark_price;

/// Open interest [`SubscriptionKind`] and the associated Barter output data model.
pub mod open_interest;
```

Add `OpenInterests` to `SubKind` after `IndexPrices`:

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
}
```

In `sub_kind_serde_uses_subscription_kind_strings`, add the new case:

```rust
(SubKind::OpenInterests, "open_interests"),
```

In `mod support_matrix`, add tests:

```rust
#[test]
fn test_binance_futures_usd_supports_perpetual_open_interests() {
    assert!(exchange_supports_instrument_kind_sub_kind(
        &ExchangeId::BinanceFuturesUsd,
        &MarketDataInstrumentKind::Perpetual,
        SubKind::OpenInterests,
    ));
}

#[test]
fn test_binance_futures_usd_rejects_spot_open_interests() {
    assert!(!exchange_supports_instrument_kind_sub_kind(
        &ExchangeId::BinanceFuturesUsd,
        &MarketDataInstrumentKind::Spot,
        SubKind::OpenInterests,
    ));
}

#[test]
fn test_binance_spot_rejects_open_interests() {
    assert!(!exchange_supports_instrument_kind_sub_kind(
        &ExchangeId::BinanceSpot,
        &MarketDataInstrumentKind::Spot,
        SubKind::OpenInterests,
    ));
}
```

- [ ] **Step 4: Run support tests to verify they fail before support matrix update**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest rtk cargo test -p barter-data open_interests -- --nocapture
```

Expected: FAIL at `test_binance_futures_usd_supports_perpetual_open_interests` because `exchange_supports_instrument_kind_sub_kind` does not yet include `OpenInterests`.

- [ ] **Step 5: Update the support matrix**

Modify the Binance USD-M perpetual match arm in `exchange_supports_instrument_kind_sub_kind`:

```rust
(
    BinanceFuturesUsd,
    Perpetual,
    PublicTrades | OrderBooksL1 | OrderBooksL2 | Liquidations | Candles | MarkPrices
    | FundingRates | IndexPrices | OpenInterests,
) => true,
```

- [ ] **Step 6: Add failing DataKind tests**

Modify imports in `barter-data/src/event.rs` to include `OpenInterest`:

```rust
subscription::{
    book::{OrderBookEvent, OrderBookL1},
    candle::Candle,
    funding::FundingRate,
    index_price::IndexPrice,
    liquidation::Liquidation,
    mark_price::MarkPrice,
    open_interest::OpenInterest,
    trade::PublicTrade,
},
```

Add this test near existing event tests in `event.rs`:

```rust
#[test]
fn data_kind_supports_open_interest_events() {
    use crate::subscription::open_interest::OpenInterest;
    use rust_decimal_macros::dec;

    let event_time = Utc::now();
    let input = MarketEvent {
        time_exchange: event_time,
        time_received: event_time,
        exchange: ExchangeId::BinanceFuturesUsd,
        instrument: "BTCUSDT",
        kind: OpenInterest {
            event_time,
            open_interest: dec!(10659.509),
        },
    };

    let actual = MarketEvent::<_, DataKind>::from(input);

    assert_eq!(actual.kind.kind_name(), "open_interest");
    let open_interest = actual.as_open_interest().unwrap();
    assert_eq!(open_interest.kind.open_interest, dec!(10659.509));
}
```

- [ ] **Step 7: Run event test to verify it fails**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest rtk cargo test -p barter-data data_kind_supports_open_interest_events -- --nocapture
```

Expected: FAIL with missing `DataKind::OpenInterest`, missing `as_open_interest`, or missing `From<MarketEvent<_, OpenInterest>>` implementation.

- [ ] **Step 8: Implement DataKind support**

In `barter-data/src/event.rs`, add accessor in `impl<InstrumentKey> MarketEvent<InstrumentKey, DataKind>`:

```rust
pub fn as_open_interest(&self) -> Option<MarketEvent<&InstrumentKey, &OpenInterest>> {
    match &self.kind {
        DataKind::OpenInterest(open_interest) => Some(self.as_event(open_interest)),
        _ => None,
    }
}
```

Add enum variant:

```rust
OpenInterest(OpenInterest),
```

Add kind name arm:

```rust
DataKind::OpenInterest(_) => "open_interest",
```

Add conversions after the `IndexPrice` conversions:

```rust
impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, OpenInterest>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, OpenInterest>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, OpenInterest>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, OpenInterest>) -> Self {
        value.map_kind(OpenInterest::into)
    }
}
```

- [ ] **Step 9: Run Task 1 tests to verify they pass**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest rtk cargo test -p barter-data open_interest -- --nocapture
```

Expected: PASS for model, `SubKind`, support matrix, and event tests containing `open_interest` names.

- [ ] **Step 10: Commit Task 1**

Run:

```bash
rtk cargo fmt --all -- --check
rtk git diff --check
git add barter-data/src/subscription/open_interest.rs barter-data/src/subscription/mod.rs barter-data/src/event.rs
git commit -m "feat(data): add open interest model"
```

Expected: commit succeeds.

---

### Task 2: Add explicit DynamicStreams rejection for REST-only OpenInterests

**Files:**

- Modify: `barter-data/src/streams/builder/dynamic/mod.rs`

- [ ] **Step 1: Write failing DynamicStreams rejection test**

In `#[cfg(test)] mod tests` in `barter-data/src/streams/builder/dynamic/mod.rs`, add this test next to `channels_reject_funding_rates_runtime_without_rest_source`:

```rust
#[test]
fn channels_reject_open_interests_runtime_without_rest_source() {
    let batches: Vec<Vec<Subscription<ExchangeId, MarketDataInstrument, SubKind>>> =
        vec![vec![Subscription::new(
            ExchangeId::BinanceFuturesUsd,
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Perpetual)),
            SubKind::OpenInterests,
        )]];

    let actual = Channels::try_from(&batches);

    match actual {
        Err(error) => assert_eq!(error, DataError::UnsupportedSubKind(SubKind::OpenInterests)),
        Ok(_) => panic!("OpenInterests dynamic channel allocation should be unsupported"),
    }
}
```

- [ ] **Step 2: Run test to verify current behavior**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest rtk cargo test -p barter-data channels_reject_open_interests_runtime_without_rest_source -- --nocapture
```

Expected before implementation is complete: either FAIL to compile because `SubKind::OpenInterests` is not handled in a match, or PASS if the existing wildcard unsupported branch already catches it. If it passes, keep the test as the regression boundary.

- [ ] **Step 3: If required, update the `Channels::try_from` match**

Find the match over `subscription.kind` inside `impl TryFrom<&Vec<Vec<Subscription<ExchangeId, Instrument, SubKind>>>> for Channels<Instrument::Key>`. Ensure there is no channel allocation branch for `SubKind::OpenInterests`. The unsupported arm must remain:

```rust
unsupported => return Err(DataError::UnsupportedSubKind(unsupported)),
```

Do not add fields to `Channels` or `DynamicStreams` for open interests.

- [ ] **Step 4: Run focused DynamicStreams test**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest rtk cargo test -p barter-data channels_reject_open_interests_runtime_without_rest_source -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

Run:

```bash
rtk cargo fmt --all -- --check
rtk git diff --check
git add barter-data/src/streams/builder/dynamic/mod.rs
git commit -m "test(data): reject dynamic open interest streams"
```

Expected: commit succeeds.

---

### Task 3: Add Binance USD-M Open Interest REST fetcher

**Files:**

- Create: `barter-data/src/exchange/binance/futures/open_interest.rs`
- Modify: `barter-data/src/exchange/binance/futures/mod.rs`

- [ ] **Step 1: Create REST module with tests and minimal non-compiling references**

Create `barter-data/src/exchange/binance/futures/open_interest.rs`:

```rust
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
        let actual = serde_json::from_str::<BinanceFuturesOpenInterestRest>(rest_fixture()).unwrap();

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
        let actual = OpenInterest::from(input);

        assert_eq!(actual.open_interest, dec!(10659.509));
        assert_eq!(
            actual.event_time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1589437530011))
        );
    }

    #[test]
    fn binance_futures_open_interest_rest_converts_to_market_iter() {
        let input = serde_json::from_str::<BinanceFuturesOpenInterestRest>(rest_fixture()).unwrap();
        let actual = MarketIter::<&str, OpenInterest>::from((
            ExchangeId::BinanceFuturesUsd,
            "BTCUSDT",
            input,
        ));

        assert_eq!(actual.0.len(), 1);
        let event = actual.0.into_iter().next().unwrap().unwrap();
        assert_eq!(event.exchange, ExchangeId::BinanceFuturesUsd);
        assert_eq!(event.instrument, "BTCUSDT");
        assert_eq!(event.time_exchange, event.kind.event_time);
        assert_eq!(event.kind.open_interest, dec!(10659.509));
    }

    #[tokio::test]
    async fn open_interest_fetcher_surfaces_http_error_statuses_before_json_decoding() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            use std::io::{Read, Write};
            let mut buffer = [0; 1024];
            let _ = stream.read(&mut buffer).unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 418 I'm a teapot\r\nContent-Type: application/json\r\nContent-Length: 16\r\n\r\nnot valid json {}",
                )
                .unwrap();
        });

        let actual = fetch_open_interest_url(format!("http://{addr}/openInterest")).await;
        server.join().unwrap();

        let error = actual.expect_err("HTTP status errors should be surfaced");
        match error {
            barter_integration::error::SocketError::Http(error) => {
                assert_eq!(error.status(), Some(reqwest::StatusCode::IM_A_TEAPOT));
            }
            other => panic!("expected HTTP status error, got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run REST tests to verify they fail before module export**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest rtk cargo test -p barter-data open_interest -- --nocapture
```

Expected: FAIL if `open_interest` is not exported from `futures/mod.rs`, or PASS for module-local tests after export is added in the next step.

- [ ] **Step 3: Export the REST module**

Modify `barter-data/src/exchange/binance/futures/mod.rs` near the existing REST module exports:

```rust
/// Index price REST and WebSocket types.
pub mod index_price;

/// Open interest REST types and fetcher.
pub mod open_interest;
```

Do not add `open_interest` to the `use self::{...}` WebSocket import block and do not add a `StreamSelector` implementation.

- [ ] **Step 4: Run REST tests to verify they pass**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest rtk cargo test -p barter-data open_interest -- --nocapture
```

Expected: PASS for URL, deserialization, conversion, HTTP status behavior, and previous Task 1/2 open interest tests.

- [ ] **Step 5: Commit Task 3**

Run:

```bash
rtk cargo fmt --all -- --check
rtk git diff --check
git add barter-data/src/exchange/binance/futures/open_interest.rs barter-data/src/exchange/binance/futures/mod.rs
git commit -m "feat(binance): add futures open interest REST fetcher"
```

Expected: commit succeeds.

---

### Task 4: Add no-credential example

**Files:**

- Create: `barter-data/examples/binance_futures_open_interest.rs`

- [ ] **Step 1: Add the example**

Create `barter-data/examples/binance_futures_open_interest.rs`:

```rust
use barter_data::{
    exchange::binance::futures::{BinanceFuturesUsd, open_interest::BinanceFuturesUsdOpenInterestFetcher},
    subscription::{Subscription, open_interest::OpenInterests},
};
use barter_instrument::instrument::market_data::{
    MarketDataInstrument, kind::MarketDataInstrumentKind,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriptions = vec![Subscription::<BinanceFuturesUsd, MarketDataInstrument, OpenInterests>::new(
        BinanceFuturesUsd::default(),
        MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Perpetual)),
        OpenInterests,
    )];

    let open_interests = BinanceFuturesUsdOpenInterestFetcher::fetch_latest(&subscriptions).await?;

    for event in open_interests {
        println!("{event:?}");
    }

    Ok(())
}
```

- [ ] **Step 2: Run examples check to verify it compiles**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest rtk cargo check -p barter-data --examples
```

Expected: PASS. If the example fails because `BinanceFuturesUsd::default()` is not the established pattern, inspect `barter-data/examples/binance_futures_funding.rs` and align this example exactly with that constructor pattern.

- [ ] **Step 3: Commit Task 4**

Run:

```bash
rtk cargo fmt --all -- --check
rtk git diff --check
git add barter-data/examples/binance_futures_open_interest.rs
git commit -m "docs(data): add futures open interest example"
```

Expected: commit succeeds.

---

### Task 5: Final verification, code review, and cleanup

**Files:**

- No new files expected.
- Review all files changed in Tasks 1-4.

- [ ] **Step 1: Run focused verification**

Run:

```bash
rtk cargo fmt --all -- --check
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest rtk cargo test -p barter-data open_interest -- --nocapture
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest rtk cargo check -p barter-data --examples
rtk git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 2: Run broader barter-data verification**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest-full rtk cargo test -p barter-data -- --nocapture
```

Expected: all `barter-data` tests pass.

- [ ] **Step 3: Check changed files against the spec**

Run:

```bash
rtk git diff --stat HEAD~4..HEAD
rtk git diff HEAD~4..HEAD -- barter-data/src/subscription/open_interest.rs barter-data/src/subscription/mod.rs barter-data/src/event.rs barter-data/src/streams/builder/dynamic/mod.rs barter-data/src/exchange/binance/futures/open_interest.rs barter-data/src/exchange/binance/futures/mod.rs barter-data/examples/binance_futures_open_interest.rs
```

Expected: diff includes only Open Interest model, REST fetcher, explicit DynamicStreams rejection test, example, and no WebSocket selector or DynamicStreams storage.

- [ ] **Step 4: Request code review**

Use the `/requesting-code-review` skill. Ask the reviewer to focus on:

```text
Review Binance USD-M Futures Open Interest REST-first implementation. Check model semantics, support matrix scope, DynamicStreams rejection, REST error_for_status behavior, test coverage, and whether any code incorrectly implies WebSocket or polling support.
```

Expected: no Critical or Important findings remain unaddressed.

- [ ] **Step 5: Fix any Critical or Important findings with TDD**

For every Critical or Important finding:

1. Add or adjust a failing test that captures the issue.
2. Run the focused test and confirm it fails for the expected reason.
3. Implement the smallest fix.
4. Run focused verification.
5. Commit with a message beginning `fix(data):` or `test(data):`.

If review returns only Minor findings, decide whether to fix immediately or document them as follow-up based on risk.

- [ ] **Step 6: Confirm final clean state**

Run:

```bash
rtk git status --short
```

Expected: no output.

If temporary target directories should be cleaned to save disk, run:

```bash
rm -rf /tmp/barter-rs-target-open-interest /tmp/barter-rs-target-open-interest-full
```

Expected: command exits 0.
