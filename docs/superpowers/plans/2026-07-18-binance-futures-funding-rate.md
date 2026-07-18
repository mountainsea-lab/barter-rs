# Binance USD-M Futures Funding Rate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Binance USD-M Futures funding rate REST collection to `barter-data` with normalized Decimal-based models, tested raw parsing/conversion, deterministic URL construction, and a manually runnable example.

**Architecture:** Funding rates are REST data, not WebSocket streams. Implement a focused REST fetch-first path under `exchange::binance::futures::funding`, keep provider raw schema local to Binance Futures, keep normalized output in `subscription::funding`, and do not wire `DynamicStreams` in this increment. Add `DataKind::FundingRate` for generic event ergonomics, but do not add `SubKind::FundingRates` or WebSocket support unless a later REST polling abstraction requires it.

**Tech Stack:** Rust 2024, `barter-data`, `reqwest`, `serde`, `chrono`, `rust_decimal`, existing `MarketEvent`/`MarketIter` patterns, `rtk cargo` verification workflow.

---

## Scope and Architecture Constraints

This plan implements only Binance USD-M Futures funding rate REST fetch support.

Hard constraints:

- Keep REST fetchers separate from `StreamSelector` and `DynamicStreams`.
- Do not add WebSocket subscription channels for funding rates.
- Do not depend on live Binance HTTP calls in automated tests.
- Use `Decimal` for new financial numeric fields.
- Commit only after green verification checkpoints.

## File Structure

### Files to modify

- `barter-data/src/subscription/mod.rs`
  - Export `funding` module.

- `barter-data/src/event.rs`
  - Import `FundingRate`.
  - Add `DataKind::FundingRate`.
  - Add `as_funding_rate` helper.
  - Add conversions from `MarketEvent<FundingRate>` and `MarketStreamResult<FundingRate>` to `DataKind`.

- `barter-data/src/exchange/binance/futures/mod.rs`
  - Export `funding` module.

### Files to create

- `barter-data/src/subscription/funding.rs`
  - Normalized funding rate subscription kind marker and output model.

- `barter-data/src/exchange/binance/futures/funding.rs`
  - Binance raw funding rate REST response model.
  - Funding request parameter type.
  - URL builder.
  - REST fetcher.
  - Raw-to-normalized conversion.
  - Fixture tests.

- `barter-data/examples/binance_futures_funding.rs`
  - Manual example fetching recent BTCUSDT funding rate data.

---

## Task 1: Add Normalized Funding Rate Model

**Files:**
- Create: `barter-data/src/subscription/funding.rs`
- Modify: `barter-data/src/subscription/mod.rs`

- [ ] **Step 1: Write the normalized funding module**

Create `barter-data/src/subscription/funding.rs` with:

```rust
use super::SubscriptionKind;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Barter [`SubscriptionKind`] marker that yields [`FundingRate`] events.
///
/// Funding rates are REST-first data in the current architecture. This marker
/// identifies the normalized output type, but does not imply WebSocket stream
/// support or `DynamicStreams` wiring.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub struct FundingRates;

impl SubscriptionKind for FundingRates {
    type Event = FundingRate;

    fn as_str(&self) -> &'static str {
        "funding_rates"
    }
}

impl std::fmt::Display for FundingRates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter funding rate model.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct FundingRate {
    pub funding_time: DateTime<Utc>,
    pub funding_rate: Decimal,
    pub mark_price: Option<Decimal>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn funding_rates_kind_formats_as_expected() {
        assert_eq!(FundingRates.as_str(), "funding_rates");
        assert_eq!(FundingRates.to_string(), "funding_rates");
    }
}
```

- [ ] **Step 2: Export the funding module**

In `barter-data/src/subscription/mod.rs`, add the module export after candles or liquidations:

```rust
/// Funding rate [`SubscriptionKind`] and the associated Barter output data model.
pub mod funding;
```

- [ ] **Step 3: Run the focused test**

Run:

```bash
rtk cargo test -p barter-data funding_rates_kind_formats_as_expected -- --nocapture
```

Expected: PASS with the new formatting test.

- [ ] **Step 4: Commit normalized funding model**

Run:

```bash
git add barter-data/src/subscription/mod.rs barter-data/src/subscription/funding.rs
git commit -m "feat(data): add funding rate model"
```

---

## Task 2: Add FundingRate to DataKind

**Files:**
- Modify: `barter-data/src/event.rs`

- [ ] **Step 1: Update imports**

In `barter-data/src/event.rs`, update the `subscription` imports to include funding:

```rust
subscription::{
    book::{OrderBookEvent, OrderBookL1},
    candle::Candle,
    funding::FundingRate,
    liquidation::Liquidation,
    trade::PublicTrade,
},
```

- [ ] **Step 2: Add accessor and enum variant**

Add this method next to `as_candle` and `as_liquidation`:

```rust
pub fn as_funding_rate(&self) -> Option<MarketEvent<&InstrumentKey, &FundingRate>> {
    match &self.kind {
        DataKind::FundingRate(funding_rate) => Some(self.as_event(funding_rate)),
        _ => None,
    }
}
```

Add the enum variant:

```rust
FundingRate(FundingRate),
```

Update `kind_name`:

```rust
DataKind::FundingRate(_) => "funding_rate",
```

- [ ] **Step 3: Add conversions**

Add these impls near the existing Candle and Liquidation conversions:

```rust
impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, FundingRate>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, FundingRate>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, FundingRate>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, FundingRate>) -> Self {
        value.map_kind(FundingRate::into)
    }
}
```

- [ ] **Step 4: Add event tests**

Add this test module at the end of `barter-data/src/event.rs` if no local tests exist. If a test module already exists, add only the test function inside it.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn funding_rate_event_converts_to_data_kind() {
        let time = Utc::now();
        let event = MarketEvent {
            time_exchange: time,
            time_received: time,
            exchange: ExchangeId::BinanceFuturesUsd,
            instrument: "btc-usdt-perp",
            kind: FundingRate {
                funding_time: time,
                funding_rate: dec!(0.00010000),
                mark_price: Some(dec!(65000.25)),
            },
        };

        let data_kind_event = MarketEvent::<_, DataKind>::from(event);

        assert_eq!(data_kind_event.kind.kind_name(), "funding_rate");
        let funding_rate_event = data_kind_event.as_funding_rate().unwrap();
        assert_eq!(funding_rate_event.kind.funding_rate, dec!(0.00010000));
        assert_eq!(funding_rate_event.kind.mark_price, Some(dec!(65000.25)));
    }
}
```

- [ ] **Step 5: Run focused event test**

Run:

```bash
rtk cargo test -p barter-data funding_rate_event_converts_to_data_kind -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit DataKind wiring**

Run:

```bash
git add barter-data/src/event.rs
git commit -m "feat(data): add funding rate event kind"
```

---

## Task 3: Add Binance Funding Raw Parser and Conversion

**Files:**
- Create: `barter-data/src/exchange/binance/futures/funding.rs`
- Modify: `barter-data/src/exchange/binance/futures/mod.rs`

- [ ] **Step 1: Export the funding module**

In `barter-data/src/exchange/binance/futures/mod.rs`, add:

```rust
/// Funding rate REST types and fetcher.
pub mod funding;
```

- [ ] **Step 2: Create raw parser and conversion**

Create `barter-data/src/exchange/binance/futures/funding.rs` with:

```rust
use crate::{
    event::{MarketEvent, MarketIter},
    exchange::binance::{futures::BinanceFuturesUsd, market::BinanceMarket},
    subscription::funding::FundingRate,
    Identifier,
};
use barter_instrument::exchange::ExchangeId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::future::Future;

/// [`BinanceFuturesUsd`] HTTP funding rate url.
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
        (exchange_id, instrument, input): (
            ExchangeId,
            InstrumentKey,
            BinanceFuturesFundingRate,
        ),
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
```

- [ ] **Step 3: Add parser and conversion tests**

Append to `barter-data/src/exchange/binance/futures/funding.rs`:

```rust
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
```

- [ ] **Step 4: Run focused parser tests**

Run:

```bash
rtk cargo test -p barter-data binance_futures_funding_rate -- --nocapture
```

Expected: PASS for deserialization and conversion tests.

- [ ] **Step 5: Commit raw parser and conversion**

Run:

```bash
git add barter-data/src/exchange/binance/futures/mod.rs barter-data/src/exchange/binance/futures/funding.rs
git commit -m "feat(binance): parse futures funding rates"
```

---

## Task 4: Add Funding URL Builder and REST Fetcher

**Files:**
- Modify: `barter-data/src/exchange/binance/futures/funding.rs`

- [ ] **Step 1: Add request parameters and URL builder**

Add below the URL constant in `funding.rs`:

```rust
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
```

- [ ] **Step 2: Add fetcher type and methods**

Add below `funding_rate_url`:

```rust
#[derive(Debug)]
pub struct BinanceFuturesUsdFundingRateFetcher;

impl BinanceFuturesUsdFundingRateFetcher {
    pub fn fetch_latest<Instrument>(
        subscriptions: &[crate::subscription::Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::funding::FundingRates,
        >],
    ) -> impl Future<Output = Result<Vec<MarketEvent<Instrument::Key, FundingRate>>, barter_integration::error::SocketError>>
    + Send
    where
        Instrument: crate::instrument::InstrumentData,
        Instrument::Key: Clone,
        crate::subscription::Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::funding::FundingRates,
        >: Identifier<BinanceMarket>,
    {
        Self::fetch(subscriptions, BinanceFuturesFundingRateRequest::latest())
    }

    pub fn fetch<Instrument>(
        subscriptions: &[crate::subscription::Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::funding::FundingRates,
        >],
        request: BinanceFuturesFundingRateRequest,
    ) -> impl Future<Output = Result<Vec<MarketEvent<Instrument::Key, FundingRate>>, barter_integration::error::SocketError>>
    + Send
    where
        Instrument: crate::instrument::InstrumentData,
        Instrument::Key: Clone,
        crate::subscription::Subscription<
            BinanceFuturesUsd,
            Instrument,
            crate::subscription::funding::FundingRates,
        >: Identifier<BinanceMarket>,
    {
        use futures_util::future::try_join_all;

        let funding_futures = subscriptions.iter().map(|sub| {
            let symbol = sub.id();
            let funding_url = funding_rate_url(symbol.as_ref(), request);

            async move {
                let rows = reqwest::get(funding_url)
                    .await
                    .map_err(barter_integration::error::SocketError::Http)?
                    .json::<Vec<BinanceFuturesFundingRate>>()
                    .await
                    .map_err(barter_integration::error::SocketError::Http)?;

                Ok(rows
                    .into_iter()
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
                    .collect::<Vec<_>>())
            }
        });

        async move {
            let nested = try_join_all(funding_futures).await?;
            Ok(nested.into_iter().flatten().collect())
        }
    }
}
```

The `Instrument::Key: Clone` bound is required because each returned funding row owns a cloned instrument key.

- [ ] **Step 3: Add URL construction tests**

Add these tests inside the existing funding test module:

```rust
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
```

- [ ] **Step 4: Run focused URL tests**

Run:

```bash
rtk cargo test -p barter-data funding_rate_url -- --nocapture
rtk cargo test -p barter-data latest_request_uses_limit_one -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run compile check for fetcher generics**

Run:

```bash
rtk cargo check -p barter-data -j1
```

Expected: PASS. If it fails on generic clone bounds, add `Instrument::Key: Clone` to both fetcher methods as described in Step 2 and rerun.

- [ ] **Step 6: Commit fetcher**

Run:

```bash
git add barter-data/src/exchange/binance/futures/funding.rs
git commit -m "feat(binance): fetch futures funding rates"
```

---

## Task 5: Add Manual Funding Example

**Files:**
- Create: `barter-data/examples/binance_futures_funding.rs`

- [ ] **Step 1: Create example**

Create `barter-data/examples/binance_futures_funding.rs` with:

```rust
use barter_data::{
    exchange::binance::futures::{
        BinanceFuturesUsd,
        funding::{BinanceFuturesFundingRateRequest, BinanceFuturesUsdFundingRateFetcher},
    },
    subscription::{Subscription, funding::FundingRates},
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let subscriptions = vec![Subscription::from((
        BinanceFuturesUsd::default(),
        "btc",
        "usdt",
        MarketDataInstrumentKind::Perpetual,
        FundingRates,
    ))];

    let events = BinanceFuturesUsdFundingRateFetcher::fetch(
        &subscriptions,
        BinanceFuturesFundingRateRequest {
            start_time: None,
            end_time: None,
            limit: Some(3),
        },
    )
    .await
    .expect("fetch Binance USD-M Futures funding rates");

    for event in events {
        tracing::info!(?event, "received Binance USD-M Futures funding rate");
    }
}
```

- [ ] **Step 2: Compile the example**

Run:

```bash
rtk cargo check -p barter-data --example binance_futures_funding -j1
```

Expected: PASS.

- [ ] **Step 3: Optional manual live run**

Run only after compile and unit tests pass:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 rtk cargo run -p barter-data --example binance_futures_funding
```

Expected: logs at least one `received Binance USD-M Futures funding rate` event if Binance REST is reachable. If the live run fails due to network, do not block automated completion. Record the error in the final report.

- [ ] **Step 4: Commit example**

Run:

```bash
git add barter-data/examples/binance_futures_funding.rs
git commit -m "docs(data): add binance futures funding example"
```

---

## Task 6: Full Verification and Final Green Commit Check

**Files:**
- No new files expected. This task verifies the full branch state.

- [ ] **Step 1: Run formatting check**

Run:

```bash
rtk cargo fmt --check
```

Expected: PASS.

- [ ] **Step 2: Run `barter-data` tests**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 rtk cargo test -p barter-data -j1
```

Expected: PASS.

- [ ] **Step 3: Run workspace check**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 rtk cargo check --workspace -j1
```

Expected: PASS.

- [ ] **Step 4: Run example check**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 rtk cargo check -p barter-data --examples -j1
```

Expected: PASS.

- [ ] **Step 5: Check git status**

Run:

```bash
rtk git status --short --branch
```

Expected: clean worktree. Branch may be ahead of origin by the new green commits.

- [ ] **Step 6: Report completion**

Final report must include:

- Files changed.
- Commit hashes created.
- Automated verification commands and pass/fail status.
- Whether optional manual Binance REST example was run.
- Confirmation that `DynamicStreams` was not changed for funding rates.

---

## Spec Coverage Self-Review

Spec requirements covered:

- Normalized FundingRate model: Task 1.
- Decimal parsing: Tasks 1 and 3.
- Binance raw REST payload parser: Task 3.
- Conversion to `MarketEvent`: Tasks 2 and 3.
- URL construction with symbol, limit, startTime, endTime: Task 4.
- REST fetcher API: Task 4.
- Manual example: Task 5.
- No automated live Binance dependency: Tasks 3, 4, 6.
- No `DynamicStreams` wiring: Scope constraints and final report requirement.

Known implementation decision:

- This plan adds `DataKind::FundingRate` for generic event ergonomics but intentionally does not add `SubKind::FundingRates` or `DynamicStreams` support. This keeps REST data separate from WebSocket stream construction while still allowing downstream code to carry funding events in the common `DataKind` enum.
