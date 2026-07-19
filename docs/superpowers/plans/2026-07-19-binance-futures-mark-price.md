# Binance USD-M Futures Mark Price Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Binance USD-M Futures mark price REST snapshot and WebSocket stream support to `barter-data`.

**Architecture:** Add one normalized `subscription::mark_price` model using `Decimal`, wire it into `DataKind`, then implement Binance provider REST and WebSocket raw schemas under `exchange::binance::futures::mark_price`. REST fetching remains explicit, while WebSocket mark price uses existing `StreamSelector`, support matrix, and `DynamicStreams` paths.

**Tech Stack:** Rust 2024, `barter-data`, `serde`, `chrono`, `rust_decimal`, `reqwest`, existing `rtk cargo test/check` workflow.

---

## File Structure

### Files to modify

- `barter-data/src/subscription/mod.rs`
  - Export `mark_price` module.
  - Add `SubKind::MarkPrices`.
  - Update Binance USD-M perpetual support matrix.
  - Add support matrix tests.

- `barter-data/src/event.rs`
  - Import `MarkPrice`.
  - Add `DataKind::MarkPrice`.
  - Add `as_mark_price`, `kind_name`, and conversion impls.
  - Add event conversion test.

- `barter-data/src/exchange/binance/channel.rs`
  - Add `BinanceChannel::MARK_PRICE_1S`.
  - Add `Identifier<BinanceChannel>` for `Subscription<BinanceFuturesUsd, Instrument, MarkPrices>`.

- `barter-data/src/exchange/binance/futures/mod.rs`
  - Export `mark_price` module.
  - Add `StreamSelector<Instrument, MarkPrices>`.

- `barter-data/src/streams/builder/dynamic/mod.rs`
  - Import `MarkPrice` and `MarkPrices`.
  - Add `mark_prices` channel storage.
  - Add Binance USD-M `SubKind::MarkPrices` init branch.
  - Add select methods and include mark price in `select_all`.

### Files to create

- `barter-data/src/subscription/mark_price.rs`
  - Normalized marker and model.

- `barter-data/src/exchange/binance/futures/mark_price.rs`
  - REST URL builder and fetcher.
  - Raw REST response model.
  - Raw WebSocket payload model.
  - Conversion tests.

- `barter-data/examples/binance_futures_mark_price.rs`
  - Manual REST example.

- `barter-data/examples/binance_futures_mark_price_stream.rs`
  - Manual WebSocket stream example.

---

## Task 1: Add Normalized Mark Price Model

**Files:**
- Create: `barter-data/src/subscription/mark_price.rs`
- Modify: `barter-data/src/subscription/mod.rs`

- [ ] **Step 1: Write failing normalized model tests**

Create `barter-data/src/subscription/mark_price.rs` with:

```rust
use super::SubscriptionKind;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Barter [`SubscriptionKind`] marker that yields [`MarkPrice`] events.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub struct MarkPrices;

impl SubscriptionKind for MarkPrices {
    type Event = MarkPrice;

    fn as_str(&self) -> &'static str {
        "mark_prices"
    }
}

impl std::fmt::Display for MarkPrices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter mark price model.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct MarkPrice {
    pub event_time: DateTime<Utc>,
    pub mark_price: Decimal,
    pub index_price: Decimal,
    pub estimated_settle_price: Option<Decimal>,
    pub last_funding_rate: Option<Decimal>,
    pub interest_rate: Option<Decimal>,
    pub next_funding_time: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn mark_prices_kind_formats_as_expected() {
        assert_eq!(MarkPrices.as_str(), "mark_prices");
        assert_eq!(MarkPrices.to_string(), "mark_prices");
    }

    #[test]
    fn mark_price_model_uses_decimal_fields() {
        let time = Utc::now();
        let actual = MarkPrice {
            event_time: time,
            mark_price: dec!(11793.63104562),
            index_price: dec!(11791.23456789),
            estimated_settle_price: Some(dec!(11790.11111111)),
            last_funding_rate: Some(dec!(0.00010000)),
            interest_rate: Some(dec!(0.00010000)),
            next_funding_time: Some(time),
        };

        assert_eq!(actual.event_time, time);
        assert_eq!(actual.mark_price, dec!(11793.63104562));
        assert_eq!(actual.index_price, dec!(11791.23456789));
        assert_eq!(actual.estimated_settle_price, Some(dec!(11790.11111111)));
        assert_eq!(actual.last_funding_rate, Some(dec!(0.00010000)));
        assert_eq!(actual.interest_rate, Some(dec!(0.00010000)));
        assert_eq!(actual.next_funding_time, Some(time));
    }
}
```

- [ ] **Step 2: Export module and add SubKind**

In `barter-data/src/subscription/mod.rs`:

```rust
/// Mark price [`SubscriptionKind`] and the associated Barter output data model.
pub mod mark_price;
```

Add enum variant:

```rust
MarkPrices,
```

- [ ] **Step 3: Run focused tests**

```bash
rtk cargo test -p barter-data mark_prices_kind_formats_as_expected mark_price_model_uses_decimal_fields -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add barter-data/src/subscription/mod.rs barter-data/src/subscription/mark_price.rs
git commit -m "feat(data): add mark price model"
```

---

## Task 2: Add MarkPrice to DataKind

**Files:**
- Modify: `barter-data/src/event.rs`

- [ ] **Step 1: Add failing event conversion test**

Add inside existing `#[cfg(test)] mod tests`:

```rust
#[test]
fn mark_price_event_converts_to_data_kind() {
    let time = Utc::now();
    let event = MarketEvent {
        time_exchange: time,
        time_received: time,
        exchange: ExchangeId::BinanceFuturesUsd,
        instrument: "btc-usdt-perp",
        kind: MarkPrice {
            event_time: time,
            mark_price: dec!(11793.63104562),
            index_price: dec!(11791.23456789),
            estimated_settle_price: Some(dec!(11790.11111111)),
            last_funding_rate: Some(dec!(0.00010000)),
            interest_rate: Some(dec!(0.00010000)),
            next_funding_time: Some(time),
        },
    };

    let data_kind_event = MarketEvent::<_, DataKind>::from(event);

    assert_eq!(data_kind_event.kind.kind_name(), "mark_price");
    let mark_price_event = data_kind_event.as_mark_price().unwrap();
    assert_eq!(mark_price_event.kind.mark_price, dec!(11793.63104562));
    assert_eq!(mark_price_event.kind.index_price, dec!(11791.23456789));
}
```

- [ ] **Step 2: Implement DataKind wiring**

Update imports to include `mark_price::MarkPrice`. Add `as_mark_price`, `DataKind::MarkPrice(MarkPrice)`, `kind_name` arm, and conversions mirroring funding rate.

- [ ] **Step 3: Run focused event test**

```bash
rtk cargo test -p barter-data mark_price_event_converts_to_data_kind -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add barter-data/src/event.rs
git commit -m "feat(data): add mark price event kind"
```

---

## Task 3: Add Binance Mark Price Parser, REST URL, Fetcher, and Tests

**Files:**
- Create: `barter-data/src/exchange/binance/futures/mark_price.rs`
- Modify: `barter-data/src/exchange/binance/futures/mod.rs`

- [ ] **Step 1: Write parser and conversion tests first**

Tests must cover:

- `mark_price_url("BTCUSDT") == "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"`.
- REST fixture parses Decimal values.
- REST fixture converts into `MarketEvent<_, MarkPrice>`.
- WebSocket fixture maps subscription id to `@markPrice@1s|BTCUSDT`.
- WebSocket fixture converts into `MarketEvent<_, MarkPrice>`.

Use fixture values from Binance docs style:

```json
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
```

WebSocket fixture:

```json
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
```

- [ ] **Step 2: Verify red**

```bash
rtk cargo test -p barter-data mark_price -- --nocapture
```

Expected: FAIL before implementation is complete.

- [ ] **Step 3: Implement provider module**

Implement:

- `HTTP_MARK_PRICE_URL_BINANCE_FUTURES_USD`.
- `mark_price_url(symbol: &str) -> String`.
- `BinanceFuturesUsdMarkPriceFetcher::fetch_latest` and `fetch` using `reqwest::get`.
- `BinanceFuturesMarkPriceRest` raw struct.
- `BinanceFuturesMarkPriceWs` raw struct.
- optional decimal deserializer helper.
- `Identifier<Option<SubscriptionId>>` for WebSocket raw struct.
- `From<(ExchangeId, InstrumentKey, BinanceFuturesMarkPriceRest)> for MarketIter<InstrumentKey, MarkPrice>`.
- `From<(ExchangeId, InstrumentKey, BinanceFuturesMarkPriceWs)> for MarketIter<InstrumentKey, MarkPrice>`.

- [ ] **Step 4: Export module**

In `barter-data/src/exchange/binance/futures/mod.rs` add:

```rust
/// Mark price REST and WebSocket types.
pub mod mark_price;
```

- [ ] **Step 5: Run focused tests**

```bash
rtk cargo test -p barter-data mark_price -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add barter-data/src/exchange/binance/futures/mod.rs barter-data/src/exchange/binance/futures/mark_price.rs
git commit -m "feat(binance): parse futures mark prices"
```

---

## Task 4: Add Binance Mark Price WebSocket Channel and StreamSelector

**Files:**
- Modify: `barter-data/src/exchange/binance/channel.rs`
- Modify: `barter-data/src/exchange/binance/futures/mod.rs`

- [ ] **Step 1: Write channel/selector tests**

Add tests proving a Binance Futures mark price subscription produces channel `@markPrice@1s` and market id `btcusdt` behavior stays consistent with existing `BinanceMarket` identifiers.

- [ ] **Step 2: Implement channel constant and identifier**

Add:

```rust
pub const MARK_PRICE_1S: Self = Self("@markPrice@1s");
```

Add `Identifier<BinanceChannel>` impl for `Subscription<BinanceFuturesUsd, Instrument, MarkPrices>`.

- [ ] **Step 3: Implement StreamSelector**

In futures module, import `mark_price::BinanceFuturesMarkPriceWs` and `subscription::mark_price::MarkPrices`, then add:

```rust
impl<Instrument> StreamSelector<Instrument, MarkPrices> for BinanceFuturesUsd
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = BinanceWsStream<StatelessTransformer<Self, Instrument::Key, MarkPrices, BinanceFuturesMarkPriceWs>>;
}
```

- [ ] **Step 4: Run focused tests**

```bash
rtk cargo test -p barter-data mark_price -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add barter-data/src/exchange/binance/channel.rs barter-data/src/exchange/binance/futures/mod.rs
git commit -m "feat(binance): add futures mark price stream selector"
```

---

## Task 5: Add Support Matrix and DynamicStreams Mark Price Wiring

**Files:**
- Modify: `barter-data/src/subscription/mod.rs`
- Modify: `barter-data/src/streams/builder/dynamic/mod.rs`

- [ ] **Step 1: Add support matrix test**

Add a test asserting Binance USD-M perpetual `SubKind::MarkPrices` is supported and Binance spot mark prices are not supported.

- [ ] **Step 2: Add DynamicStreams compile coverage test**

Add or extend compile-only coverage to construct a Binance USD-M perpetual mark price `Subscription<ExchangeId, MarketDataInstrument, SubKind>` and call `DynamicStreams::<_>::init` in a non-running compile path if an existing pattern exists. If no runtime-free pattern exists, rely on `cargo check -p barter-data --examples` plus generic bounds.

- [ ] **Step 3: Implement support matrix**

Add `MarkPrices` to the Binance USD-M perpetual sub kind match arm.

- [ ] **Step 4: Implement DynamicStreams wiring**

Add `mark_prices` maps to `DynamicStreams`, `Channels`, `Txs`, and `Rxs`. Add Binance USD-M init branch mapping `SubKind::MarkPrices` to typed `MarkPrices`. Add `select_mark_prices`, `select_all_mark_prices`, and include mark prices in `select_all` bounds and chain order after candles and before liquidations.

- [ ] **Step 5: Run checks**

```bash
rtk cargo check -p barter-data -j1
rtk cargo test -p barter-data mark_price -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add barter-data/src/subscription/mod.rs barter-data/src/streams/builder/dynamic/mod.rs
git commit -m "feat(data): wire mark prices into dynamic streams"
```

---

## Task 6: Add REST and WebSocket Examples

**Files:**
- Create: `barter-data/examples/binance_futures_mark_price.rs`
- Create: `barter-data/examples/binance_futures_mark_price_stream.rs`

- [ ] **Step 1: Add REST example**

Create a manual example that initializes logging, builds a BTCUSDT Binance Futures perpetual keyed instrument subscription, calls `BinanceFuturesUsdMarkPriceFetcher::fetch_latest`, and logs the normalized events.

- [ ] **Step 2: Add WebSocket stream example**

Create a manual example that initializes logging, subscribes to BTCUSDT mark price through `Streams::<Public>` or `DynamicStreams`, and logs several events.

- [ ] **Step 3: Compile examples**

```bash
rtk cargo check -p barter-data --examples -j1
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add barter-data/examples/binance_futures_mark_price.rs barter-data/examples/binance_futures_mark_price_stream.rs
git commit -m "docs(data): add futures mark price examples"
```

---

## Task 7: Full Verification

- [ ] **Step 1: Format**

```bash
rtk cargo fmt --all -- --check
```

Expected: PASS.

- [ ] **Step 2: Run full barter-data tests**

```bash
rtk cargo test -p barter-data -j1
```

Expected: PASS.

- [ ] **Step 3: Check barter-data and examples**

```bash
rtk cargo check -p barter-data -j1
rtk cargo check -p barter-data --examples -j1
```

Expected: PASS.

- [ ] **Step 4: Commit plan completion note if needed**

If all verification passes, update this plan's checkbox state or add completion note, then commit:

```bash
git add docs/superpowers/plans/2026-07-19-binance-futures-mark-price.md
git commit -m "docs(data): plan binance futures mark prices"
```
