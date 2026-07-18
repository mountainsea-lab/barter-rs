# Binance USD-M Futures Candles/Bars Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add working Binance USD-M Futures candle/bar streaming support to `barter-data` and make it available through typed streams and `DynamicStreams`.

**Architecture:** Reuse existing `barter-data` abstractions. Add a Binance kline channel, raw Binance kline payload parser, normalized conversion into the existing `Candle` model, a `StreamSelector<Instrument, Candles>` implementation for `BinanceFuturesUsd`, and DynamicStreams wiring so `SubKind::Candles` works like trades, L1, L2, and liquidations.

**Tech Stack:** Rust 2024, `barter-data`, `barter-integration`, `barter-instrument`, `serde`, `chrono`, `tokio`, `futures`, existing `rtk cargo test` workflow.

## Confirmed Design Decisions

1. **Interval scope:** implement realtime `1m` Binance USD-M Futures candles only. Use `@kline_1m` and defer multi-interval subscription design until a later task.
2. **Validation level:** automated completion requires parser/conversion tests, support matrix tests, DynamicStreams compile coverage, full `barter-data` tests, workspace check, and example compile coverage. Live Binance WebSocket smoke testing is useful manually but not a required automated gate.
3. **Commit strategy:** do not commit red-test checkpoints. Keep local TDD workflow if useful, but commit only green, focused checkpoints.
4. **DynamicStreams ordering:** include candles in `select_all` as `trades -> l1s -> l2s -> candles -> liquidations`.
5. **Example acceptance:** examples must follow existing examples style, compile, and be manually runnable so logs can show real Binance candle data. Automated verification should not require receiving live data.

---

## File Structure

### Files to modify

- `barter-data/src/exchange/binance/channel.rs`
  - Add `BinanceChannel::CANDLES_1M` and `Identifier<BinanceChannel>` for `Subscription<BinanceFuturesUsd, Instrument, Candles>`.

- `barter-data/src/exchange/binance/futures/mod.rs`
  - Add `pub mod candle`.
  - Add `StreamSelector<Instrument, Candles>` for `BinanceFuturesUsd`.

- `barter-data/src/subscription/mod.rs`
  - Extend Binance USD-M support matrix to allow `SubKind::Candles` for `Perpetual`.
  - Add a unit test proving the support matrix accepts Binance USD-M perpetual candles and rejects spot candles for Binance USD-M.

- `barter-data/src/streams/builder/dynamic/mod.rs`
  - Import `Candle` and `Candles`.
  - Add `DynamicStreams::candles` channel storage.
  - Add `Subscription<BinanceFuturesUsd, Instrument, Candles>: Identifier<BinanceMarket>` bound.
  - Add Binance USD-M `SubKind::Candles` init branch.
  - Add select methods for candles.
  - Include candles in `select_all`.
  - Add channels in `Channels`, `Txs`, and `Rxs`.

- `barter-data/examples/indexed_market_stream.rs`
  - Add `SubKind::Candles` to the example subscription list after DynamicStreams wiring works.

### Files to create

- `barter-data/src/exchange/binance/futures/candle.rs`
  - Raw Binance futures kline payload models.
  - Subscription id mapping.
  - Normalized conversion into `MarketIter<InstrumentKey, Candle>`.
  - Parser and conversion tests.

- `barter-data/examples/binance_futures_candles.rs`
  - Minimal example for Binance USD-M candle streams.

---

## Task 1: Add Binance Futures Candle Raw Parser

**Files:**
- Create: `barter-data/src/exchange/binance/futures/candle.rs`

- [ ] **Step 1: Write the raw parser and conversion tests first**

Create `barter-data/src/exchange/binance/futures/candle.rs` with this complete file:

```rust
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
    fn from((exchange_id, instrument, input): (ExchangeId, InstrumentKey, BinanceFuturesKline)) -> Self {
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
        assert_eq!(actual.kline.subscription_id, SubscriptionId::from("@kline_1m|BTCUSDT"));
        assert_eq!(actual.kline.interval, "1m");
        assert_eq!(actual.kline.close_time, datetime_utc_from_epoch_duration(Duration::from_millis(1749354839999)));
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
```

- [ ] **Step 2: Run parser tests and verify the expected module import failure**

Run:

```bash
rtk cargo test -p barter-data binance_futures_kline -- --nocapture
```

Expected result: compile failure because `BinanceChannel::CANDLES_1M` and the module export do not exist yet.

- [ ] **Step 3: Commit the failing test file if working with TDD checkpoint commits is allowed**

If committing red tests is not desired in this repository, skip this commit and continue to Task 2. If committing red tests is allowed, run:

```bash
rtk git add barter-data/src/exchange/binance/futures/candle.rs
rtk git commit -m "test(data): add binance futures candle parser coverage"
```

---

## Task 2: Add Binance Candle Channel and Module Wiring

**Files:**
- Modify: `barter-data/src/exchange/binance/channel.rs`
- Modify: `barter-data/src/exchange/binance/futures/mod.rs`

- [ ] **Step 1: Update `channel.rs` imports**

Change the subscription import block in `barter-data/src/exchange/binance/channel.rs` to include `candle::Candles`:

```rust
    subscription::{
        Subscription,
        book::{OrderBooksL1, OrderBooksL2},
        candle::Candles,
        liquidation::Liquidations,
        trade::PublicTrades,
    },
```

- [ ] **Step 2: Add Binance candle channel constant**

Add this constant inside `impl BinanceChannel`, after `ORDER_BOOK_L2` and before `LIQUIDATIONS`:

```rust
    /// [`BinanceFuturesUsd`] one minute kline/candlestick channel name.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-streams>
    pub const CANDLES_1M: Self = Self("@kline_1m");
```

- [ ] **Step 3: Add channel identifier for Binance USD-M candles**

Add this impl after the `OrderBooksL2` identifier impl and before the `Liquidations` impl:

```rust
impl<Instrument> Identifier<BinanceChannel> for Subscription<BinanceFuturesUsd, Instrument, Candles> {
    fn id(&self) -> BinanceChannel {
        BinanceChannel::CANDLES_1M
    }
}
```

- [ ] **Step 4: Update `futures/mod.rs` imports and module exports**

Modify the top of `barter-data/src/exchange/binance/futures/mod.rs` so the imports include the raw candle model and `Candles`:

```rust
use self::{candle::BinanceFuturesKline, liquidation::BinanceLiquidation};
use super::{Binance, ExchangeServer};
use crate::{
    NoInitialSnapshots,
    exchange::{
        StreamSelector,
        binance::{
            BinanceWsStream,
            futures::l2::{
                BinanceFuturesUsdOrderBooksL2SnapshotFetcher,
                BinanceFuturesUsdOrderBooksL2Transformer,
            },
        },
    },
    instrument::InstrumentData,
    subscription::{book::OrderBooksL2, candle::Candles, liquidation::Liquidations},
    transformer::stateless::StatelessTransformer,
};
```

Add this module export before `pub mod l2;`:

```rust
/// Kline/candlestick types.
pub mod candle;
```

- [ ] **Step 5: Add `StreamSelector` for Binance USD-M candles**

Add this impl after the `OrderBooksL2` `StreamSelector` impl and before the `Liquidations` impl:

```rust
impl<Instrument> StreamSelector<Instrument, Candles> for BinanceFuturesUsd
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = BinanceWsStream<StatelessTransformer<Self, Instrument::Key, Candles, BinanceFuturesKline>>;
}
```

- [ ] **Step 6: Run parser tests and verify they pass**

Run:

```bash
rtk cargo test -p barter-data binance_futures_kline -- --nocapture
```

Expected result: both tests in `exchange::binance::futures::candle::tests` pass.

- [ ] **Step 7: Commit parser and channel wiring**

Run:

```bash
rtk git add barter-data/src/exchange/binance/channel.rs barter-data/src/exchange/binance/futures/mod.rs barter-data/src/exchange/binance/futures/candle.rs
rtk git commit -m "feat(data): add binance futures candle stream parser"
```

---

## Task 3: Add Candle Support Matrix Coverage

**Files:**
- Modify: `barter-data/src/subscription/mod.rs`

- [ ] **Step 1: Write support matrix tests**

Append this test module inside the existing `#[cfg(test)] mod tests` in `barter-data/src/subscription/mod.rs`:

```rust
    mod support_matrix {
        use super::*;
        use barter_instrument::{
            exchange::ExchangeId,
            instrument::market_data::kind::MarketDataInstrumentKind,
        };

        #[test]
        fn test_binance_futures_usd_supports_perpetual_candles() {
            assert!(exchange_supports_instrument_kind_sub_kind(
                &ExchangeId::BinanceFuturesUsd,
                &MarketDataInstrumentKind::Perpetual,
                SubKind::Candles,
            ));
        }

        #[test]
        fn test_binance_futures_usd_rejects_spot_candles() {
            assert!(!exchange_supports_instrument_kind_sub_kind(
                &ExchangeId::BinanceFuturesUsd,
                &MarketDataInstrumentKind::Spot,
                SubKind::Candles,
            ));
        }
    }
```

- [ ] **Step 2: Run support matrix tests and verify the first test fails**

Run:

```bash
rtk cargo test -p barter-data support_matrix -- --nocapture
```

Expected result: `test_binance_futures_usd_supports_perpetual_candles` fails because `SubKind::Candles` is not yet enabled for Binance USD-M perpetual instruments.

- [ ] **Step 3: Update support matrix implementation**

In `exchange_supports_instrument_kind_sub_kind`, change the Binance USD-M perpetual match arm from:

```rust
        (
            BinanceFuturesUsd,
            Perpetual,
            PublicTrades | OrderBooksL1 | OrderBooksL2 | Liquidations,
        ) => true,
```

to:

```rust
        (
            BinanceFuturesUsd,
            Perpetual,
            PublicTrades | OrderBooksL1 | OrderBooksL2 | Liquidations | Candles,
        ) => true,
```

- [ ] **Step 4: Run support matrix tests and verify they pass**

Run:

```bash
rtk cargo test -p barter-data support_matrix -- --nocapture
```

Expected result: both support matrix tests pass.

- [ ] **Step 5: Commit support matrix change**

Run:

```bash
rtk git add barter-data/src/subscription/mod.rs
rtk git commit -m "feat(data): enable binance futures candle subscriptions"
```

---

## Task 4: Wire Candles into DynamicStreams

**Files:**
- Modify: `barter-data/src/streams/builder/dynamic/mod.rs`

- [ ] **Step 1: Add imports for `Candle` and `Candles`**

In `barter-data/src/streams/builder/dynamic/mod.rs`, change the `subscription` imports to include candles:

```rust
    subscription::{
        SubKind, Subscription,
        book::{OrderBookEvent, OrderBookL1, OrderBooksL1, OrderBooksL2},
        candle::{Candle, Candles},
        liquidation::{Liquidation, Liquidations},
        trade::{PublicTrade, PublicTrades},
    },
```

- [ ] **Step 2: Add `candles` field to `DynamicStreams`**

Change the struct to include this field after `l2s` and before `liquidations`:

```rust
    pub candles:
        VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Candle>>>,
```

- [ ] **Step 3: Add the Binance USD-M candle identifier bound**

In `DynamicStreams::init` where other Binance USD-M bounds are listed, add:

```rust
        Subscription<BinanceFuturesUsd, Instrument, Candles>: Identifier<BinanceMarket>,
```

- [ ] **Step 4: Add the Binance USD-M candles init branch**

Add this match arm after the Binance USD-M `OrderBooksL2` branch and before `Liquidations`:

```rust
                                    (ExchangeId::BinanceFuturesUsd, SubKind::Candles) => {
                                        init_market_stream(
                                            STREAM_RECONNECTION_POLICY,
                                            subs.into_iter()
                                                .map(|sub| {
                                                    Subscription::<_, Instrument, _>::new(
                                                        BinanceFuturesUsd::default(),
                                                        sub.instrument,
                                                        Candles,
                                                    )
                                                })
                                                .collect(),
                                        )
                                        .await
                                        .map(|stream| {
                                            tokio::spawn(stream.forward_to(
                                                txs.candles.get(&exchange).unwrap().clone(),
                                            ))
                                        })
                                    }
```

- [ ] **Step 5: Add candles to the `Ok(Self { ... })` construction**

Add this field after `l2s` and before `liquidations`:

```rust
            candles: channels
                .rxs
                .candles
                .into_iter()
                .map(|(exchange, rx)| (exchange, rx.into_stream()))
                .collect(),
```

- [ ] **Step 6: Add candle select methods**

Add these methods in `impl<InstrumentKey> DynamicStreams<InstrumentKey>` after the L2 select methods and before liquidation select methods:

```rust
    /// Remove an exchange [`Candle`] `Stream` from the [`DynamicStreams`] collection.
    pub fn select_candles(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Candle>>> {
        self.candles.remove(&exchange)
    }

    /// Select and merge every exchange [`Candle`] `Stream`.
    pub fn select_all_candles(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Candle>>> {
        futures_util::stream::select_all::select_all(
            std::mem::take(&mut self.candles).into_values(),
        )
    }
```

- [ ] **Step 7: Include candles in `select_all`**

Change the `select_all` bounds to include:

```rust
        MarketStreamResult<InstrumentKey, Candle>: Into<Output>,
```

Change the destructuring to include `candles`:

```rust
        let Self {
            trades,
            l1s,
            l2s,
            candles,
            liquidations,
        } = self;
```

Add candle stream mapping after `l2s`:

```rust
        let candles = candles
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());
```

Change the combined stream line to:

```rust
        let all = trades.chain(l1s).chain(l2s).chain(candles).chain(liquidations);
```

- [ ] **Step 8: Add candle channel creation in `Channels::try_from`**

Add this match arm after `SubKind::OrderBooksL2` and before `SubKind::Liquidations`:

```rust
                SubKind::Candles => {
                    if let (None, None) = (
                        txs.candles.get(&sub.exchange),
                        rxs.candles.get(&sub.exchange),
                    ) {
                        let (tx, rx) = mpsc_unbounded();
                        txs.candles.insert(sub.exchange, tx);
                        rxs.candles.insert(sub.exchange, rx);
                    }
                }
```

- [ ] **Step 9: Add candle fields to `Txs`, `Default for Txs`, `Rxs`, and `Default for Rxs`**

In `Txs<InstrumentKey>`, add:

```rust
    candles: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, Candle>>>,
```

In `Default for Txs`, add:

```rust
            candles: Default::default(),
```

In `Rxs<InstrumentKey>`, add:

```rust
    candles: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, Candle>>>,
```

In `Default for Rxs`, add:

```rust
            candles: Default::default(),
```

- [ ] **Step 10: Run focused build tests**

Run:

```bash
rtk cargo test -p barter-data dynamic -- --nocapture
```

Expected result: dynamic stream tests compile and pass. If no dynamic tests match, the crate should still compile successfully for this test filter.

- [ ] **Step 11: Commit DynamicStreams wiring**

Run:

```bash
rtk git add barter-data/src/streams/builder/dynamic/mod.rs
rtk git commit -m "feat(data): wire candles into dynamic streams"
```

---

## Task 5: Add Candle Examples

**Files:**
- Create: `barter-data/examples/binance_futures_candles.rs`
- Modify: `barter-data/examples/indexed_market_stream.rs`

- [ ] **Step 1: Create Binance futures candle example**

Create `barter-data/examples/binance_futures_candles.rs`:

```rust
use barter_data::{
    exchange::binance::futures::BinanceFuturesUsd,
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscription::candle::Candles,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures_util::StreamExt;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let streams = Streams::<Candles>::builder()
        .subscribe([
            (BinanceFuturesUsd::default(), "btc", "usdt", MarketDataInstrumentKind::Perpetual, Candles),
            (BinanceFuturesUsd::default(), "eth", "usdt", MarketDataInstrumentKind::Perpetual, Candles),
        ])
        .init()
        .await?;

    let mut joined_stream = streams
        .select_all()
        .with_error_handler(|error| warn!(?error, "Candle stream generated error"));

    while let Some(event) = joined_stream.next().await {
        info!(?event, "received candle");
    }

    Ok(())
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_ansi(cfg!(debug_assertions))
        .json()
        .init()
}
```

- [ ] **Step 2: Add candles to indexed example**

In `barter-data/examples/indexed_market_stream.rs`, change the subscription list from:

```rust
        &[SubKind::PublicTrades, SubKind::OrderBooksL1, SubKind::OrderBooksL2]
```

to:

```rust
        &[SubKind::PublicTrades, SubKind::OrderBooksL1, SubKind::OrderBooksL2, SubKind::Candles]
```

- [ ] **Step 3: Check examples compile**

Run:

```bash
rtk cargo check -p barter-data --examples
```

Expected result: all `barter-data` examples compile.

- [ ] **Step 4: Commit examples**

Run:

```bash
rtk git add barter-data/examples/binance_futures_candles.rs barter-data/examples/indexed_market_stream.rs
rtk git commit -m "docs(data): add binance futures candle examples"
```

---

## Task 6: Full Verification

**Files:**
- No source edits expected.

- [ ] **Step 1: Run all barter-data tests**

Run:

```bash
rtk cargo test -p barter-data
```

Expected result: all `barter-data` tests pass.

- [ ] **Step 2: Run workspace check**

Run:

```bash
rtk cargo check --workspace
```

Expected result: workspace compiles.

- [ ] **Step 3: Review diff**

Run:

```bash
rtk git status --short
rtk git diff --stat HEAD
```

Expected result: no uncommitted source files from this plan remain. Existing unrelated `clash.txt` may still appear as untracked and should not be committed.

- [ ] **Step 4: Record final verification commit if needed**

If Step 3 shows no uncommitted plan files, no commit is needed. If formatting or documentation fixes were made during verification, commit only those files:

```bash
rtk git add <changed-files-from-this-plan>
rtk git commit -m "chore(data): verify binance futures candle support"
```
