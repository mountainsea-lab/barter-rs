# Binance Futures Funding Rates Follow-up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete Binance USD-M Futures Funding Rates as a first-class REST-backed data kind with stable dynamic configuration semantics and no fake WebSocket stream.

**Architecture:** Keep `BinanceFuturesUsdFundingRateFetcher` as the canonical source for real funding-rate rows. Add `SubKind::FundingRates` and support-matrix coverage so configs can express the data kind, but make `DynamicStreams` runtime reject it explicitly until a REST polling source exists. This preserves protocol honesty: funding rates are REST-backed here, not derived from Binance mark-price WebSocket messages.

**Tech Stack:** Rust, serde, chrono, rust_decimal, reqwest, tokio, existing `barter-data` subscription, event, and dynamic stream builder APIs.

---

## File Map

- Modify `barter-data/src/subscription/mod.rs`
  - Add `SubKind::FundingRates`.
  - Extend SubKind serde regression test.
  - Extend support matrix and support-matrix tests.
- Modify `barter-data/src/streams/builder/dynamic/mod.rs`
  - Import `FundingRate` and `FundingRates` only if selector helper APIs are added.
  - For this plan, do not add runtime channel allocation for `FundingRates`.
  - Add a test proving DynamicStreams currently rejects `FundingRates` explicitly with `DataError::UnsupportedSubKind(SubKind::FundingRates)`.
- Modify `barter-data/src/exchange/binance/futures/funding.rs`
  - Add `error_for_status()` to REST fetcher.
  - Keep existing parser, conversion, and URL tests.
- Verify `barter-data/examples/binance_futures_funding.rs`
  - Existing REST example already demonstrates recent historical fetch. Keep it unless check fails.
- Create `docs/superpowers/plans/2026-07-19-binance-futures-funding-rates-follow-up.md`
  - This plan.

---

### Task 1: Add `SubKind::FundingRates` and support matrix

**Files:**
- Modify: `barter-data/src/subscription/mod.rs`

- [ ] **Step 1: Write failing tests for SubKind serde and support matrix**

In `barter-data/src/subscription/mod.rs`, extend the existing test `sub_kind_serde_uses_subscription_kind_strings` by adding this case to the `cases` array:

```rust
(SubKind::FundingRates, "funding_rates"),
```

Add these two tests near the existing MarkPrices support tests in the same `#[cfg(test)] mod tests` block:

```rust
#[test]
fn test_binance_futures_usd_supports_perpetual_funding_rates() {
    assert!(exchange_supports_instrument_kind_sub_kind(
        &ExchangeId::BinanceFuturesUsd,
        &MarketDataInstrumentKind::Perpetual,
        SubKind::FundingRates,
    ));
}

#[test]
fn test_binance_futures_usd_rejects_spot_funding_rates() {
    assert!(!exchange_supports_instrument_kind_sub_kind(
        &ExchangeId::BinanceFuturesUsd,
        &MarketDataInstrumentKind::Spot,
        SubKind::FundingRates,
    ));
}
```

If the local test module does not already import `ExchangeId` and `MarketDataInstrumentKind` at that scope, add:

```rust
use barter_instrument::{
    exchange::ExchangeId,
    instrument::market_data::kind::MarketDataInstrumentKind,
};
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-funding-follow-up rtk cargo test -p barter-data funding_rates -- --nocapture
```

Expected: compile failure because `SubKind::FundingRates` does not exist, or test failure because support matrix does not include it.

- [ ] **Step 3: Add the minimal implementation**

In `barter-data/src/subscription/mod.rs`, add `FundingRates` to `SubKind`:

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
}
```

In `exchange_supports_instrument_kind_sub_kind`, update the Binance USD-M perpetual arm from:

```rust
PublicTrades | OrderBooksL1 | OrderBooksL2 | Liquidations | Candles | MarkPrices,
```

to:

```rust
PublicTrades
| OrderBooksL1
| OrderBooksL2
| Liquidations
| Candles
| MarkPrices
| FundingRates,
```

Keep `#[serde(rename_all = "snake_case")]` on `SubKind`. It should make the new variant serialize as `"funding_rates"`.

- [ ] **Step 4: Run focused tests to verify they pass**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-funding-follow-up rtk cargo test -p barter-data funding_rates -- --nocapture
```

Expected: all funding-rate focused tests pass.

- [ ] **Step 5: Run formatting and diff check**

Run:

```bash
rtk cargo fmt --all -- --check
rtk git diff --check
```

Expected: both exit 0.

- [ ] **Step 6: Commit**

Run:

```bash
rtk git add barter-data/src/subscription/mod.rs
rtk git commit -m "feat(data): add funding rates sub kind support"
```

---

### Task 2: Make DynamicStreams runtime rejection explicit and tested

**Files:**
- Modify: `barter-data/src/streams/builder/dynamic/mod.rs`

**Design decision for this task:** Do not add `FundingRate` channels or WebSocket selectors. Binance does not provide a distinct funding-rate WebSocket event, and the existing `DynamicStreams::init` runtime is WebSocket-oriented. `FundingRates` should be expressible and support-matrix-valid, but runtime selection should fail clearly until a REST polling source abstraction exists.

- [ ] **Step 1: Write failing test for explicit runtime rejection**

In `barter-data/src/streams/builder/dynamic/mod.rs`, add this test in the existing `#[cfg(test)] mod tests` block:

```rust
#[test]
fn channels_reject_funding_rates_runtime_without_rest_source() {
    let batches: Vec<Vec<Subscription<ExchangeId, MarketDataInstrument, SubKind>>> =
        vec![vec![Subscription::new(
            ExchangeId::BinanceFuturesUsd,
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Perpetual)),
            SubKind::FundingRates,
        )]];

    let actual = Channels::try_from(&batches);

    assert_eq!(
        actual.unwrap_err(),
        DataError::UnsupportedSubKind(SubKind::FundingRates)
    );
}
```

This test may already pass after Task 1 because the existing fallback returns `UnsupportedSubKind`. Keep it as a regression test documenting the intended runtime behavior.

- [ ] **Step 2: Run focused test**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-funding-follow-up rtk cargo test -p barter-data channels_reject_funding_rates_runtime_without_rest_source -- --nocapture
```

Expected: pass with the existing fallback or fail with a mismatch that must be corrected to `DataError::UnsupportedSubKind(SubKind::FundingRates)`.

- [ ] **Step 3: Minimal implementation only if the test fails**

If the test fails because `Channels::try_from` returns a different error, adjust the final fallback in the channel allocation match to preserve:

```rust
unsupported => return Err(DataError::UnsupportedSubKind(unsupported)),
```

Do not add `FundingRates` to `Txs`, `Rxs`, `DynamicStreams`, `select_all`, or the `init_market_stream` match in this task. That would imply a live stream source that does not exist yet.

- [ ] **Step 4: Run focused dynamic tests**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-funding-follow-up rtk cargo test -p barter-data channels_ -- --nocapture
```

Expected: existing channel tests pass, including MarkPrices allocation and FundingRates rejection.

- [ ] **Step 5: Run formatting and diff check**

Run:

```bash
rtk cargo fmt --all -- --check
rtk git diff --check
```

Expected: both exit 0.

- [ ] **Step 6: Commit**

Run:

```bash
rtk git add barter-data/src/streams/builder/dynamic/mod.rs
rtk git commit -m "test(data): document funding rates dynamic runtime boundary"
```

---

### Task 3: Improve Binance funding REST HTTP error handling

**Files:**
- Modify: `barter-data/src/exchange/binance/futures/funding.rs`

- [ ] **Step 1: Inspect current fetcher code**

Find this block in `BinanceFuturesUsdFundingRateFetcher::fetch`:

```rust
let rows = reqwest::get(funding_url)
    .await
    .map_err(barter_integration::error::SocketError::Http)?
    .json::<Vec<BinanceFuturesFundingRate>>()
    .await
    .map_err(barter_integration::error::SocketError::Http)?;
```

- [ ] **Step 2: Add minimal implementation**

Change it to:

```rust
let rows = reqwest::get(funding_url)
    .await
    .map_err(barter_integration::error::SocketError::Http)?
    .error_for_status()
    .map_err(barter_integration::error::SocketError::Http)?
    .json::<Vec<BinanceFuturesFundingRate>>()
    .await
    .map_err(barter_integration::error::SocketError::Http)?;
```

This mirrors the code-review feedback previously applied to Mark Price REST semantics without introducing live HTTP tests.

- [ ] **Step 3: Run funding REST parser and URL tests**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-funding-follow-up rtk cargo test -p barter-data funding_rate -- --nocapture
```

Expected: funding parser, conversion, and URL tests pass.

- [ ] **Step 4: Run examples check**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-funding-follow-up rtk cargo check -p barter-data --examples -j1
```

Expected: examples compile, including `barter-data/examples/binance_futures_funding.rs`.

- [ ] **Step 5: Run formatting and diff check**

Run:

```bash
rtk cargo fmt --all -- --check
rtk git diff --check
```

Expected: both exit 0.

- [ ] **Step 6: Commit**

Run:

```bash
rtk git add barter-data/src/exchange/binance/futures/funding.rs
rtk git commit -m "fix(binance): surface funding rate HTTP errors"
```

---

### Task 4: Full verification and code review

**Files:**
- No required source changes unless verification or review finds an issue.

- [ ] **Step 1: Run full verification with disk-space-safe settings**

Run:

```bash
rm -rf /tmp/barter-rs-target-funding-follow-up-final
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-funding-follow-up-final rtk cargo fmt --all -- --check
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-funding-follow-up-final rtk cargo test -p barter-data -j1
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-funding-follow-up-final rtk cargo check -p barter-data -j1
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-funding-follow-up-final rtk cargo check -p barter-data --examples -j1
rtk git diff --check
```

Expected: all commands exit 0. If disk space fails, remove the temp target directory and rerun one command at a time.

- [ ] **Step 2: Check worktree and recent commits**

Run:

```bash
rtk git status --short
rtk git log --oneline -5
```

Expected: `git status --short` is empty after all task commits.

- [ ] **Step 3: Request code review**

Use `superpowers:requesting-code-review` with the range covering Task 1 through Task 3 commits.

Review prompt summary:

```text
Review Binance USD-M Futures Funding Rates follow-up. It adds SubKind::FundingRates, support-matrix coverage, documents DynamicStreams runtime rejection because no Binance funding-rate WS stream exists, and improves REST fetcher HTTP error handling with error_for_status. Check protocol correctness, config/API compatibility, tests, and whether runtime rejection is clear.
```

- [ ] **Step 4: Handle review feedback**

For any Critical or Important item:

1. Verify it against the codebase.
2. Add a failing test if applicable.
3. Implement the minimal fix.
4. Rerun the focused test.
5. Rerun full verification from Step 1.
6. Commit with a focused message.

- [ ] **Step 5: Clean temp targets and final status**

Run:

```bash
rm -rf /tmp/barter-rs-target-funding-follow-up /tmp/barter-rs-target-funding-follow-up-final
rtk git status --short
```

Expected: no temp build artifacts in `/tmp` for this task and clean worktree.

---

## Self-Review Notes

Spec coverage:

- `SubKind::FundingRates` and snake_case serde: Task 1.
- Binance USD-M perpetual support and spot rejection: Task 1.
- REST fetcher remains canonical: Task 3 keeps fetcher and improves HTTP errors.
- Dynamic wiring honesty: Task 2 tests explicit runtime rejection rather than fake WebSocket routing.
- Example check: Task 3 compiles the existing funding example.
- Full verification and code review: Task 4.

No generic REST polling runtime is included. No Mark Price to Funding Rate derivation is included. No live Binance network test is included.
