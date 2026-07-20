# Provider Capability Descriptor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a static `barter-data` provider capability descriptor for Binance USD-M Futures, with consistency tests and lifecycle/error categories, without changing runtime subscription or stream behavior.

**Architecture:** Add one focused `barter-data/src/capability.rs` module exported from `barter-data/src/lib.rs`. The module declares static provider capabilities and helper lookup functions. Existing `exchange_supports_instrument_kind_sub_kind` and `DynamicStreams` remain the runtime paths, while tests protect descriptor consistency.

**Tech Stack:** Rust, `barter-data`, `barter_instrument`, existing `cargo test` / `cargo check` workflows. Use `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`, and `CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow` for verification.

---

## File Structure

- Create: `barter-data/src/capability.rs`
  - Owns descriptor types, static Binance USD-M Futures descriptor, lookup helpers, lifecycle/error categories, and descriptor tests.
- Modify: `barter-data/src/lib.rs`
  - Exports `pub mod capability;` near the existing public modules.
- No changes to:
  - `barter-data/src/subscription/mod.rs` runtime validation.
  - `barter-data/src/streams/builder/dynamic/mod.rs` runtime behavior.
  - MDB bridge code.

## Shared Commands

Use these exact cargo environment variables for all commands in this plan:

```bash
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow
export CARGO=/Volumes/wdata/rust/.cargo/bin/cargo
```

If `/Volumes/wdata/rust/.cargo/bin/cargo` is unavailable, use the active `cargo` in `PATH` and record that deviation in the task report.

---

### Task 1: Add capability module skeleton and failing public API test

**Files:**
- Create: `barter-data/src/capability.rs`
- Modify: `barter-data/src/lib.rs`

- [ ] **Step 1: Write the failing module export test**

Create `barter-data/src/capability.rs` with this test-only skeleton:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use barter_instrument::{
        exchange::ExchangeId,
        instrument::market_data::kind::MarketDataInstrumentKind,
    };

    #[test]
    fn binance_futures_usd_descriptor_exists() {
        let descriptor = provider_capability(ExchangeId::BinanceFuturesUsd)
            .expect("Binance USD-M Futures capability descriptor should exist");

        assert_eq!(descriptor.exchange, ExchangeId::BinanceFuturesUsd);
        assert!(
            descriptor
                .instrument_kinds
                .contains(&MarketDataInstrumentKind::Perpetual),
            "Binance USD-M Futures descriptor should declare perpetual instruments"
        );
    }
}
```

Modify `barter-data/src/lib.rs` by adding this module export after `pub mod books;` and before `pub mod transformer;`:

```rust
/// Static provider capability descriptors used by bridge and validation consumers.
pub mod capability;
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data capability::tests::binance_futures_usd_descriptor_exists -- --nocapture
```

Expected: FAIL to compile with missing `provider_capability` and descriptor types.

- [ ] **Step 3: Commit nothing**

Do not commit after the failing test. Continue to Task 2 to make the test pass.

---

### Task 2: Implement descriptor types, static Binance descriptor, and lookup helpers

**Files:**
- Modify: `barter-data/src/capability.rs`

- [ ] **Step 1: Replace `barter-data/src/capability.rs` with the implementation**

Use this full file content:

```rust
use crate::subscription::SubKind;
use barter_instrument::{
    exchange::ExchangeId,
    instrument::market_data::kind::MarketDataInstrumentKind,
};

/// Static capability descriptor for a market-data provider.
#[derive(Debug, PartialEq, Eq)]
pub struct ProviderCapabilityDescriptor {
    pub exchange: ExchangeId,
    pub instrument_kinds: &'static [MarketDataInstrumentKind],
    pub capabilities: &'static [DataCapability],
    pub reference_capabilities: &'static [ReferenceCapability],
}

/// Capability for a subscription-backed market-data surface.
#[derive(Debug, PartialEq, Eq)]
pub struct DataCapability {
    pub sub_kind: SubKind,
    pub data_kind: CapabilityDataKind,
    pub transports: &'static [TransportKind],
    pub dynamic_stream: DynamicStreamCapability,
    pub history: HistoryCapability,
    pub notes: &'static str,
}

/// Capability for reference data that is not represented by `SubKind`.
#[derive(Debug, PartialEq, Eq)]
pub struct ReferenceCapability {
    pub data_kind: CapabilityDataKind,
    pub transports: &'static [TransportKind],
    pub history: HistoryCapability,
    pub notes: &'static str,
}

/// Normalized descriptor data surface identifiers.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CapabilityDataKind {
    PublicTrade,
    OrderBookL1,
    OrderBookL2,
    Candle,
    Liquidation,
    MarkPrice,
    IndexPrice,
    FundingRate,
    OpenInterest,
    Instrument,
    TakerFlow,
}

/// Transport available for a capability.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TransportKind {
    WebSocket,
    RestFetch,
    RestPoll,
}

/// Whether a capability can be initialized through `DynamicStreams`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DynamicStreamCapability {
    Supported,
    Unsupported,
}

/// Historical access supported by a capability.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum HistoryCapability {
    Unsupported,
    LatestOnly,
    RecentWindow,
    HistoricalRange,
}

/// Stable capability and lifecycle error categories for bridge diagnostics.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CapabilityErrorKind {
    UnsupportedCapability,
    UnsupportedDynamicStream,
    RestFetchFailed,
    DecodeFailed,
    TransportDisconnected,
}

const BINANCE_FUTURES_USD_INSTRUMENT_KINDS: &[MarketDataInstrumentKind] =
    &[MarketDataInstrumentKind::Perpetual];

const WS: &[TransportKind] = &[TransportKind::WebSocket];
const REST_FETCH: &[TransportKind] = &[TransportKind::RestFetch];
const WS_AND_REST_FETCH: &[TransportKind] = &[TransportKind::WebSocket, TransportKind::RestFetch];

const BINANCE_FUTURES_USD_CAPABILITIES: &[DataCapability] = &[
    DataCapability {
        sub_kind: SubKind::PublicTrades,
        data_kind: CapabilityDataKind::PublicTrade,
        transports: WS,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::Unsupported,
        notes: "WebSocket public trade stream.",
    },
    DataCapability {
        sub_kind: SubKind::OrderBooksL1,
        data_kind: CapabilityDataKind::OrderBookL1,
        transports: WS,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::Unsupported,
        notes: "WebSocket best bid/ask stream.",
    },
    DataCapability {
        sub_kind: SubKind::OrderBooksL2,
        data_kind: CapabilityDataKind::OrderBookL2,
        transports: WS,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::Unsupported,
        notes: "WebSocket order book delta stream with initial snapshot handling.",
    },
    DataCapability {
        sub_kind: SubKind::Candles,
        data_kind: CapabilityDataKind::Candle,
        transports: WS,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::Unsupported,
        notes: "WebSocket kline stream.",
    },
    DataCapability {
        sub_kind: SubKind::Liquidations,
        data_kind: CapabilityDataKind::Liquidation,
        transports: WS,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::Unsupported,
        notes: "WebSocket force-order liquidation stream.",
    },
    DataCapability {
        sub_kind: SubKind::MarkPrices,
        data_kind: CapabilityDataKind::MarkPrice,
        transports: WS_AND_REST_FETCH,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::LatestOnly,
        notes: "WebSocket mark price stream and latest REST fetch.",
    },
    DataCapability {
        sub_kind: SubKind::IndexPrices,
        data_kind: CapabilityDataKind::IndexPrice,
        transports: WS_AND_REST_FETCH,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::LatestOnly,
        notes: "WebSocket index price stream and latest REST fetch.",
    },
    DataCapability {
        sub_kind: SubKind::FundingRates,
        data_kind: CapabilityDataKind::FundingRate,
        transports: REST_FETCH,
        dynamic_stream: DynamicStreamCapability::Unsupported,
        history: HistoryCapability::HistoricalRange,
        notes: "REST funding rate history fetcher with startTime, endTime, and limit request fields.",
    },
    DataCapability {
        sub_kind: SubKind::OpenInterests,
        data_kind: CapabilityDataKind::OpenInterest,
        transports: REST_FETCH,
        dynamic_stream: DynamicStreamCapability::Unsupported,
        history: HistoryCapability::LatestOnly,
        notes: "REST latest open interest fetcher.",
    },
    DataCapability {
        sub_kind: SubKind::TakerFlows,
        data_kind: CapabilityDataKind::TakerFlow,
        transports: REST_FETCH,
        dynamic_stream: DynamicStreamCapability::Unsupported,
        history: HistoryCapability::RecentWindow,
        notes: "REST taker buy/sell volume ratio fetcher with period and limit request fields.",
    },
];

const BINANCE_FUTURES_USD_REFERENCE_CAPABILITIES: &[ReferenceCapability] = &[ReferenceCapability {
    data_kind: CapabilityDataKind::Instrument,
    transports: REST_FETCH,
    history: HistoryCapability::LatestOnly,
    notes: "REST exchangeInfo instrument discovery for trading USD-M perpetual contracts.",
}];

pub const BINANCE_FUTURES_USD_DESCRIPTOR: ProviderCapabilityDescriptor =
    ProviderCapabilityDescriptor {
        exchange: ExchangeId::BinanceFuturesUsd,
        instrument_kinds: BINANCE_FUTURES_USD_INSTRUMENT_KINDS,
        capabilities: BINANCE_FUTURES_USD_CAPABILITIES,
        reference_capabilities: BINANCE_FUTURES_USD_REFERENCE_CAPABILITIES,
    };

const PROVIDER_CAPABILITIES: &[ProviderCapabilityDescriptor] = &[BINANCE_FUTURES_USD_DESCRIPTOR];

/// Return all static provider capability descriptors.
pub fn provider_capabilities() -> &'static [ProviderCapabilityDescriptor] {
    PROVIDER_CAPABILITIES
}

/// Return the static provider capability descriptor for an exchange.
pub fn provider_capability(
    exchange: ExchangeId,
) -> Option<&'static ProviderCapabilityDescriptor> {
    provider_capabilities()
        .iter()
        .find(|descriptor| descriptor.exchange == exchange)
}

/// Return whether the descriptor declares a subscription-backed capability.
pub fn supports_capability(
    exchange: ExchangeId,
    instrument_kind: &MarketDataInstrumentKind,
    sub_kind: SubKind,
) -> bool {
    provider_capability(exchange).is_some_and(|descriptor| {
        descriptor.instrument_kinds.contains(instrument_kind)
            && descriptor
                .capabilities
                .iter()
                .any(|capability| capability.sub_kind == sub_kind)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::exchange_supports_instrument_kind_sub_kind;

    fn binance_futures_usd_descriptor() -> &'static ProviderCapabilityDescriptor {
        provider_capability(ExchangeId::BinanceFuturesUsd)
            .expect("Binance USD-M Futures capability descriptor should exist")
    }

    fn capability(sub_kind: SubKind) -> &'static DataCapability {
        binance_futures_usd_descriptor()
            .capabilities
            .iter()
            .find(|capability| capability.sub_kind == sub_kind)
            .unwrap_or_else(|| panic!("missing capability for {sub_kind:?}"))
    }

    #[test]
    fn binance_futures_usd_descriptor_exists() {
        let descriptor = binance_futures_usd_descriptor();

        assert_eq!(descriptor.exchange, ExchangeId::BinanceFuturesUsd);
        assert!(
            descriptor
                .instrument_kinds
                .contains(&MarketDataInstrumentKind::Perpetual),
            "Binance USD-M Futures descriptor should declare perpetual instruments"
        );
    }
}
```

- [ ] **Step 2: Run focused test**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data capability::tests::binance_futures_usd_descriptor_exists -- --nocapture
```

Expected: PASS with `1 passed` for the focused capability test.

- [ ] **Step 3: Run compile check for public API**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo check -p barter-data
```

Expected: PASS with no new errors.

- [ ] **Step 4: Format and diff check**

Run:

```bash
cargo fmt --all -- --check
git diff --check
```

Expected: both commands exit 0.

- [ ] **Step 5: Commit Task 2**

Run:

```bash
git add barter-data/src/lib.rs barter-data/src/capability.rs
git commit -m "feat(data): add provider capability descriptor"
```

Expected: commit succeeds. Working tree should be clean after `git status --short`.

---

### Task 3: Add support-matrix and descriptor consistency tests

**Files:**
- Modify: `barter-data/src/capability.rs`

- [ ] **Step 1: Add failing consistency tests**

Append these tests inside the existing `#[cfg(test)] mod tests` block in `barter-data/src/capability.rs`, after `binance_futures_usd_descriptor_exists`:

```rust
    #[test]
    fn binance_futures_usd_descriptor_does_not_claim_spot_support() {
        let descriptor = binance_futures_usd_descriptor();

        assert!(
            !descriptor
                .instrument_kinds
                .contains(&MarketDataInstrumentKind::Spot),
            "Binance USD-M Futures descriptor must not imply spot instrument support"
        );
    }

    #[test]
    fn descriptor_capabilities_match_existing_support_matrix() {
        for capability in binance_futures_usd_descriptor().capabilities {
            assert!(
                exchange_supports_instrument_kind_sub_kind(
                    &ExchangeId::BinanceFuturesUsd,
                    &MarketDataInstrumentKind::Perpetual,
                    capability.sub_kind,
                ),
                "descriptor capability {capability:?} should be supported by existing support matrix"
            );
        }
    }

    #[test]
    fn every_binance_futures_support_matrix_sub_kind_has_descriptor_entry() {
        let supported = [
            SubKind::PublicTrades,
            SubKind::OrderBooksL1,
            SubKind::OrderBooksL2,
            SubKind::Liquidations,
            SubKind::Candles,
            SubKind::MarkPrices,
            SubKind::FundingRates,
            SubKind::IndexPrices,
            SubKind::OpenInterests,
            SubKind::TakerFlows,
        ];

        for sub_kind in supported {
            assert!(
                binance_futures_usd_descriptor()
                    .capabilities
                    .iter()
                    .any(|capability| capability.sub_kind == sub_kind),
                "support matrix sub_kind {sub_kind:?} should have a descriptor capability"
            );
        }
    }

    #[test]
    fn supports_capability_agrees_with_descriptor_contents() {
        for capability in binance_futures_usd_descriptor().capabilities {
            assert!(supports_capability(
                ExchangeId::BinanceFuturesUsd,
                &MarketDataInstrumentKind::Perpetual,
                capability.sub_kind,
            ));
        }

        assert!(!supports_capability(
            ExchangeId::BinanceFuturesUsd,
            &MarketDataInstrumentKind::Spot,
            SubKind::PublicTrades,
        ));
        assert!(!supports_capability(
            ExchangeId::BinanceSpot,
            &MarketDataInstrumentKind::Perpetual,
            SubKind::PublicTrades,
        ));
    }
```

- [ ] **Step 2: Run tests to verify behavior**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data capability -- --nocapture
```

Expected: PASS. If this fails, fix only descriptor contents or helper logic. Do not change `exchange_supports_instrument_kind_sub_kind` unless the user explicitly approves a scope change.

- [ ] **Step 3: Format and diff check**

Run:

```bash
cargo fmt --all -- --check
git diff --check
```

Expected: both commands exit 0.

- [ ] **Step 4: Commit Task 3**

Run:

```bash
git add barter-data/src/capability.rs
git commit -m "test(data): verify capability support matrix consistency"
```

Expected: commit succeeds. Working tree should be clean after `git status --short`.

---

### Task 4: Add transport, DynamicStreams boundary, reference capability, and error-category tests

**Files:**
- Modify: `barter-data/src/capability.rs`

- [ ] **Step 1: Add descriptor boundary tests**

Append these tests inside the existing `#[cfg(test)] mod tests` block in `barter-data/src/capability.rs`:

```rust
    #[test]
    fn rest_only_surfaces_are_not_dynamic_stream_capable() {
        for sub_kind in [
            SubKind::FundingRates,
            SubKind::OpenInterests,
            SubKind::TakerFlows,
        ] {
            let capability = capability(sub_kind);
            assert_eq!(
                capability.dynamic_stream,
                DynamicStreamCapability::Unsupported,
                "{sub_kind:?} should be REST-only and not DynamicStreams capable"
            );
            assert_eq!(capability.transports, REST_FETCH);
            assert!(
                !capability.transports.contains(&TransportKind::WebSocket),
                "{sub_kind:?} should not declare WebSocket transport"
            );
        }
    }

    #[test]
    fn websocket_surfaces_are_dynamic_stream_capable() {
        for sub_kind in [
            SubKind::PublicTrades,
            SubKind::OrderBooksL1,
            SubKind::OrderBooksL2,
            SubKind::Candles,
            SubKind::Liquidations,
            SubKind::MarkPrices,
            SubKind::IndexPrices,
        ] {
            let capability = capability(sub_kind);
            assert_eq!(
                capability.dynamic_stream,
                DynamicStreamCapability::Supported,
                "{sub_kind:?} should be DynamicStreams capable"
            );
            assert!(
                capability.transports.contains(&TransportKind::WebSocket),
                "{sub_kind:?} should declare WebSocket transport"
            );
        }
    }

    #[test]
    fn mark_and_index_prices_declare_websocket_and_rest_fetch() {
        for sub_kind in [SubKind::MarkPrices, SubKind::IndexPrices] {
            let capability = capability(sub_kind);
            assert!(capability.transports.contains(&TransportKind::WebSocket));
            assert!(capability.transports.contains(&TransportKind::RestFetch));
        }
    }

    #[test]
    fn history_capabilities_match_existing_fetchers() {
        assert_eq!(
            capability(SubKind::FundingRates).history,
            HistoryCapability::HistoricalRange
        );
        assert_eq!(
            capability(SubKind::OpenInterests).history,
            HistoryCapability::LatestOnly
        );
        assert_eq!(
            capability(SubKind::TakerFlows).history,
            HistoryCapability::RecentWindow
        );
        assert_eq!(
            capability(SubKind::MarkPrices).history,
            HistoryCapability::LatestOnly
        );
        assert_eq!(
            capability(SubKind::IndexPrices).history,
            HistoryCapability::LatestOnly
        );
    }

    #[test]
    fn reference_discovery_is_not_a_synthetic_sub_kind() {
        let descriptor = binance_futures_usd_descriptor();

        assert_eq!(descriptor.reference_capabilities.len(), 1);
        assert_eq!(
            descriptor.reference_capabilities[0].data_kind,
            CapabilityDataKind::Instrument
        );
        assert_eq!(descriptor.reference_capabilities[0].transports, REST_FETCH);
        assert_eq!(
            descriptor.reference_capabilities[0].history,
            HistoryCapability::LatestOnly
        );
        assert!(
            descriptor
                .capabilities
                .iter()
                .all(|capability| capability.data_kind != CapabilityDataKind::Instrument),
            "Instrument discovery should live in reference_capabilities, not SubKind-backed capabilities"
        );
    }

    #[test]
    fn capability_error_kinds_are_stable_bridge_categories() {
        let categories = [
            CapabilityErrorKind::UnsupportedCapability,
            CapabilityErrorKind::UnsupportedDynamicStream,
            CapabilityErrorKind::RestFetchFailed,
            CapabilityErrorKind::DecodeFailed,
            CapabilityErrorKind::TransportDisconnected,
        ];

        assert_eq!(categories.len(), 5);
        assert_eq!(
            format!("{:?}", CapabilityErrorKind::UnsupportedDynamicStream),
            "UnsupportedDynamicStream"
        );
    }
```

- [ ] **Step 2: Run focused tests**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data capability -- --nocapture
```

Expected: PASS with all capability tests passing.

- [ ] **Step 3: Run targeted dynamic boundary regression tests**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data dynamic -- --nocapture
```

Expected: PASS. This confirms existing `DynamicStreams` boundary tests still pass. If no tests match because the local test names are more specific, run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data channels_ -- --nocapture
```

Expected: PASS for channel generation and unsupported REST-only dynamic behavior.

- [ ] **Step 4: Format and diff check**

Run:

```bash
cargo fmt --all -- --check
git diff --check
```

Expected: both commands exit 0.

- [ ] **Step 5: Commit Task 4**

Run:

```bash
git add barter-data/src/capability.rs
git commit -m "test(data): verify capability transport boundaries"
```

Expected: commit succeeds. Working tree should be clean after `git status --short`.

---

### Task 5: Final verification and two-stage review readiness

**Files:**
- No source changes expected.
- Review source: `barter-data/src/capability.rs`, `barter-data/src/lib.rs`, `docs/superpowers/specs/2026-07-20-provider-capability-descriptor-design.md`

- [ ] **Step 1: Run full focused verification matrix**

Run:

```bash
cargo fmt --all -- --check
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data capability -- --nocapture
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data dynamic -- --nocapture
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo check -p barter-data
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo check -p barter-data --examples
git diff --check
git status --short
git log --oneline -5
```

Expected:

- `cargo fmt --all -- --check`: exit 0.
- `cargo test -p barter-data capability`: all capability tests pass.
- `cargo test -p barter-data dynamic`: dynamic boundary tests pass, or if no test names match, record that and use the `channels_` fallback from Task 4.
- `cargo test -p barter-data`: all barter-data tests pass.
- `cargo check -p barter-data`: exit 0.
- `cargo check -p barter-data --examples`: exit 0.
- `git diff --check`: exit 0.
- `git status --short`: no output.
- `git log --oneline -5`: includes the descriptor implementation commits.

- [ ] **Step 2: Request spec compliance review**

Dispatch a review agent or perform an explicit review with this checklist:

```text
Review scope: provider capability descriptor implementation.
Spec file: docs/superpowers/specs/2026-07-20-provider-capability-descriptor-design.md
Files: barter-data/src/capability.rs, barter-data/src/lib.rs
Check:
1. Descriptor is static and machine-readable.
2. Binance USD-M Futures Perpetual capabilities match the spec table.
3. Reference discovery is in reference_capabilities, not a synthetic SubKind.
4. Runtime validation and DynamicStreams behavior are not replaced.
5. Lifecycle/error categories exist without large error rewrites.
6. Acceptance criteria are covered by tests.
Return Critical, Important, Minor findings.
```

Expected: no Critical or Important findings. Fix any Critical or Important findings before final completion.

- [ ] **Step 3: Request quality review**

Dispatch a review agent or perform an explicit review with this checklist:

```text
Review scope: code quality for provider capability descriptor.
Files: barter-data/src/capability.rs, barter-data/src/lib.rs
Check:
1. Public API is small, readable, and stable enough for MDB bridge consumption.
2. Tests are deterministic and do not perform network calls.
3. Descriptor data does not duplicate behavior in a way that can silently drift without tests.
4. Naming is clear and idiomatic Rust.
5. No unnecessary dependencies or feature changes were added.
6. No unrelated refactors were introduced.
Return Critical, Important, Minor findings.
```

Expected: no Critical or Important findings. Fix any Critical or Important findings before final completion.

- [ ] **Step 4: Commit review fixes if needed**

If reviews require fixes, make the smallest changes, run:

```bash
cargo fmt --all -- --check
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data capability -- --nocapture
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow \
/Volumes/wdata/rust/.cargo/bin/cargo check -p barter-data
git diff --check
```

Then commit:

```bash
git add barter-data/src/capability.rs barter-data/src/lib.rs
git commit -m "fix(data): address capability descriptor review"
```

Expected: fixes are committed and `git status --short` is clean.

- [ ] **Step 5: Report completion**

Final report must include:

- Commits created.
- Verification commands and outcomes.
- Spec review result.
- Quality review result.
- Statement that runtime validation and `DynamicStreams` behavior were not changed.
- Next recommended step: start MDB bridge design or implement descriptor serialization only if MDB needs exported JSON.
