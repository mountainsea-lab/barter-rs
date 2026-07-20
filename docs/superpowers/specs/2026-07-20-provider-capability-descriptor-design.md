# Provider Capability Descriptor Design

## Status

Approved for design by user on 2026-07-20.

## Context

The `barter-rs` fork is being extended as the canonical market-data collection layer for the MDB ingestion path. Recent Binance USD-M Futures work added or confirmed support for WebSocket streams, REST fetchers, normalized data models, support matrix entries, and explicit `DynamicStreams` rejection for REST-only surfaces.

MDB should consume a thin `fdc-ingestion` barter bridge instead of maintaining a separate `fdc-adapters` provider implementation. Before building that bridge, `barter-data` needs a machine-readable provider capability descriptor so MDB can discover which data surfaces exist and how each surface should be consumed.

Existing state:

- `SubKind` and `exchange_supports_instrument_kind_sub_kind` define supported exchange/instrument/subscription combinations.
- `DataKind` defines normalized event payload variants.
- `DynamicStreams` supports runtime WebSocket streams and explicitly rejects REST-only surfaces such as funding rates, open interest, and taker flow.
- Binance USD-M Futures now has REST/WS support across trades, order books, candles, liquidations, mark price, index price, funding rates, open interest, exchange info, and taker flow.
- No first-class `capability` module or provider descriptor exists yet.

## Goals

- Add a static, machine-readable provider capability descriptor in `barter-data`.
- Keep descriptor V0 as a declaration and test target. Do not replace existing runtime validation yet.
- Make Binance USD-M Futures support discoverable by MDB without requiring MDB to duplicate support tables.
- Express transport mode per data surface: WebSocket, REST poll/fetch, or both.
- Express whether a surface is eligible for `DynamicStreams`.
- Document lifecycle/error contract V0 so MDB can map Barter collection failures into stable bridge diagnostics.
- Preserve the existing public behavior of `Subscription` validation and `DynamicStreams`.

## Non-goals

- Do not implement the MDB bridge in this increment.
- Do not replace `exchange_supports_instrument_kind_sub_kind` with descriptor-driven validation.
- Do not redesign `DataError` or force all REST/WS paths into a new error enum.
- Do not add runtime provider discovery, remote metadata fetching, caching, or registry persistence.
- Do not add historical-window support for REST-only data unless the surface already supports it.
- Do not change current stream initialization APIs.

## Recommended Approach

Use a static descriptor module with consistency tests.

The descriptor becomes the bridge-facing source of truth for provider capabilities, while existing runtime validation remains unchanged for V0. Tests compare the descriptor against existing support matrix behavior and the known `DynamicStreams` REST-only boundary.

This is intentionally conservative. It gives MDB a stable contract now, while leaving a later migration path where runtime validation can be driven by the descriptor after enough tests and consumers exist.

## Public Module Shape

Add a new module under `barter-data/src/capability.rs` or `barter-data/src/capability/mod.rs`, exported from `barter-data/src/lib.rs`.

Core types:

```rust
pub struct ProviderCapabilityDescriptor {
    pub exchange: ExchangeId,
    pub instrument_kinds: &'static [MarketDataInstrumentKind],
    pub capabilities: &'static [DataCapability],
    pub reference_capabilities: &'static [ReferenceCapability],
}

pub struct DataCapability {
    pub sub_kind: SubKind,
    pub data_kind: CapabilityDataKind,
    pub transports: &'static [TransportKind],
    pub dynamic_stream: DynamicStreamCapability,
    pub history: HistoryCapability,
    pub notes: &'static str,
}

pub struct ReferenceCapability {
    pub data_kind: CapabilityDataKind,
    pub transports: &'static [TransportKind],
    pub history: HistoryCapability,
    pub notes: &'static str,
}

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

pub enum TransportKind {
    WebSocket,
    RestFetch,
    RestPoll,
}

pub enum DynamicStreamCapability {
    Supported,
    Unsupported,
}

pub enum HistoryCapability {
    Unsupported,
    LatestOnly,
    RecentWindow,
    HistoricalRange,
}
```

V0 may derive `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, and optionally `Serialize` when existing crate feature policy allows it. If adding `Serialize` changes public dependencies or feature behavior, leave serialization for a later MDB bridge-specific wrapper.

## Descriptor API

Expose functions that are simple and static:

```rust
pub fn provider_capabilities() -> &'static [ProviderCapabilityDescriptor];

pub fn provider_capability(
    exchange: ExchangeId,
) -> Option<&'static ProviderCapabilityDescriptor>;

pub fn supports_capability(
    exchange: ExchangeId,
    instrument_kind: MarketDataInstrumentKind,
    sub_kind: SubKind,
) -> bool;
```

`supports_capability` should be descriptor-based and used by tests or consumers, but it should not replace `exchange_supports_instrument_kind_sub_kind` in V0.

## Binance USD-M Futures V0 Coverage

Add one descriptor for `ExchangeId::BinanceFuturesUsd`.

Instrument kinds:

- `MarketDataInstrumentKind::Perpetual`

Capabilities:

| Surface | SubKind | Data kind | Transports | DynamicStreams | History |
| --- | --- | --- | --- | --- | --- |
| Public trades | `PublicTrades` | `PublicTrade` | `WebSocket` | Supported | Unsupported |
| Order book L1 | `OrderBooksL1` | `OrderBookL1` | `WebSocket` | Supported | Unsupported |
| Order book L2 | `OrderBooksL2` | `OrderBookL2` | `WebSocket` | Supported | Unsupported |
| Candles | `Candles` | `Candle` | `WebSocket` | Supported | Unsupported |
| Liquidations | `Liquidations` | `Liquidation` | `WebSocket` | Supported | Unsupported |
| Mark price | `MarkPrices` | `MarkPrice` | `WebSocket`, `RestFetch` | Supported | LatestOnly |
| Index price | `IndexPrices` | `IndexPrice` | `WebSocket`, `RestFetch` | Supported | LatestOnly |
| Funding rates | `FundingRates` | `FundingRate` | `RestFetch` | Unsupported | HistoricalRange |
| Open interest | `OpenInterests` | `OpenInterest` | `RestFetch` | Unsupported | LatestOnly |
| Taker flow | `TakerFlows` | `TakerFlow` | `RestFetch` | Unsupported | RecentWindow |

Reference capabilities:

| Surface | Data kind | Transports | History |
| --- | --- | --- | --- |
| Instrument discovery | `Instrument` | `RestFetch` | LatestOnly |

Reference discovery is not represented by `SubKind` in V0 because current `SubKind` variants describe market-data subscriptions. It should live in `reference_capabilities` rather than adding a new subscription kind or forcing a fake `SubKind` value.

## Lifecycle and Error Contract V0

The descriptor should document stable categories that MDB can map to bridge diagnostics. V0 categories are semantic categories, not necessarily new Rust error variants used everywhere.

```rust
pub enum CapabilityErrorKind {
    UnsupportedCapability,
    UnsupportedDynamicStream,
    RestFetchFailed,
    DecodeFailed,
    TransportDisconnected,
}
```

Meanings:

- `UnsupportedCapability`: the requested exchange/instrument/sub_kind combination is not declared by descriptor or existing support matrix.
- `UnsupportedDynamicStream`: the data surface is supported, but not via `DynamicStreams`, usually because it is REST-only.
- `RestFetchFailed`: REST request construction, HTTP status, transport, timeout, or response retrieval failed.
- `DecodeFailed`: provider response was received but could not be parsed or normalized into the expected Barter type.
- `TransportDisconnected`: a WebSocket stream disconnected or terminated after initialization.

V0 should include these categories in docs and descriptor-facing helper code only if it does not require widespread error rewrites. Existing `DataError` remains the implementation error type.

## Consistency Tests

Add tests that protect the descriptor from diverging from existing behavior.

Required tests:

1. Binance USD-M Futures descriptor exists.
2. Descriptor declares `Perpetual` and does not imply Spot support.
3. For every descriptor capability with a `SubKind`, `exchange_supports_instrument_kind_sub_kind(BinanceFuturesUsd, Perpetual, sub_kind)` returns true.
4. Known REST-only surfaces are declared as `dynamic_stream = Unsupported`:
   - Funding rates
   - Open interest
   - Taker flow
5. Known WebSocket surfaces are declared as `dynamic_stream = Supported`:
   - Public trades
   - L1
   - L2
   - Candles
   - Liquidations
   - Mark price
   - Index price
6. Mark price and index price include both `WebSocket` and `RestFetch`.
7. REST-only surfaces do not include `WebSocket`.
8. Descriptor helper `supports_capability` agrees with descriptor contents for Binance USD-M Futures.

Optional tests if low-cost:

- Verify every Binance USD-M Futures support-matrix `SubKind` has a descriptor entry.
- Verify reference discovery appears only in `reference_capabilities` and does not require a synthetic `SubKind`.

## Migration Path

V0 is declarative. Later increments can:

1. Generate support-matrix behavior from descriptors.
2. Generate MDB bridge source registration from descriptors.
3. Add serialization for descriptor export.
4. Add per-surface polling interval hints, rate-limit hints, and required request parameters.
5. Add richer history semantics such as latest, recent window, time range, and pagination.
6. Add descriptors for other exchanges.

## Implementation Boundaries

The implementation should be split into small tasks:

1. Add capability types, reference capability types, and Binance USD-M Futures descriptor with compile-only tests.
2. Add support-matrix consistency tests.
3. Add DynamicStreams boundary consistency tests or descriptor tests that mirror the existing boundary.
4. Add lifecycle/error contract V0 docs and optional type definitions.
5. Run focused tests, full `barter-data` checks, format check, diff check, commit, and two-stage review.

Each task should follow TDD where practical and should not modify runtime behavior unless a test exposes a descriptor consistency bug.

## Acceptance Criteria

- `barter-data` exports a provider capability descriptor API.
- Binance USD-M Futures capabilities are machine-readable and cover all current supported market-data surfaces relevant to MDB bridge planning.
- Descriptor marks REST-only surfaces as not eligible for `DynamicStreams`.
- Descriptor marks dual REST/WS surfaces correctly.
- Consistency tests pass against existing support matrix behavior.
- Existing runtime subscription validation behavior is unchanged.
- Existing dynamic stream behavior is unchanged.
- No MDB bridge code is added in this increment.
- Design and implementation are committed in small steps.
