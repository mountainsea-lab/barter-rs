# Binance USD-M Futures Funding Rate Design

## Status

Approved for design by user on 2026-07-18.

## Context

The project is extending `barter-data` as the external data collection layer for Binance USD-M Futures. The previous roadmap item, Binance USD-M Futures candles, is complete and wired through the existing WebSocket stream architecture.

Current relevant architecture:

- `subscription::*` contains typed market data kinds such as trades, books, candles, and liquidations.
- `event::DataKind` is used for stream-combined WebSocket market events.
- `StreamSelector` and `DynamicStreams` are oriented around live WebSocket subscriptions.
- Binance Futures L2 uses REST only as an initial snapshot fetcher attached to a WebSocket stream.
- `reqwest` and `rust_decimal` are already available in `barter-data`.

Funding rate is different from candles, trades, books, and liquidations. Binance USD-M Futures funding rates are naturally exposed through REST history/latest endpoints. They should not be forced into the existing WebSocket-first `DynamicStreams` path before a dedicated REST polling/source abstraction exists.

## Goal

Implement Binance USD-M Futures funding rate collection as the next roadmap task in a way that fits the current architecture.

The first increment should provide a tested REST fetch capability for latest and historical funding rates, with normalized Barter data types and examples. It should avoid changing WebSocket stream abstractions unless the current task explicitly needs a stream.

## Non-goals

- Do not add funding rates to `DynamicStreams` in this increment.
- Do not add a generic REST polling runtime before at least one REST fetcher is proven.
- Do not add MDB ingestion bridge code in this repository task.
- Do not refactor existing `f64` market models unrelated to funding rates.
- Do not rely on live Binance HTTP calls in automated tests.

## Architecture Decision

Use a REST fetch-first design.

Funding rate support should be modeled as:

1. A normalized subscription/data module: `subscription::funding`.
2. Binance raw REST response structs and conversion logic under `exchange::binance::futures::funding`.
3. A small Binance Futures funding fetcher API that builds URLs, performs HTTP GET, parses provider payloads, and returns normalized `MarketEvent<InstrumentKey, FundingRate>` values.
4. Fixture-based unit tests for raw parsing, conversion, and URL construction.
5. A manually runnable example that fetches funding rates for a Binance USD-M perpetual instrument.

This follows existing project boundaries:

- Provider-specific raw schema stays under `exchange/binance/futures`.
- Barter-normalized output stays under `subscription` and `event`.
- REST transport is kept separate from `StreamSelector` and `DynamicStreams` because those are WebSocket stream constructs.

## Data Model

Add `barter-data/src/subscription/funding.rs`:

```rust
pub struct FundingRates;

pub struct FundingRate {
    pub funding_time: DateTime<Utc>,
    pub funding_rate: Decimal,
    pub mark_price: Option<Decimal>,
}
```

`FundingRates` implements `SubscriptionKind<Event = FundingRate>`.

Financial numeric fields use `Decimal` so the new data type does not expand existing `f64` precision debt. Binance REST returns numeric strings, so parsing should preserve decimal semantics.

## Event Integration

Add `DataKind::FundingRate(FundingRate)` only if the implementation plan confirms that non-WebSocket REST events are expected to participate in generic event handling now.

Preferred first increment:

- Add `FundingRate` as a typed normalized output.
- Do not add it to `DynamicStreams`.
- Consider `DataKind` addition optional and only useful if existing examples or bridge planning need one generic enum to hold REST and WebSocket results.

The implementation plan should choose explicitly between:

- **Typed-only:** `MarketEvent<InstrumentKey, FundingRate>` without `DataKind` wiring.
- **Typed plus enum:** add `DataKind::FundingRate` and conversions, while still not adding `DynamicStreams`.

Recommendation: typed plus enum is acceptable if the code change is small and improves downstream bridge planning, but `DynamicStreams` should remain unchanged.

## Binance REST API Scope

Use Binance USD-M Futures funding endpoints:

- Base URL: `https://fapi.binance.com`
- Endpoint: `/fapi/v1/fundingRate`

Supported query parameters for the first increment:

- `symbol`: required, derived from the instrument subscription identifier such as `BTCUSDT`.
- `limit`: optional, default small value for examples.
- `startTime`: optional.
- `endTime`: optional.

The fetcher should support:

1. Latest funding rate for one or more instruments by calling the endpoint with a small limit per symbol.
2. Historical funding rates for one instrument with optional time range and limit.

If Binance returns multiple entries for a symbol, preserve all entries in chronological provider order unless tests prove Binance returns reverse chronological order. Do not sort unless a requirement or API behavior requires it.

## Components

### `subscription::funding`

Responsibilities:

- Define `FundingRates` marker type.
- Define normalized `FundingRate`.
- Implement `SubscriptionKind` and display behavior consistent with existing modules.

Dependencies:

- `chrono`
- `rust_decimal`
- existing `SubscriptionKind`

### `exchange::binance::futures::funding`

Responsibilities:

- Define raw Binance funding response struct.
- Parse Binance string fields into `Decimal`.
- Convert raw funding rows into `MarketEvent<InstrumentKey, FundingRate>`.
- Build funding endpoint URLs from symbol/time-range/limit inputs.
- Provide an async fetcher API that uses `reqwest`.

Dependencies:

- `reqwest`
- `serde`
- `chrono`
- `rust_decimal`
- `barter_instrument::ExchangeId`
- existing `Identifier<BinanceMarket>` pattern where practical

### Example

Add a small example such as `barter-data/examples/binance_futures_funding.rs` that:

- Fetches recent funding data for BTCUSDT perpetual.
- Logs or prints normalized funding events.
- Is manually runnable.
- Does not become an automated live-data gate.

## Error Handling

REST errors should map into existing project error types where possible.

Preferred first increment:

- Reuse `SocketError::Http` if the fetcher is implemented near existing `SnapshotFetcher` patterns.
- If this becomes awkward, add a focused `DataError` or fetcher error variant only after checking existing error boundaries.

Automated tests should not require network access. HTTP behavior can be covered by URL construction tests and fixture parser tests. Live Binance HTTP verification may be done manually after automated checks pass.

## Validation Matrix

Automated validation must include:

1. `FundingRates::as_str()` and display behavior test.
2. Raw Binance funding fixture deserializes correctly.
3. Decimal parsing preserves rates such as `"0.00010000"`.
4. Funding timestamp converts from epoch milliseconds into `DateTime<Utc>`.
5. Raw payload converts into `MarketEvent<InstrumentKey, FundingRate>` with `ExchangeId::BinanceFuturesUsd`.
6. URL construction includes required `symbol` and optional `limit`, `startTime`, `endTime` parameters correctly.
7. `cargo fmt --check` passes.
8. `cargo test -p barter-data -j1` passes.
9. `cargo check --workspace -j1` passes.
10. `cargo check -p barter-data --examples -j1` passes.

Manual validation may include a short Binance REST example run, but this must not be required for CI-style completion.

## Implementation Sequence

1. Add `subscription::funding` with tests.
2. Add Binance raw funding fixture parser and Decimal conversion tests.
3. Add normalized conversion into `MarketEvent<InstrumentKey, FundingRate>`.
4. Add URL builder and fetcher API.
5. Decide and implement `DataKind::FundingRate` only if useful and low-risk.
6. Add example.
7. Run full verification.
8. Commit only after all automated checks pass.

## Risks and Mitigations

### Risk: Forcing REST data into WebSocket stream architecture

Mitigation: keep the first increment REST fetch-first and explicitly exclude `DynamicStreams` wiring.

### Risk: Precision loss

Mitigation: use `Decimal` for new funding numeric fields and parse Binance numeric strings directly.

### Risk: Unclear latest versus historical semantics

Mitigation: expose endpoint parameters explicitly. Use `limit = 1` for latest helper behavior and a separate range-capable fetch path for historical data.

### Risk: Overbuilding a generic REST provider framework

Mitigation: implement a small Binance-specific fetcher now. Extract a generic REST source only after funding, open interest, mark price, and index price reveal shared patterns.

## Acceptance Criteria

The task is complete when:

- Funding rate has a normalized Barter data model.
- Binance USD-M raw funding REST payloads parse from fixtures.
- Conversion to normalized `MarketEvent` is covered by tests.
- REST URL construction is deterministic and tested.
- A manually runnable example exists.
- Full automated verification passes.
- The implementation is committed as a green checkpoint.
