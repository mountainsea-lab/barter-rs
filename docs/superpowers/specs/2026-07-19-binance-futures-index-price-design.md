# Binance USD-M Futures Index Price Design

## Status

Approved for design by user on 2026-07-19. User selected approach A: expose Index Price as a first-class Barter data type while sourcing Binance USD-M Futures values from the `indexPrice` field that Binance already publishes in mark price REST and WebSocket payloads.

## Context

The project is extending `barter-data` as the external data collection layer for Binance USD-M Futures. The current roadmap in `docs/superpowers/specs/2026-07-15-market-data-extension-design.md` lists Layer 1 data types in this order:

1. Candles/bars.
2. Funding rate.
3. Mark price.
4. Index price.
5. Open interest.
6. Reference discovery.

Candles, funding rates, and mark price are already implemented. Index price is the next roadmap item.

Relevant existing architecture:

- `subscription::*` contains typed market data kinds and normalized event models.
- `event::DataKind` is the generic enum used by downstream market events.
- `StreamSelector` and `DynamicStreams` are the existing path for live WebSocket subscriptions.
- Binance Futures REST fetchers live under `exchange::binance::futures::*`.
- New precision-sensitive numeric financial fields should use `rust_decimal::Decimal`.
- The existing Binance USD-M Futures mark price support already parses REST `/fapi/v1/premiumIndex` and WebSocket `{symbol}@markPrice@1s` payloads, both of which include Binance's `indexPrice` field.

## Goal

Implement Binance USD-M Futures index price collection as a first-class data type with one normalized Barter model and two provider collection paths:

1. REST snapshot fetch from `/fapi/v1/premiumIndex`, extracting the `indexPrice` field.
2. WebSocket real-time updates from `{symbol}@markPrice@1s`, extracting the `i` field.

The output should be consumable as typed `MarketEvent<InstrumentKey, IndexPrice>` and generic `DataKind::IndexPrice`.

## Non-goals

- Do not implement Binance composite index constituents as this data type.
- Do not implement Binance multi-assets asset index streams in this task.
- Do not implement index price kline/candlestick streams in this first increment.
- Do not implement all-market mark price array streams in this first increment.
- Do not add a generic REST polling scheduler.
- Do not emit `MarkPrice` events from `IndexPrices` subscriptions, or `IndexPrice` events from `MarkPrices` subscriptions unless explicitly selected.
- Do not require live Binance HTTP or WebSocket calls for automated CI-style validation.

## Architecture Decision

Use a dual-path design with a dedicated normalized `IndexPrice` output, while reusing Binance's real provider payloads.

Binance USD-M Futures does not need a separate, invented WebSocket channel for index price. Binance already publishes the contract's index price in the mark price stream payload:

- Stream: `{symbol}@markPrice@1s`.
- Field: `i`, documented as index price.

It also publishes the same concept in the REST premium index endpoint:

- Endpoint: `/fapi/v1/premiumIndex`.
- Field: `indexPrice`.

Therefore `IndexPrices` should be streamable in `DynamicStreams`, but the transport mapping must be explicit and honest: the subscription uses Binance's mark price stream as the provider source and only emits the normalized index price field.

This differs from funding rates. Funding rate history has a canonical REST endpoint and no independent historical WebSocket funding stream, so it remains REST-first and rejected by `DynamicStreams`.

## Data Model

Add `barter-data/src/subscription/index_price.rs`:

```rust
pub struct IndexPrices;

pub struct IndexPrice {
    pub event_time: DateTime<Utc>,
    pub index_price: Decimal,
}
```

`IndexPrices` implements `SubscriptionKind<Event = IndexPrice>`.

Field semantics:

- `event_time`: provider event/current time for the index price snapshot or stream update.
- `index_price`: Binance USD-M Futures index price for the subscribed contract symbol.

All numeric fields use `Decimal`. Binance sends these values as strings, so deserialization and conversion should avoid intermediate `f64`.

## Event Integration

Add `DataKind::IndexPrice(IndexPrice)` because index price has a live WebSocket path and should be consumable through the same generic event path as trades, books, candles, liquidations, funding rates, and mark prices.

Add conversions and helpers consistent with existing patterns:

- `From<MarketEvent<InstrumentKey, IndexPrice>> for MarketEvent<InstrumentKey, DataKind>`.
- `From<MarketStreamResult<InstrumentKey, IndexPrice>> for MarketStreamResult<InstrumentKey, DataKind>`.
- `DataKind::as_index_price()`.
- `DataKind::kind_name()` returns `"index_price"`.

Add `SubKind::IndexPrices`. With the existing `#[serde(rename_all = "snake_case")]`, it should serialize as `"index_prices"`.

## Binance REST API Scope

Use Binance USD-M Futures REST endpoint:

- Base URL: `https://fapi.binance.com`.
- Endpoint: `/fapi/v1/premiumIndex`.
- Method: `GET`.

Supported first-increment query mode:

- Single symbol fetch using `symbol=BTCUSDT`.

The REST fetcher should return normalized `IndexPrice` data by parsing the existing premium index response shape and extracting:

- `time` or equivalent provider timestamp into `event_time`.
- `indexPrice` into `index_price`.

HTTP behavior should match the funding and mark price fetchers: use `error_for_status()` so non-2xx responses surface as request errors instead of later JSON parse errors.

## Binance WebSocket API Scope

Use Binance USD-M Futures WebSocket stream:

- Stream name: `{symbol}@markPrice@1s`.
- Source field for index price: `i`.
- Source field for event time: `E`.

The stream selector for `IndexPrices` should map Binance Futures instruments to the same provider stream name as mark price, but decode the payload into `IndexPrice` events.

This does not mean mark price and index price are the same Barter data type. They are two normalized outputs that happen to share a Binance provider payload.

## Support Matrix and DynamicStreams

Support matrix:

- `BinanceFuturesUsd` + perpetual instruments + `SubKind::IndexPrices`: supported.
- `BinanceFuturesUsd` + spot instruments + `SubKind::IndexPrices`: unsupported.
- Other exchanges remain unsupported unless they already have explicit implementation.

DynamicStreams:

- Add `index_prices` receiver storage.
- Add `select_index_prices` and `select_all_index_prices` helpers.
- Include `IndexPrices` in `select_all` only where the existing `select_all` semantics include all supported streamable kinds.
- Ensure `Channels::try_from` accepts `SubKind::IndexPrices` only when the support matrix says it is supported.

## Testing Strategy

Use TDD with focused tests before implementation.

Required tests:

1. Subscription/model tests:
   - `IndexPrices::as_str()` returns `"index_prices"`.
   - `SubKind::IndexPrices` serde round-trips as `"index_prices"`.
   - `IndexPrice` uses `Decimal` for price fields.

2. Event integration tests:
   - `DataKind::kind_name()` returns `"index_price"`.
   - `DataKind::as_index_price()` returns the expected typed event.
   - `MarketEvent<InstrumentKey, IndexPrice>` converts to `DataKind::IndexPrice`.

3. Binance REST tests:
   - REST URL construction for single symbol.
   - Fixture deserialization of premium index REST payload containing `indexPrice`.
   - Normalized conversion to `IndexPrice`.
   - Non-2xx HTTP status handling is covered structurally where practical, or by matching the fetcher pattern if direct HTTP mocking is not present.

4. Binance WebSocket tests:
   - Fixture deserialization of mark price stream payload containing `i`.
   - Subscription id extraction maps `btcusdt@markPrice@1s` style ids to `SubscriptionId`.
   - Normalized conversion emits `IndexPrice` with only `event_time` and `index_price`.

5. Stream wiring tests:
   - `StreamSelector<Instrument, IndexPrices>` compiles for `BinanceFuturesUsd`.
   - Support matrix accepts Binance USD-M perpetual index prices and rejects spot.
   - `Channels::try_from` accepts supported index price subscriptions.

6. Example check:
   - Add a REST example and WebSocket example, or one combined example if consistent with existing examples.
   - `cargo check -p barter-data --examples` must pass.

## Implementation Notes

Prefer a small amount of reuse with the existing mark price module, but keep the public Barter semantics explicit.

Acceptable implementation shape:

- Add `subscription::index_price` as the normalized data module.
- Add `exchange::binance::futures::index_price` for index-price-specific fetcher/conversion/tests.
- Reuse or mirror the existing Binance mark price raw REST and WS structs only if doing so keeps names and ownership clear.
- If reusing raw structs directly creates confusing APIs, define index-price-specific raw aliases or helper conversions in `index_price.rs`.

The most important invariant is that a user subscribing to `IndexPrices` receives only `IndexPrice` events, even though Binance's underlying stream is named `markPrice`.

## Risks and Mitigations

### Risk: Confusing index price with mark price

Mitigation: maintain separate normalized modules, `DataKind` variants, examples, and tests. Document that Binance's mark price stream is the provider transport for both outputs.

### Risk: Accidentally implementing composite index or asset index semantics

Mitigation: scope this task to the per-contract `indexPrice` field from premium index and mark price stream payloads. Leave composite index constituents and multi-assets asset index for future work.

### Risk: Duplicate WebSocket subscriptions if a user asks for both MarkPrices and IndexPrices

Mitigation: accept duplication in the first increment. Deduplicating shared provider streams would require a stream fan-out layer and is out of scope. This should be documented as a future optimization.

### Risk: Overbuilding REST polling

Mitigation: add explicit fetcher methods and examples only. Do not add a runtime polling scheduler until funding, open interest, and index price fetchers reveal common needs.

## Success Criteria

The implementation is complete when:

1. `IndexPrices` and `IndexPrice` exist as normalized subscription types.
2. `SubKind::IndexPrices` serializes as `"index_prices"`.
3. `DataKind::IndexPrice` and helper conversions exist.
4. Binance USD-M REST fetch can return normalized `IndexPrice` from `/fapi/v1/premiumIndex`.
5. Binance USD-M WebSocket stream can return normalized `IndexPrice` from `{symbol}@markPrice@1s`.
6. Support matrix and `DynamicStreams` allow Binance USD-M perpetual `IndexPrices` and reject unsupported markets.
7. Fixture tests cover REST parsing, WS parsing, normalized conversion, support matrix, and stream wiring.
8. Examples compile.
9. Full `barter-data` verification passes without requiring live network access.
