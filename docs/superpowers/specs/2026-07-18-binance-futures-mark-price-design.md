# Binance USD-M Futures Mark Price Design

## Status

Approved for design by user on 2026-07-18. User selected transport scope C: implement both REST fetch support and WebSocket real-time stream support.

## Context

The project is extending `barter-data` as the external data collection layer for Binance USD-M Futures. The previous roadmap items are complete:

- Binance USD-M Futures candles are wired through the existing WebSocket stream architecture.
- Binance USD-M Futures funding rates are implemented as REST fetch-first data and intentionally not forced into `DynamicStreams`.

Current relevant architecture:

- `subscription::*` contains typed market data kinds and normalized event models.
- `event::DataKind` is the generic enum used by combined downstream market events.
- `StreamSelector` and `DynamicStreams` are the existing path for live WebSocket subscriptions.
- Binance Futures REST fetchers can live under `exchange::binance::futures::*` without changing stream infrastructure.
- New precision-sensitive financial fields should use `rust_decimal::Decimal`.

Mark price differs from funding rate because Binance exposes both a REST snapshot endpoint and a WebSocket mark price stream. It should therefore use both paths, but each path should remain cleanly separated:

- REST fetcher for current snapshot and manual fetch.
- WebSocket stream for continuous real-time collection through existing stream abstractions.

## Goal

Implement Binance USD-M Futures mark price collection with one normalized Barter model and two provider collection paths:

1. REST current mark price fetch from `/fapi/v1/premiumIndex`.
2. WebSocket real-time mark price stream using `{symbol}@markPrice@1s`.

The design should make mark price consumable through typed `MarketEvent<InstrumentKey, MarkPrice>` and generic `DataKind::MarkPrice`, while preserving the existing FundingRates implementation as the source of historical funding rate data.

## Non-goals

- Do not replace the already implemented funding rate REST fetcher.
- Do not treat mark price payload funding fields as historical `FundingRate` events.
- Do not implement index price as a separate subscription in this task, even though Binance mark price payload includes `indexPrice`.
- Do not implement all-market mark price streams in the first increment.
- Do not add a generic REST polling scheduler in this task.
- Do not require live Binance HTTP or WebSocket calls for automated CI-style validation.
- Do not refactor unrelated existing `f64` models.

## Architecture Decision

Use a dual-path design with shared normalized output.

Mark price support should be modeled as:

1. A normalized subscription/data module: `subscription::mark_price`.
2. `DataKind::MarkPrice` and conversions for generic downstream handling.
3. Binance raw REST response parsing and REST fetcher under `exchange::binance::futures::mark_price`.
4. Binance raw WebSocket payload parsing, `Identifier<Option<SubscriptionId>>`, and normalized conversion under the same module.
5. `StreamSelector<Instrument, MarkPrices>` for `BinanceFuturesUsd` using `{symbol}@markPrice@1s`.
6. Support matrix and `DynamicStreams` wiring because WebSocket mark price is streamable.
7. Fixture-based tests for raw parsing, subscription id mapping, conversion, URL construction, support matrix, and stream compile coverage.
8. Examples for REST fetch and WebSocket stream use.

This follows existing boundaries:

- Provider-specific raw schemas stay under `exchange/binance/futures`.
- Normalized Barter output stays under `subscription` and `event`.
- REST fetch is explicit and does not imply a generic polling runtime.
- WebSocket stream support uses existing stream wiring because mark price is natively streamable.

## Data Model

Add `barter-data/src/subscription/mark_price.rs`:

```rust
pub struct MarkPrices;

pub struct MarkPrice {
    pub event_time: DateTime<Utc>,
    pub mark_price: Decimal,
    pub index_price: Decimal,
    pub estimated_settle_price: Option<Decimal>,
    pub last_funding_rate: Option<Decimal>,
    pub interest_rate: Option<Decimal>,
    pub next_funding_time: Option<DateTime<Utc>>,
}
```

`MarkPrices` implements `SubscriptionKind<Event = MarkPrice>`.

Field semantics:

- `event_time`: provider event/current time for the mark price snapshot or stream update.
- `mark_price`: Binance mark price.
- `index_price`: Binance index price included in mark price payloads. This is included for context but does not replace a future dedicated `IndexPrice` subscription.
- `estimated_settle_price`: optional because Binance describes it as only useful around settlement windows.
- `last_funding_rate`: optional latest funding rate included by Binance mark price payloads. This is context only and does not replace `FundingRate` history.
- `interest_rate`: optional interest rate field from Binance.
- `next_funding_time`: optional timestamp for the next funding event.

All numeric fields use `Decimal`. Binance sends these fields as strings, so deserialization must parse directly from strings and avoid intermediate `f64`.

## Event Integration

Add `DataKind::MarkPrice(MarkPrice)` because mark price has a live WebSocket path and should be consumable through the same generic event path as trades, books, candles, liquidations, and funding rate.

Add conversions:

- `From<MarketEvent<InstrumentKey, MarkPrice>> for MarketEvent<InstrumentKey, DataKind>`.
- `From<MarketStreamResult<InstrumentKey, MarkPrice>> for MarketStreamResult<InstrumentKey, DataKind>`.
- `DataKind::as_mark_price()` helper consistent with existing `as_*` helpers.
- `DataKind::kind_name()` returns `"mark_price"`.

## Binance REST API Scope

Use Binance USD-M Futures REST endpoint:

- Base URL: `https://fapi.binance.com`
- Endpoint: `/fapi/v1/premiumIndex`
- Method: `GET`

Supported first-increment query parameters:

- `symbol`: required for per-instrument fetches, derived from the subscription identifier such as `BTCUSDT`.

The Binance endpoint can return all symbols when `symbol` is omitted, but the first increment should use explicit per-symbol requests. This keeps mapping to `Instrument::Key` unambiguous and avoids higher request weight.

REST fetcher behavior:

1. `fetch_latest` for one or more mark price subscriptions.
2. Build one URL per subscription with a required symbol.
3. Parse Binance response into raw rows.
4. Return normalized `Vec<MarketEvent<Instrument::Key, MarkPrice>>`.
5. Use provider `time` as both `event_time` and `time_exchange`.
6. Use local `Utc::now()` as `time_received`.

## Binance WebSocket API Scope

Use Binance USD-M Futures mark price stream:

- Stream: `{symbol}@markPrice@1s`
- Symbol form: lower-case stream id such as `btcusdt`.
- Update speed: 1 second.

The first increment should intentionally choose `@1s` rather than the default 3 second stream so the data is useful for live factor/risk calculations.

Do not implement all-market mark price stream in the first increment. Per-symbol subscriptions fit the existing `Subscription` and `StreamSelector` patterns and are simpler to validate.

Expected raw stream fields include:

- `e`: event type, expected `markPriceUpdate`.
- `E`: event time in epoch milliseconds.
- `s`: symbol.
- `p`: mark price.
- `i`: index price.
- `P`: estimated settle price.
- `r`: funding rate.
- `T`: next funding time in epoch milliseconds.

If Binance includes an interest rate field in REST but not in WebSocket payloads, WebSocket `interest_rate` should be `None` unless official fixture evidence proves a stream field exists.

## Components

### `subscription::mark_price`

Responsibilities:

- Define `MarkPrices` marker type.
- Define normalized `MarkPrice`.
- Implement `SubscriptionKind` and display behavior consistent with existing modules.

Dependencies:

- `chrono`
- `rust_decimal`
- existing `SubscriptionKind`

### `event`

Responsibilities:

- Add `DataKind::MarkPrice`.
- Add helper and conversion implementations.
- Keep generic downstream event behavior consistent with funding and candle additions.

### `exchange::binance::futures::mark_price`

Responsibilities:

- Define raw Binance REST mark price response struct.
- Define raw Binance WebSocket mark price payload struct.
- Parse Binance decimal strings into `Decimal`.
- Parse epoch milliseconds into `DateTime<Utc>`.
- Build REST mark price URLs from symbols.
- Provide `BinanceFuturesUsdMarkPriceFetcher` for REST latest snapshot fetches.
- Implement `Identifier<Option<SubscriptionId>>` for WebSocket raw payloads.
- Convert raw REST and WebSocket payloads into `MarketEvent<InstrumentKey, MarkPrice>`.

Dependencies:

- `reqwest`
- `serde`
- `chrono`
- `rust_decimal`
- `barter_instrument::ExchangeId`
- existing Binance `Identifier<BinanceMarket>` and stream parser patterns

### Binance Futures module wiring

Responsibilities:

- Export `mark_price` module.
- Implement `StreamSelector<Instrument, MarkPrices>` for `BinanceFuturesUsd`.
- Add support matrix entries for USD-M perpetual mark price streams.
- Add `DynamicStreams` channel storage and `select_all` integration for `MarkPrices`.

### Examples

Add two examples or one example with clearly separated modes:

1. REST example, e.g. `barter-data/examples/binance_futures_mark_price.rs`, fetches BTCUSDT mark price snapshot and logs normalized events.
2. WebSocket example, e.g. `barter-data/examples/binance_futures_mark_price_stream.rs`, subscribes to BTCUSDT mark price stream and logs real-time events.

Automated verification should compile examples but should not require live Binance network data.

## Error Handling

REST errors should reuse existing fetcher error boundaries where practical:

- Map transport and JSON decode failures into `SocketError::Http` if following the funding fetcher pattern.
- Keep URL construction deterministic and tested.
- Do not add a generic REST error hierarchy unless required by compiler/API boundaries.

WebSocket errors should follow existing stream behavior:

- Raw payload parse failures should become non-terminal or terminal consistently with existing Binance stream parsers.
- Event type mismatch should fail parsing or conversion in tests rather than silently producing invalid data.
- Missing optional fields should map to `None` only when Binance docs or fixtures show the field is legitimately absent.

## Validation Matrix

Automated validation must include:

1. `MarkPrices::as_str()` and display behavior test.
2. `DataKind::MarkPrice` kind name and `as_mark_price()` test.
3. Raw REST fixture deserializes correctly.
4. Raw WebSocket fixture deserializes correctly.
5. Decimal parsing preserves values such as `"11793.63104562"` and `"0.00010000"`.
6. Epoch millisecond timestamps convert into `DateTime<Utc>`.
7. REST URL construction includes required `symbol` correctly.
8. REST raw payload converts into `MarketEvent<InstrumentKey, MarkPrice>` with `ExchangeId::BinanceFuturesUsd`.
9. WebSocket raw payload maps to the expected `SubscriptionId`.
10. WebSocket raw payload converts into normalized `MarketEvent<InstrumentKey, MarkPrice>`.
11. `StreamSelector<Instrument, MarkPrices>` emits `{symbol}@markPrice@1s`.
12. Support matrix accepts Binance USD-M perpetual mark price subscriptions.
13. `DynamicStreams` compile coverage includes mark price streams.
14. REST and WebSocket examples compile.
15. `cargo fmt --check` passes.
16. `cargo test -p barter-data -j1` passes.
17. `cargo check -p barter-data --examples -j1` passes.
18. `cargo check -p barter-data -j1` passes.

Manual validation may include a short REST example run and a short WebSocket stream smoke run, but neither should be required for automated completion.

## Implementation Sequence

1. Add `subscription::mark_price` with tests.
2. Add `DataKind::MarkPrice` integration and tests.
3. Add Binance REST raw parser, URL builder, fetcher, conversion, and tests.
4. Add Binance WebSocket raw parser, subscription id mapping, conversion, and tests.
5. Wire `StreamSelector<Instrument, MarkPrices>` for `BinanceFuturesUsd`.
6. Add support matrix and `DynamicStreams` integration.
7. Add REST and WebSocket examples.
8. Run full automated verification.
9. Optionally run manual Binance REST/WS smoke checks.
10. Commit only green checkpoints.

## Risks and Mitigations

### Risk: Confusing mark price with index price or funding rate

Mitigation: model `index_price`, `last_funding_rate`, and `next_funding_time` as context fields on `MarkPrice`. Do not emit `IndexPrice` or `FundingRate` events from mark price payloads in this task.

### Risk: Overloading the first increment with all-market streams

Mitigation: implement per-symbol `{symbol}@markPrice@1s` only. Add all-market streams later if needed by ingestion throughput requirements.

### Risk: Precision loss

Mitigation: use `Decimal` for all new mark price numeric fields and parse Binance numeric strings directly.

### Risk: REST and WebSocket schema drift

Mitigation: define separate raw structs for REST and WebSocket payloads, then convert into one normalized `MarkPrice` model. Shared helper deserializers are fine, but raw provider schemas should remain distinct.

### Risk: Network-dependent validation

Mitigation: automated tests use fixtures, URL construction tests, support matrix tests, and example compile checks. Live Binance runs are manual smoke checks only.

## Acceptance Criteria

The task is complete when:

- Mark price has a normalized Barter data model.
- `DataKind::MarkPrice` exists and is tested.
- Binance USD-M REST mark price snapshots parse from fixtures and can be fetched manually.
- Binance USD-M WebSocket mark price updates parse from fixtures and can be subscribed to through existing stream APIs.
- `DynamicStreams` includes mark price because the data type is streamable.
- Support matrix accurately declares Binance USD-M perpetual mark price availability.
- REST and WebSocket examples compile and can be run manually to inspect provider data when network access is available.
- Full automated verification passes.
- Implementation is committed as green checkpoints.
