# Binance USD-M Futures Taker Flow Design

## Status

Draft approved for specification and implementation planning.

## Summary

Add Binance USD-M Futures taker buy/sell volume collection to `barter-data` as the first Layer 2 factor-enhancement dataset. The first increment is REST-first and uses Binance's public `/futures/data/takerlongshortRatio` endpoint to retrieve per-period taker buy volume, taker sell volume, and buy/sell ratio for one symbol.

This feature is intended for factor computation and strategy analysis. It does not implement real-time aggregated trade streaming or a downstream factor engine.

## Goals

- Add a normalized `TakerFlow` event model and `TakerFlows` subscription marker.
- Add `SubKind::TakerFlows` and `DataKind::TakerFlow` so downstream consumers can route this data by kind.
- Add Binance USD-M Futures support matrix entries for perpetual instruments.
- Add a Binance REST fetcher for `/futures/data/takerlongshortRatio`.
- Parse Binance raw rows into Decimal-based normalized data.
- Provide a no-credential example that fetches and prints recent taker flow rows.
- Add fixtures and tests for raw parsing, normalized conversion, URL construction, HTTP error handling, support matrix behavior, and example compilation.

## Non-Goals

- Do not implement Binance `@aggTrade` WebSocket support in this increment.
- Do not derive taker flow from public trade streams in this increment.
- Do not add `DynamicStreams` support, because this endpoint is REST-first.
- Do not implement historical pagination beyond one bounded request with `symbol`, `period`, and `limit`.
- Do not implement factor computation or strategy signals in `barter-data`.
- Do not add MDB bridge mappings in this increment.

## Source Endpoint

Use the Binance USD-M Futures global long/short account ratio family endpoint for taker buy/sell volume:

```text
GET https://fapi.binance.com/futures/data/takerlongshortRatio
```

Initial query parameters:

- `symbol`: required Binance market symbol, for example `BTCUSDT`.
- `period`: required interval string supported by Binance, for example `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`.
- `limit`: optional request limit, capped by Binance. The implementation should pass the caller-provided value without inventing pagination.

The first increment should not expose `startTime` or `endTime`. Those can be added later when a general historical REST polling/page abstraction exists.

## Normalized Model

Create `barter-data/src/subscription/taker_flow.rs`:

```rust
pub struct TakerFlows;

pub struct TakerFlow {
    pub period_start: DateTime<Utc>,
    pub buy_volume: Decimal,
    pub sell_volume: Decimal,
    pub buy_sell_ratio: Decimal,
}
```

Field semantics:

- `period_start` is Binance's row timestamp converted from epoch milliseconds to `DateTime<Utc>`.
- `buy_volume` is taker buy volume for the period.
- `sell_volume` is taker sell volume for the period.
- `buy_sell_ratio` is the ratio reported by Binance, not recomputed locally. Tests may verify fixture consistency, but implementation should preserve the source value.

Use `Decimal` for all numerical fields to avoid precision loss in factor inputs.

## Binance Raw Model and Fetcher

Create `barter-data/src/exchange/binance/futures/taker_flow.rs`.

Expected raw response shape is an array of rows similar to:

```json
[
  {
    "buySellRatio": "1.4342",
    "buyVol": "387.3300",
    "sellVol": "270.0700",
    "timestamp": 1585614900000
  }
]
```

Add:

- `HTTP_TAKER_LONG_SHORT_RATIO_URL_BINANCE_FUTURES_USD` constant.
- `taker_flow_url(symbol, period, limit)` URL builder.
- `BinanceFuturesTakerFlowRest` raw row struct.
- `impl From<BinanceFuturesTakerFlowRest> for TakerFlow`.
- `impl From<(ExchangeId, InstrumentKey, BinanceFuturesTakerFlowRest)> for MarketIter<InstrumentKey, TakerFlow>`.
- `BinanceFuturesUsdTakerFlowFetcher` with `fetch` and `fetch_recent` naming aligned to existing REST fetchers.

The fetcher should return `Vec<MarketEvent<Instrument::Key, TakerFlow>>`. Each Binance row becomes one market event with:

- `time_exchange = row.period_start`
- `time_received = Utc::now()`
- `exchange = ExchangeId::BinanceFuturesUsd`
- `instrument = subscription.instrument.key().clone()`
- `kind = TakerFlow::from(row)`

HTTP behavior should call `error_for_status()` before JSON decoding, matching the funding, index price, and open interest fetcher quality standard.

## REST-Only Boundary

`TakerFlows` is a normalized subscription kind, but it is not streamable through the current WebSocket-first `DynamicStreams` builder.

Implementation should:

- Add support matrix entries so `BinanceFuturesUsd + Perpetual + TakerFlows` is supported.
- Explicitly reject `DynamicStreams` requests for `TakerFlows` with a clear unsupported-channel error until a REST polling abstraction exists.
- Avoid adding fake WebSocket channel wiring.

## Example

Add `barter-data/examples/binance_futures_taker_flow.rs`.

The example should:

- Use no credentials.
- Fetch recent `BTCUSDT` taker flow rows for a short period such as `5m`.
- Print row count and the first few normalized rows.
- Make the REST-only boundary obvious through naming and comments.

## Testing Strategy

Implementation should follow TDD. Expected tests:

1. `TakerFlows.as_str()` and serde name are `taker_flows`.
2. `TakerFlow` preserves Decimal fields.
3. `SubKind::TakerFlows` serializes/deserializes and maps to the normalized event kind.
4. Binance USD-M perpetual support matrix accepts `TakerFlows`, and Spot rejects it.
5. `DynamicStreams` rejects `TakerFlows` clearly instead of attempting channel selection.
6. URL builder includes required `symbol`, `period`, and optional `limit` parameters.
7. Raw REST fixture parses Decimal fields and timestamp correctly.
8. Raw row converts to normalized `TakerFlow` and `MarketIter` correctly.
9. Fetcher surfaces non-2xx HTTP status before JSON decoding.
10. No-credential example compiles.

Use disk-sensitive verification during implementation:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow /Volumes/wdata/rust/.cargo/bin/cargo test -p barter-data taker_flow -- --nocapture
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-taker-flow /Volumes/wdata/rust/.cargo/bin/cargo check -p barter-data --example binance_futures_taker_flow
```

## Alternatives Considered

### A. REST-first taker buy/sell volume

This is the selected approach. It gives direct factor inputs with the smallest architectural change and matches existing REST-only handling for funding rates and open interest.

### B. Binance `@aggTrade` WebSocket

This would provide lower-latency trade aggregation and could support real-time taker flow derivation. It is a larger stream feature, requires channel wiring and DynamicStreams support, and still leaves downstream aggregation policy unspecified.

### C. Implement both REST taker flow and `@aggTrade`

This is the most complete data surface, but it mixes a bounded REST dataset with a live stream dataset in one task. It should be split after the REST-first version is stable.

## Acceptance Criteria

- The design is implemented as a small REST-first data source.
- Tests cover parser, conversion, URL builder, support matrix, REST error handling, DynamicStreams rejection, and example compilation.
- No credentials are required.
- No WebSocket channel is added for `TakerFlows`.
- Final implementation has focused verification, full `barter-data` verification, code review, and a clean git status.
