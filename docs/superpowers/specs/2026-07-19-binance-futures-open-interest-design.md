# Binance USD-M Futures Open Interest Design

Date: 2026-07-19

## Summary

Add first-class Open Interest support for Binance USD-M Futures as a REST-first data type. The first increment fetches the latest open interest from `GET /fapi/v1/openInterest` for perpetual instruments. It does not implement historical open interest, notional open interest, WebSocket streaming, or a generic REST polling runtime.

## Goals

- Add a normalized `OpenInterest` event model and `OpenInterests` subscription marker.
- Add Binance USD-M Futures REST fetch support for the latest open interest per symbol.
- Make quantity and notional semantics explicit.
- Add support matrix entries so `BinanceFuturesUsd + Perpetual + OpenInterests` is supported.
- Make `DynamicStreams` reject `OpenInterests` at runtime until a REST source abstraction exists.
- Add fixtures, tests, and an example.

## Non-Goals

- Do not implement `/futures/data/openInterestHist` in this increment.
- Do not expose notional or value fields in the first normalized model.
- Do not add WebSocket support. Binance USD-M latest open interest is a REST endpoint in this scope.
- Do not create a generic polling scheduler or force REST-only data into the WebSocket-oriented `DynamicStreams` path.
- Do not implement reference discovery or `exchangeInfo` as part of this task.

## API Scope

Use the Binance USD-M Futures endpoint:

```text
GET https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT
```

Expected response shape:

```json
{
  "openInterest": "10659.509",
  "symbol": "BTCUSDT",
  "time": 1589437530011
}
```

## Normalized Model

Add `barter-data/src/subscription/open_interest.rs`:

```rust
pub struct OpenInterests;

pub struct OpenInterest {
    pub event_time: DateTime<Utc>,
    pub open_interest: Decimal,
}
```

Semantics:

- `open_interest` maps directly from Binance `openInterest`.
- The unit is the open contract quantity reported by Binance for the requested futures symbol. For USD-M perpetual contracts this should be treated as a base-asset quantity, not quote notional.
- `event_time` maps from Binance `time`.
- The model intentionally does not include notional, value, mark price, or quote currency value in this increment.
- If a later task adds `/futures/data/openInterestHist`, it can add a separate historical model or extend the model with optional notional fields after the endpoint semantics are tested.

`OpenInterests::as_str()` should return `open_interests`.

## Barter Integration

Add the following wiring:

- Export `subscription::open_interest`.
- Add `SubKind::OpenInterests` with serde string `open_interests`.
- Add `DataKind::OpenInterest(OpenInterest)`.
- Add `From<MarketEvent<InstrumentKey, OpenInterest>> for DataKind` if required by existing event conversion patterns.
- Update the support matrix so only `BinanceFuturesUsd` perpetual instruments support `OpenInterests`.
- Spot and unsupported instrument kinds should reject `OpenInterests` consistently with current funding-rate support behavior.

## Binance REST Fetcher

Add `barter-data/src/exchange/binance/futures/open_interest.rs`.

Core pieces:

- `HTTP_OPEN_INTEREST_URL_BINANCE_FUTURES_USD` constant.
- `open_interest_url(symbol: &str) -> String`.
- Raw response struct `BinanceFuturesOpenInterest` with:
  - `symbol: String`
  - `open_interest: Decimal`, deserialized from `openInterest` string.
  - `time: DateTime<Utc>`, deserialized from epoch milliseconds.
- `BinanceFuturesUsdOpenInterestFetcher::fetch_latest` accepting `Subscription<BinanceFuturesUsd, Instrument, OpenInterests>` values.
- A helper such as `fetch_open_interest_url(url: String)` so HTTP status error behavior can be tested with a local server.

HTTP behavior:

- Call `error_for_status()` before JSON decoding.
- Surface reqwest transport, non-2xx status, and JSON decoding failures through `SocketError::Http`, matching the funding/index price REST patterns.
- Fetch multiple subscriptions with `try_join_all`, returning a flat `Vec<MarketEvent<Instrument::Key, OpenInterest>>`.

## DynamicStreams Boundary

`OpenInterests` remains REST-first. Until the codebase has a REST polling/source abstraction, `DynamicStreams` must reject it explicitly.

Expected behavior:

- `Channels::try_from` returns `DataError::UnsupportedSubKind(SubKind::OpenInterests)` for `OpenInterests` batches.
- No `DynamicStreams` storage field, select method, or WebSocket channel selector is added.
- This mirrors the current `FundingRates` boundary and avoids pretending REST data is a live WebSocket stream.

## Example

Add `barter-data/examples/binance_futures_open_interest.rs`.

The example should:

- Create one or more Binance USD-M perpetual subscriptions.
- Call `BinanceFuturesUsdOpenInterestFetcher::fetch_latest`.
- Print each market event or normalized open interest result.
- Avoid requiring credentials.

## Testing Strategy

Follow TDD with small focused tests.

Required test coverage:

1. `OpenInterests` marker formats as `open_interests`.
2. `OpenInterest` stores `DateTime<Utc>` and `Decimal` accurately.
3. `SubKind::OpenInterests` serde round-trips with `open_interests`.
4. Support matrix accepts `BinanceFuturesUsd + Perpetual + OpenInterests`.
5. Support matrix rejects unsupported exchange or instrument-kind combinations.
6. `open_interest_url("BTCUSDT")` produces the Binance REST URL.
7. Raw fixture parses `openInterest` string decimal and `time` epoch milliseconds.
8. Raw-to-normalized conversion preserves exchange, instrument, event time, and open interest decimal.
9. `fetch_open_interest_url` surfaces non-2xx HTTP status before JSON decoding, using a local HTTP server that returns an error status with invalid JSON.
10. `DynamicStreams` channel allocation rejects `OpenInterests` until REST polling exists.
11. The example compiles under `cargo check --examples`.

## Verification

Use disk-conscious verification commands:

```bash
rtk cargo fmt --all -- --check
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest rtk cargo test -p barter-data open_interest -- --nocapture
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest rtk cargo check -p barter-data --examples
rtk git diff --check
```

Before claiming the implementation is complete, also run the broader package test suite if time and disk allow:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-open-interest-full rtk cargo test -p barter-data -- --nocapture
```

## Future Extensions

- Add `/futures/data/openInterestHist` as a separate historical-data task.
- Decide whether historical open interest should use a separate normalized type or extend `OpenInterest` with optional notional fields.
- Add a generic REST source/polling abstraction shared by funding rates, open interest, and reference discovery.
- Add capability descriptors once multiple REST-first data types are stable.
