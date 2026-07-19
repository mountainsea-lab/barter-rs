# Binance USD-M Futures Funding Rates Follow-up Design

## Status

Approved for design direction by user on 2026-07-19.

## Context

`barter-data` already contains the first Funding Rates increment:

- `subscription::funding::{FundingRates, FundingRate}` exists.
- `event::DataKind` and `MarketEvent` already support `FundingRate`.
- `exchange::binance::futures::funding` contains a Binance USD-M Futures REST URL builder, raw REST schema, conversion tests, and async fetcher.
- The original design document at `docs/superpowers/specs/2026-07-18-binance-futures-funding-rate-design.md` intentionally avoided `DynamicStreams` because the first increment was REST fetch-first.

Since then, Mark Prices have been fully wired as a first-class subscription kind across `SubKind`, support matrix, `DynamicStreams`, examples, and verification. Funding Rates should now be brought up to the same public API completeness where it is architecturally valid.

## Goal

Complete Binance USD-M Futures Funding Rates as a first-class Barter data kind without misrepresenting Binance protocol semantics.

The follow-up should:

1. Preserve the existing REST fetcher as the canonical way to retrieve actual funding rate history/latest records.
2. Add missing `SubKind::FundingRates` support so dynamic subscription configuration can express funding rates consistently.
3. Wire support matrix and dynamic channels/selectors where the code can safely carry typed `FundingRate` events.
4. Add examples and tests that make REST versus live-stream semantics explicit.

## Non-goals

- Do not invent a Binance WebSocket funding rate stream if Binance does not expose one as a distinct funding-rate event.
- Do not derive `FundingRate` directly from Mark Price stream fields in this increment. Mark Price updates expose `lastFundingRate`, but that value is not the same event model as a settled or scheduled funding rate history row.
- Do not build a generic REST polling runtime yet.
- Do not require live Binance HTTP or WebSocket calls in automated tests.
- Do not refactor unrelated stream builder internals except where needed to add the new subscription kind.

## Recommended Approach

Use a hybrid REST-first, dynamic-config-aware design.

REST remains the source for Funding Rate records. Dynamic configuration should be able to allocate and route `SubKind::FundingRates`, but only a real source should feed those channels. If the existing `DynamicStreams` path only supports WebSocket stream selectors, the implementation must either:

- add only the `SubKind` and support-matrix pieces now and keep dynamic stream selection unsupported with a clear error, or
- add a small source abstraction that can plug the existing REST fetcher into dynamic selection without pretending it is WebSocket.

The implementation plan must verify which path fits existing types before coding. The preferred first step is to add expressibility and REST examples, then only add dynamic runtime forwarding if it can be done cleanly and honestly.

## Components

### `subscription::SubKind`

Add `FundingRates` to `SubKind`.

Requirements:

- `serde(rename_all = "snake_case")` must serialize it as `"funding_rates"`.
- The existing SubKind serde regression test should include `FundingRates`.
- `Display` should remain consistent with other variants through derive behavior where currently used.

### Support Matrix

Allow `SubKind::FundingRates` for:

- `ExchangeId::BinanceFuturesUsd`
- `MarketDataInstrumentKind::Perpetual`

Reject it for unsupported markets such as Binance Spot.

### REST Fetcher

Keep `BinanceFuturesUsdFundingRateFetcher` as the canonical source for:

- latest funding rate per subscribed instrument using `limit = 1`
- historical funding rows with optional `startTime`, `endTime`, and `limit`

Follow-up quality improvements are allowed if small:

- call `error_for_status()` before JSON decoding so HTTP errors do not appear as confusing decode errors
- add tests around URL construction and raw schema only, not live HTTP

### Dynamic Configuration and Streams

The follow-up should make dynamic subscription configuration understand `FundingRates`.

Implementation must choose based on existing architecture:

1. **Expressibility only**: `Channels::try_from` can validate or reject `FundingRates` with an explicit unsupported-runtime error if no REST source abstraction exists.
2. **Full dynamic REST source**: add funding-rate channel maps and selectors, and feed them from the REST fetcher through a clearly named REST path.

Do not map `FundingRates` to Binance `@markPrice@1s` WebSocket events. That stream belongs to `MarkPrices`.

### Examples

Add or update examples so users can retrieve Funding Rates directly:

- `barter-data/examples/binance_futures_funding.rs` for REST latest or recent historical fetch.
- If dynamic REST selection is implemented, add a separate example that demonstrates it and clearly names it as REST-backed.

### Tests

Automated validation must cover:

1. `FundingRates::as_str()` and display behavior.
2. `SubKind::FundingRates` serde as `"funding_rates"`.
3. `exchange_supports_instrument_kind_sub_kind` accepts Binance USD-M perpetual funding rates.
4. The same support matrix rejects Binance Spot funding rates.
5. REST URL construction remains deterministic.
6. REST raw fixture parsing preserves decimal precision and timestamps.
7. If dynamic channels are added, `Channels::try_from` allocates funding-rate channels for Binance USD-M perpetual subscriptions.
8. If dynamic selectors are added, selector tests prove `FundingRate` streams can be selected without conflating them with `MarkPrice` streams.

## Risks and Mitigations

### Risk: Confusing mark price funding fields with funding rate events

Mitigation: keep `MarkPrice.last_funding_rate` and `FundingRate.funding_rate` as separate models. Do not implement a WebSocket `FundingRates` selector using `@markPrice@1s` unless a future design explicitly introduces derived events.

### Risk: Overbuilding REST streaming

Mitigation: implement only what current code boundaries can support cleanly. If a generic REST polling runtime is needed, write a separate design after this follow-up.

### Risk: Config compatibility

Mitigation: extend the existing `SubKind` snake_case serde test with `FundingRates` before implementation.

### Risk: Network-dependent tests

Mitigation: keep automated tests fixture-based and URL-based. Live REST examples are manual checks only.

## Acceptance Criteria

The follow-up implementation is complete when:

- `SubKind::FundingRates` exists and serializes as `"funding_rates"`.
- Binance USD-M perpetual funding-rate support is represented in the support matrix.
- Unsupported market combinations are rejected by tests.
- Existing REST funding fetcher remains working and documented by an example.
- Any dynamic wiring added is honest about REST versus WebSocket semantics.
- Automated verification passes:
  - `cargo fmt --all -- --check`
  - `cargo test -p barter-data -j1`
  - `cargo check -p barter-data -j1`
  - `cargo check -p barter-data --examples -j1`
  - `git diff --check`
- Code review has no unresolved Critical or Important issues.
