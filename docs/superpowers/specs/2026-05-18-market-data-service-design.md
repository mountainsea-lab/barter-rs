# Multi-Exchange Market Data Service Design

Date: 2026-05-18
Project: barter-rs
Status: Draft for review

## 1. Intent

Add a unified data access layer on top of the existing Barter crates to support:

1. Multi-exchange realtime market data subscriptions.
2. Multi-exchange historical market data retrieval, starting with candles/Klines.
3. A normalized output model compatible with existing `barter-data`, `barter`, and backtest code.

The first implementation should be useful for research, backtesting data preparation, and live data ingestion without turning the project into a separate data platform.

## 2. Current Capabilities

The project already has strong realtime data foundations:

- `barter-data::streams::builder::dynamic::DynamicStreams` can initialize dynamic multi-exchange streams.
- `barter-data::streams::builder::dynamic::indexed::init_indexed_multi_exchange_market_stream` can produce indexed market streams from `IndexedInstruments`.
- `barter-data::event::MarketEvent` and `DataKind` provide normalized output models.
- `barter-data::subscription::SubKind` models supported realtime data kinds.
- `barter-integration::protocol::http::rest::RestClient` provides reusable REST infrastructure.
- `barter/src/backtest` already consumes finite historical-like market data streams.

Realtime supported combinations are currently defined by `exchange_supports_instrument_kind_sub_kind`. Public trades are broadly supported. L1/L2 order books are supported for selected exchanges. Liquidations are supported for Binance futures. `Candle` and `SubKind::Candles` exist as models, but dynamic realtime candle support is not currently wired through the support matrix.

Historical data does not yet have a unified abstraction. The project has generic REST primitives, but no `HistoricalDataClient`, no exchange-specific Kline request modules, and no common pagination/rate-limit layer.

## 3. Recommended Scope

Recommended approach: balanced MVP.

### In Scope

- Add a `MarketDataService` facade in `barter-data`.
- Wrap existing realtime stream initialization behind a simpler API.
- Add a historical data module focused on candles/Klines.
- Implement historical candles for Binance Spot and Binance Futures USD first.
- Normalize historical output to `MarketEvent<MarketDataInstrument, Candle>` and provide an indexed conversion to `MarketEvent<InstrumentIndex, DataKind>`.
- Add pagination over a time range with deterministic `[start, end)` semantics.
- Add tests for interval mapping, response normalization, pagination boundaries, and indexed conversion.

### Out of Scope for MVP

- Historical public trades.
- Historical order book reconstruction.
- Database storage or long-running data warehouse sync.
- Standalone microservice or daemon.
- Authenticated/private market data APIs.
- Full realtime candle support across all exchanges.
- Production-grade distributed rate limiting.

## 4. Public API Sketch

The service should expose two separate flows because realtime streams are unbounded and historical queries are finite.

```rust
pub struct MarketDataService {
    historical: HistoricalClientRegistry,
}

impl MarketDataService {
    pub async fn subscribe_realtime(
        &self,
        instruments: &IndexedInstruments,
        kinds: &[SubKind],
    ) -> Result<impl Stream<Item = MarketStreamEvent<InstrumentIndex, DataKind>>, DataError>;

    pub async fn fetch_historical_candles_indexed(
        &self,
        instruments: &IndexedInstruments,
        request: HistoricalCandleRequest,
    ) -> Result<Vec<MarketEvent<InstrumentIndex, DataKind>>, HistoricalDataError>;
}
```

The implementation may need concrete return types or boxed streams to satisfy Rust type constraints. The public intent is more important than this exact signature.

## 5. New Historical Types

```rust
pub struct HistoricalCandleRequest {
    pub exchange: ExchangeId,
    pub instrument: MarketDataInstrument,
    pub interval: CandleInterval,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub limit: Option<usize>,
}

pub enum CandleInterval {
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
    FourHours,
    OneDay,
}
```

The request time range uses `[start, end)`: include events at or after `start`, exclude events at or after `end`.

For MVP, candle events should use `Candle.close_time` as provided by the normalized model. If an exchange response includes both open and close time, adapters should preserve close time in `Candle.close_time` and use it consistently for sorting and deduplication.

## 6. Historical Client Trait

```rust
#[async_trait]
pub trait HistoricalDataClient {
    async fn fetch_candles(
        &self,
        request: HistoricalCandleRequest,
    ) -> Result<Vec<MarketEvent<MarketDataInstrument, Candle>>, HistoricalDataError>;
}
```

The first concrete implementations should be:

- `BinanceSpotHistoricalClient`
- `BinanceFuturesUsdHistoricalClient`

Both should use existing `barter-integration` REST abstractions where practical.

## 7. Module Layout

Proposed files:

```text
barter-data/src/service/
  mod.rs
  realtime.rs
  historical.rs
  config.rs
  error.rs

barter-data/src/historical/
  mod.rs
  client.rs
  request.rs
  interval.rs
  registry.rs
  pagination.rs
  binance/
    mod.rs
    spot.rs
    futures.rs
    model.rs
    request.rs
```

If this feels too many files during implementation, `service` and `historical` can be collapsed slightly, but exchange-specific REST request/response code should stay separate from generic historical orchestration.

## 8. Realtime Data Flow

```text
User request
  -> MarketDataService::subscribe_realtime
  -> init_indexed_multi_exchange_market_stream
  -> DynamicStreams
  -> exchange WebSocket connectors
  -> MarketStreamEvent<InstrumentIndex, DataKind>
```

This should be a thin wrapper around existing code. It should not duplicate WebSocket connector logic.

## 9. Historical Data Flow

```text
HistoricalCandleRequest
  -> MarketDataService
  -> HistoricalClientRegistry selects client by ExchangeId
  -> client splits [start, end) into REST pages
  -> RestClient executes exchange requests
  -> adapter converts raw response to Candle
  -> adapter wraps Candle in MarketEvent<MarketDataInstrument, Candle>
  -> service sorts, dedups, filters [start, end)
  -> service indexes instrument to InstrumentIndex
  -> service maps Candle into DataKind::Candle
```

Historical output should be deterministic:

- Sorted by event time.
- Deduplicated by `(exchange, instrument, close_time)`.
- Filtered to `[start, end)` after normalization.

## 10. Error Handling

Add a `HistoricalDataError` with variants similar to:

```rust
pub enum HistoricalDataError {
    Unsupported { exchange: ExchangeId, kind: &'static str },
    InvalidRequest(String),
    Http(SocketError),
    Parse(String),
    Index(IndexError),
    RateLimited { exchange: ExchangeId, retry_after: Option<Duration> },
}
```

The MVP does not need advanced retry policy, but errors should preserve enough detail for callers to distinguish unsupported requests, network errors, parse errors, and index errors.

## 11. Pagination and Rate Limits

For Binance candles:

- Use exchange max limit when `request.limit` is `None`.
- Split long ranges into pages using interval duration and limit.
- Move the next page start to the next interval boundary after the last returned candle.
- Stop when the next page start is `>= end` or a page returns no data.

MVP rate limiting:

- Sequential requests per exchange client.
- Optional fixed small delay between pages if needed.
- No global distributed limiter.

## 12. Testing Strategy

### Unit Tests

- `CandleInterval` to Binance interval string mapping.
- Binance raw Kline model to normalized `Candle` conversion.
- Pagination range splitting for `[start, end)`.
- Sort/dedup/filter behavior.
- `MarketDataInstrument` to `InstrumentIndex` conversion using `IndexedInstruments`.
- Unsupported exchange returns `HistoricalDataError::Unsupported`.

### Integration-style Tests Without Network

Use fixture JSON for Binance Kline responses. Avoid live HTTP tests by default so CI remains deterministic.

### Compile Validation

Expected baseline command:

```bash
cargo test -p barter-data --lib
```

Optional broader validation after example issues are fixed:

```bash
cargo test --workspace --lib --no-run
```

Known current issue: `cargo test --workspace --no-run` fails on `barter-integration/examples/simple_websocket_integration.rs` because the example passes an unboxed WebSocket stream where `Box<dyn WebSocketStreamExt>` is expected.

## 13. Compatibility Notes

- The design should not change existing `DynamicStreams` behavior.
- The design should not change `Engine` event handling.
- Historical data should be additive and should not be required by existing realtime examples.
- Existing `Candle` model should be reused rather than duplicated.
- Existing `RestClient` should be reused unless it blocks a simple MVP implementation.

## 14. Future Extensions

After MVP:

1. Add OKX and Bybit historical candles.
2. Add realtime candle support to `DynamicStreams` and support matrix.
3. Add local cache readers/writers, likely JSONL first, then Parquet if needed.
4. Add historical public trades.
5. Add a backtest data source that reads historical REST/cache output directly.
6. Add metrics for request latency, rate-limit events, page counts, and record counts.

## 15. Acceptance Criteria

- A caller can subscribe to existing multi-exchange realtime data through `MarketDataService` without directly using `DynamicStreams`.
- A caller can request Binance historical candles for a time range and receive normalized `MarketEvent<MarketDataInstrument, Candle>` values.
- A caller can convert historical candle events to indexed `MarketEvent<InstrumentIndex, DataKind>` using `IndexedInstruments`.
- Historical results are sorted, deduplicated, and filtered to `[start, end)`.
- Unsupported historical exchange/kind combinations return a clear error.
- Unit tests cover interval mapping, normalization, pagination, dedup/filter, and indexing.
- `cargo test -p barter-data --lib` passes.
