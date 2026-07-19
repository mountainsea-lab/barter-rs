# Binance USD-M Futures Reference Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Binance USD-M Futures `exchangeInfo` reference discovery to `barter-data`, exposing raw Binance metadata and normalized Barter instruments/specs for tradeable perpetual contracts.

**Architecture:** Implement a focused REST module at `barter-data/src/exchange/binance/futures/instrument.rs`. The module owns Binance raw response types, filter parsing, raw-to-normalized conversion, and a small fetcher helper. It reuses existing `barter_instrument::Instrument`, `InstrumentKind::Perpetual`, and `InstrumentSpec` rather than creating a new global registry.

**Tech Stack:** Rust 2024, `reqwest`, `serde`, `rust_decimal`, `chrono`, `barter-instrument`, `barter-integration::error::SocketError`, existing `barter-data` Binance futures module patterns.

---

## File Structure

- Create: `barter-data/src/exchange/binance/futures/instrument.rs`
  - REST URL constant.
  - Raw `exchangeInfo` response structs.
  - Filter enum and filter accessors.
  - Conversion error type.
  - Normalized Barter `Instrument<ExchangeId, AssetNameInternal>` conversion helpers.
  - REST fetcher and test-only/local-URL helper.
  - Unit tests with inline fixtures.
- Modify: `barter-data/src/exchange/binance/futures/mod.rs`
  - Export the new `instrument` module.
- Create: `barter-data/examples/binance_futures_reference_discovery.rs`
  - No-credential example that fetches instruments and prints a compact summary.
- No changes in this increment:
  - `DynamicStreams`.
  - Support matrix.
  - MDB bridge.
  - Global registry/cache.

## Task 1: Raw Binance exchangeInfo models and filter parsing

**Files:**
- Create: `barter-data/src/exchange/binance/futures/instrument.rs`

- [ ] **Step 1: Write failing raw parser tests**

Create `barter-data/src/exchange/binance/futures/instrument.rs` with this initial test-focused skeleton:

```rust
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// [`crate::exchange::binance::futures::BinanceFuturesUsd`] HTTP exchange info url.
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#exchange-information>
pub const HTTP_EXCHANGE_INFO_URL_BINANCE_FUTURES_USD: &str =
    "https://fapi.binance.com/fapi/v1/exchangeInfo";

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesExchangeInfo {
    #[serde(alias = "symbols")]
    pub symbols: Vec<BinanceFuturesSymbolInfo>,
}

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesSymbolInfo {
    #[serde(alias = "symbol")]
    pub symbol: String,
    #[serde(alias = "pair")]
    pub pair: String,
    #[serde(alias = "contractType")]
    pub contract_type: String,
    #[serde(alias = "status")]
    pub status: String,
    #[serde(alias = "baseAsset")]
    pub base_asset: String,
    #[serde(alias = "quoteAsset")]
    pub quote_asset: String,
    #[serde(alias = "marginAsset")]
    pub margin_asset: String,
    #[serde(alias = "pricePrecision")]
    pub price_precision: u32,
    #[serde(alias = "quantityPrecision")]
    pub quantity_precision: u32,
    #[serde(alias = "onboardDate")]
    pub onboard_date: u64,
    #[serde(alias = "deliveryDate")]
    pub delivery_date: u64,
    #[serde(alias = "filters")]
    pub filters: Vec<BinanceFuturesSymbolFilter>,
}

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(tag = "filterType")]
pub enum BinanceFuturesSymbolFilter {}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn exchange_info_fixture() -> &'static str {
        r#"
        {
          "timezone": "UTC",
          "serverTime": 1569398400000,
          "symbols": [
            {
              "symbol": "BTCUSDT",
              "pair": "BTCUSDT",
              "contractType": "PERPETUAL",
              "deliveryDate": 4133404800000,
              "onboardDate": 1569398400000,
              "status": "TRADING",
              "maintMarginPercent": "2.5000",
              "requiredMarginPercent": "5.0000",
              "baseAsset": "BTC",
              "quoteAsset": "USDT",
              "marginAsset": "USDT",
              "pricePrecision": 2,
              "quantityPrecision": 3,
              "baseAssetPrecision": 8,
              "quotePrecision": 8,
              "underlyingType": "COIN",
              "underlyingSubType": ["PoW"],
              "settlePlan": 0,
              "triggerProtect": "0.0500",
              "filters": [
                {
                  "filterType": "PRICE_FILTER",
                  "maxPrice": "4529764",
                  "minPrice": "0.10",
                  "tickSize": "0.10"
                },
                {
                  "filterType": "LOT_SIZE",
                  "maxQty": "1000",
                  "minQty": "0.001",
                  "stepSize": "0.001"
                },
                {
                  "filterType": "MIN_NOTIONAL",
                  "notional": "100"
                },
                {
                  "filterType": "MARKET_LOT_SIZE",
                  "maxQty": "1000",
                  "minQty": "0.001",
                  "stepSize": "0.001"
                }
              ],
              "OrderType": ["LIMIT", "MARKET"],
              "timeInForce": ["GTC"]
            }
          ]
        }
        "#
    }

    #[test]
    fn exchange_info_url_matches_binance_usd_m_endpoint() {
        assert_eq!(
            HTTP_EXCHANGE_INFO_URL_BINANCE_FUTURES_USD,
            "https://fapi.binance.com/fapi/v1/exchangeInfo"
        );
    }

    #[test]
    fn binance_futures_exchange_info_deserialises_symbol_metadata() {
        let actual = serde_json::from_str::<BinanceFuturesExchangeInfo>(exchange_info_fixture())
            .expect("fixture should parse");

        assert_eq!(actual.symbols.len(), 1);
        let symbol = &actual.symbols[0];
        assert_eq!(symbol.symbol, "BTCUSDT");
        assert_eq!(symbol.pair, "BTCUSDT");
        assert_eq!(symbol.contract_type, "PERPETUAL");
        assert_eq!(symbol.status, "TRADING");
        assert_eq!(symbol.base_asset, "BTC");
        assert_eq!(symbol.quote_asset, "USDT");
        assert_eq!(symbol.margin_asset, "USDT");
        assert_eq!(symbol.price_precision, 2);
        assert_eq!(symbol.quantity_precision, 3);
        assert_eq!(symbol.onboard_date, 1569398400000);
        assert_eq!(symbol.delivery_date, 4133404800000);
        assert_eq!(symbol.filters.len(), 4);
    }

    #[test]
    fn binance_futures_symbol_filters_preserve_decimal_precision() {
        let actual = serde_json::from_str::<BinanceFuturesExchangeInfo>(exchange_info_fixture())
            .expect("fixture should parse");
        let symbol = &actual.symbols[0];

        assert_eq!(symbol.price_filter().unwrap().min_price, dec!(0.10));
        assert_eq!(symbol.price_filter().unwrap().tick_size, dec!(0.10));
        assert_eq!(symbol.lot_size_filter().unwrap().min_qty, dec!(0.001));
        assert_eq!(symbol.lot_size_filter().unwrap().step_size, dec!(0.001));
        assert_eq!(symbol.min_notional_filter().unwrap().notional, dec!(100));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-reference-discovery rtk cargo test -p barter-data exchange_info -- --nocapture
```

Expected: FAIL because `BinanceFuturesSymbolFilter` has no variants and `price_filter`, `lot_size_filter`, and `min_notional_filter` are undefined.

- [ ] **Step 3: Implement minimal raw filter parsing**

Replace the empty filter enum and add filter structs/accessors:

```rust
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(tag = "filterType")]
pub enum BinanceFuturesSymbolFilter {
    #[serde(rename = "PRICE_FILTER")]
    PriceFilter(BinanceFuturesPriceFilter),
    #[serde(rename = "LOT_SIZE")]
    LotSize(BinanceFuturesLotSizeFilter),
    #[serde(rename = "MIN_NOTIONAL")]
    MinNotional(BinanceFuturesMinNotionalFilter),
    #[serde(other)]
    Other,
}

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesPriceFilter {
    #[serde(
        alias = "minPrice",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub min_price: Decimal,
    #[serde(
        alias = "tickSize",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub tick_size: Decimal,
}

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesLotSizeFilter {
    #[serde(
        alias = "minQty",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub min_qty: Decimal,
    #[serde(
        alias = "stepSize",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub step_size: Decimal,
}

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct BinanceFuturesMinNotionalFilter {
    #[serde(
        alias = "notional",
        deserialize_with = "rust_decimal::serde::str::deserialize"
    )]
    pub notional: Decimal,
}

impl BinanceFuturesSymbolInfo {
    pub fn price_filter(&self) -> Option<&BinanceFuturesPriceFilter> {
        self.filters.iter().find_map(|filter| match filter {
            BinanceFuturesSymbolFilter::PriceFilter(filter) => Some(filter),
            _ => None,
        })
    }

    pub fn lot_size_filter(&self) -> Option<&BinanceFuturesLotSizeFilter> {
        self.filters.iter().find_map(|filter| match filter {
            BinanceFuturesSymbolFilter::LotSize(filter) => Some(filter),
            _ => None,
        })
    }

    pub fn min_notional_filter(&self) -> Option<&BinanceFuturesMinNotionalFilter> {
        self.filters.iter().find_map(|filter| match filter {
            BinanceFuturesSymbolFilter::MinNotional(filter) => Some(filter),
            _ => None,
        })
    }
}
```

If `#[serde(other)]` with externally tagged content fails to compile for data-carrying variants, replace `Other` with this robust approach:

```rust
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(tag = "filterType")]
pub enum BinanceFuturesSymbolFilter {
    #[serde(rename = "PRICE_FILTER")]
    PriceFilter(BinanceFuturesPriceFilter),
    #[serde(rename = "LOT_SIZE")]
    LotSize(BinanceFuturesLotSizeFilter),
    #[serde(rename = "MIN_NOTIONAL")]
    MinNotional(BinanceFuturesMinNotionalFilter),
    #[serde(untagged)]
    Other(serde_json::Value),
}
```

Then update match arms to use `_ => None` as shown above.

- [ ] **Step 4: Run focused tests**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-reference-discovery rtk cargo test -p barter-data exchange_info -- --nocapture
```

Expected: PASS for the 3 tests in this module.

- [ ] **Step 5: Format and commit**

Run:

```bash
rtk cargo fmt --all -- --check
rtk git diff --check
rtk git status --short
```

Commit:

```bash
rtk git add barter-data/src/exchange/binance/futures/instrument.rs
rtk git commit -m "feat(data): parse binance futures exchange info"
```

## Task 2: Normalize tradeable perpetual symbols into Barter instruments

**Files:**
- Modify: `barter-data/src/exchange/binance/futures/instrument.rs`

- [ ] **Step 1: Write failing normalization tests**

Add these imports near the top of the test module:

```rust
use barter_instrument::{
    Underlying,
    asset::name::AssetNameInternal,
    exchange::ExchangeId,
    instrument::{
        kind::{InstrumentKind, perpetual::PerpetualContract},
        name::InstrumentNameInternal,
        quote::InstrumentQuoteAsset,
        spec::{OrderQuantityUnits, InstrumentSpecNotional, InstrumentSpecPrice},
    },
};
```

Add a fixture helper for multiple symbols:

```rust
fn exchange_info_with_filtered_symbols_fixture() -> &'static str {
    r#"
    {
      "symbols": [
        {
          "symbol": "BTCUSDT",
          "pair": "BTCUSDT",
          "contractType": "PERPETUAL",
          "deliveryDate": 4133404800000,
          "onboardDate": 1569398400000,
          "status": "TRADING",
          "baseAsset": "BTC",
          "quoteAsset": "USDT",
          "marginAsset": "USDT",
          "pricePrecision": 2,
          "quantityPrecision": 3,
          "filters": [
            { "filterType": "PRICE_FILTER", "minPrice": "0.10", "tickSize": "0.10" },
            { "filterType": "LOT_SIZE", "minQty": "0.001", "stepSize": "0.001" },
            { "filterType": "MIN_NOTIONAL", "notional": "100" }
          ]
        },
        {
          "symbol": "BTCUSDT_240628",
          "pair": "BTCUSDT",
          "contractType": "CURRENT_QUARTER",
          "deliveryDate": 1719532800000,
          "onboardDate": 1569398400000,
          "status": "TRADING",
          "baseAsset": "BTC",
          "quoteAsset": "USDT",
          "marginAsset": "USDT",
          "pricePrecision": 2,
          "quantityPrecision": 3,
          "filters": [
            { "filterType": "PRICE_FILTER", "minPrice": "0.10", "tickSize": "0.10" },
            { "filterType": "LOT_SIZE", "minQty": "0.001", "stepSize": "0.001" },
            { "filterType": "MIN_NOTIONAL", "notional": "100" }
          ]
        },
        {
          "symbol": "ETHUSDT",
          "pair": "ETHUSDT",
          "contractType": "PERPETUAL",
          "deliveryDate": 4133404800000,
          "onboardDate": 1569398400000,
          "status": "BREAK",
          "baseAsset": "ETH",
          "quoteAsset": "USDT",
          "marginAsset": "USDT",
          "pricePrecision": 2,
          "quantityPrecision": 3,
          "filters": [
            { "filterType": "PRICE_FILTER", "minPrice": "0.01", "tickSize": "0.01" },
            { "filterType": "LOT_SIZE", "minQty": "0.001", "stepSize": "0.001" },
            { "filterType": "MIN_NOTIONAL", "notional": "20" }
          ]
        }
      ]
    }
    "#
}
```

Add tests:

```rust
#[test]
fn trading_perpetual_symbol_normalises_to_barter_instrument_with_specs() {
    let exchange_info = serde_json::from_str::<BinanceFuturesExchangeInfo>(exchange_info_fixture())
        .expect("fixture should parse");
    let actual = exchange_info.symbols[0]
        .try_into_usd_m_perpetual_instrument()
        .expect("BTCUSDT should convert");

    assert_eq!(actual.exchange, ExchangeId::BinanceFuturesUsd);
    assert_eq!(actual.name_exchange.as_ref(), "BTCUSDT");
    assert_eq!(
        actual.name_internal,
        InstrumentNameInternal::new_from_exchange(ExchangeId::BinanceFuturesUsd, "BTCUSDT")
    );
    assert_eq!(actual.underlying, Underlying::new(AssetNameInternal::from("BTC"), AssetNameInternal::from("USDT")));
    assert_eq!(actual.quote, InstrumentQuoteAsset::UnderlyingQuote);
    assert_eq!(
        actual.kind,
        InstrumentKind::Perpetual(PerpetualContract {
            contract_size: Decimal::ONE,
            settlement_asset: AssetNameInternal::from("USDT"),
        })
    );

    let spec = actual.spec.expect("perpetual instrument should include spec");
    assert_eq!(spec.price, InstrumentSpecPrice::new(dec!(0.10), dec!(0.10)));
    assert_eq!(spec.quantity.min, dec!(0.001));
    assert_eq!(spec.quantity.increment, dec!(0.001));
    assert_eq!(spec.quantity.unit, OrderQuantityUnits::Asset(AssetNameInternal::from("BTC")));
    assert_eq!(spec.notional, InstrumentSpecNotional::new(dec!(100)));
}

#[test]
fn exchange_info_discovers_only_trading_perpetual_instruments() {
    let exchange_info = serde_json::from_str::<BinanceFuturesExchangeInfo>(
        exchange_info_with_filtered_symbols_fixture(),
    )
    .expect("fixture should parse");

    let actual = exchange_info.usd_m_perpetual_instruments();

    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].name_exchange.as_ref(), "BTCUSDT");
}

#[test]
fn missing_required_filters_return_deterministic_conversion_error() {
    let mut exchange_info = serde_json::from_str::<BinanceFuturesExchangeInfo>(exchange_info_fixture())
        .expect("fixture should parse");
    exchange_info.symbols[0]
        .filters
        .retain(|filter| !matches!(filter, BinanceFuturesSymbolFilter::MinNotional(_)));

    let actual = exchange_info.symbols[0].try_into_usd_m_perpetual_instrument();

    assert_eq!(
        actual.unwrap_err(),
        BinanceFuturesInstrumentError::MissingFilter {
            symbol: "BTCUSDT".to_owned(),
            filter: "MIN_NOTIONAL",
        }
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-reference-discovery rtk cargo test -p barter-data exchange_info -- --nocapture
```

Expected: FAIL because `try_into_usd_m_perpetual_instrument`, `usd_m_perpetual_instruments`, and `BinanceFuturesInstrumentError` do not exist.

- [ ] **Step 3: Implement conversion error and normalized mapping**

Add imports at the top of `instrument.rs`:

```rust
use barter_instrument::{
    Underlying,
    asset::name::AssetNameInternal,
    exchange::ExchangeId,
    instrument::{
        Instrument,
        kind::{InstrumentKind, perpetual::PerpetualContract},
        name::InstrumentNameInternal,
        quote::InstrumentQuoteAsset,
        spec::{
            InstrumentSpec, InstrumentSpecNotional, InstrumentSpecPrice, InstrumentSpecQuantity,
            OrderQuantityUnits,
        },
    },
};
use thiserror::Error;
```

Add the error type and helper methods:

```rust
#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum BinanceFuturesInstrumentError {
    #[error("Binance futures symbol {symbol} missing required {filter} filter")]
    MissingFilter { symbol: String, filter: &'static str },
    #[error("Binance futures symbol {symbol} is not a trading perpetual: contract_type={contract_type}, status={status}")]
    NotTradingPerpetual {
        symbol: String,
        contract_type: String,
        status: String,
    },
}

impl BinanceFuturesExchangeInfo {
    pub fn usd_m_perpetual_instruments(&self) -> Vec<Instrument<ExchangeId, AssetNameInternal>> {
        self.symbols
            .iter()
            .filter(|symbol| symbol.is_trading_perpetual())
            .filter_map(|symbol| symbol.try_into_usd_m_perpetual_instrument().ok())
            .collect()
    }
}

impl BinanceFuturesSymbolInfo {
    pub fn is_trading_perpetual(&self) -> bool {
        self.contract_type == "PERPETUAL" && self.status == "TRADING"
    }

    pub fn try_into_usd_m_perpetual_instrument(
        &self,
    ) -> Result<Instrument<ExchangeId, AssetNameInternal>, BinanceFuturesInstrumentError> {
        if !self.is_trading_perpetual() {
            return Err(BinanceFuturesInstrumentError::NotTradingPerpetual {
                symbol: self.symbol.clone(),
                contract_type: self.contract_type.clone(),
                status: self.status.clone(),
            });
        }

        let price = self.price_filter().ok_or_else(|| {
            BinanceFuturesInstrumentError::MissingFilter {
                symbol: self.symbol.clone(),
                filter: "PRICE_FILTER",
            }
        })?;
        let lot_size = self.lot_size_filter().ok_or_else(|| {
            BinanceFuturesInstrumentError::MissingFilter {
                symbol: self.symbol.clone(),
                filter: "LOT_SIZE",
            }
        })?;
        let min_notional = self.min_notional_filter().ok_or_else(|| {
            BinanceFuturesInstrumentError::MissingFilter {
                symbol: self.symbol.clone(),
                filter: "MIN_NOTIONAL",
            }
        })?;

        let base = AssetNameInternal::from(self.base_asset.clone());
        let quote = AssetNameInternal::from(self.quote_asset.clone());
        let settlement_asset = AssetNameInternal::from(self.margin_asset.clone());

        Ok(Instrument::new(
            ExchangeId::BinanceFuturesUsd,
            InstrumentNameInternal::new_from_exchange(ExchangeId::BinanceFuturesUsd, self.symbol.clone()),
            self.symbol.clone(),
            Underlying::new(base.clone(), quote),
            InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Perpetual(PerpetualContract {
                contract_size: Decimal::ONE,
                settlement_asset,
            }),
            Some(InstrumentSpec::new(
                InstrumentSpecPrice::new(price.min_price, price.tick_size),
                InstrumentSpecQuantity::new(
                    OrderQuantityUnits::Asset(base),
                    lot_size.min_qty,
                    lot_size.step_size,
                ),
                InstrumentSpecNotional::new(min_notional.notional),
            )),
        ))
    }
}
```

If Rust type inference rejects `Underlying::new(base.clone(), quote)` because both args must have the same concrete type, keep both as `AssetNameInternal` as shown. If `InstrumentNameInternal::new_from_exchange` rejects `String`, pass `self.symbol.as_str()`.

- [ ] **Step 4: Run focused tests**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-reference-discovery rtk cargo test -p barter-data exchange_info -- --nocapture
```

Expected: PASS for raw parsing and normalization tests.

- [ ] **Step 5: Format and commit**

Run:

```bash
rtk cargo fmt --all -- --check
rtk git diff --check
rtk git status --short
```

Commit:

```bash
rtk git add barter-data/src/exchange/binance/futures/instrument.rs
rtk git commit -m "feat(data): normalize binance futures instruments"
```

## Task 3: REST fetcher and module export

**Files:**
- Modify: `barter-data/src/exchange/binance/futures/instrument.rs`
- Modify: `barter-data/src/exchange/binance/futures/mod.rs`

- [ ] **Step 1: Write failing fetcher/export tests**

Add this async test to the existing test module in `instrument.rs`:

```rust
#[tokio::test]
async fn exchange_info_fetcher_surfaces_http_error_statuses_before_json_decoding() {
    use std::{
        io::{Read, Write},
        net::TcpListener,
    };

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0; 1024];
        let _ = stream.read(&mut request).unwrap();
        stream
            .write_all(
                b"HTTP/1.1 418 I'm a teapot\r\ncontent-type: application/json\r\ncontent-length: 8\r\n\r\nnot-json",
            )
            .unwrap();
    });

    let actual = fetch_exchange_info_url(url).await;

    server.join().unwrap();
    match actual {
        Err(barter_integration::error::SocketError::Http(error)) => {
            assert_eq!(error.status(), Some(reqwest::StatusCode::IM_A_TEAPOT));
        }
        other => panic!("expected HTTP status error before JSON decoding, got {other:?}"),
    }
}
```

Add this export compile assertion test near the module tests or rely on compilation by modifying `mod.rs` in Step 3:

```rust
#[test]
fn exchange_info_fetcher_type_is_publicly_constructible() {
    let _fetcher = BinanceFuturesUsdExchangeInfoFetcher;
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-reference-discovery rtk cargo test -p barter-data exchange_info -- --nocapture
```

Expected: FAIL because `fetch_exchange_info_url` and `BinanceFuturesUsdExchangeInfoFetcher` do not exist.

- [ ] **Step 3: Implement REST fetcher and export module**

Add to `instrument.rs`:

```rust
#[derive(Debug)]
pub struct BinanceFuturesUsdExchangeInfoFetcher;

impl BinanceFuturesUsdExchangeInfoFetcher {
    pub async fn fetch() -> Result<BinanceFuturesExchangeInfo, barter_integration::error::SocketError>
    {
        fetch_exchange_info_url(HTTP_EXCHANGE_INFO_URL_BINANCE_FUTURES_USD.to_owned()).await
    }

    pub async fn fetch_usd_m_perpetual_instruments(
    ) -> Result<Vec<Instrument<ExchangeId, AssetNameInternal>>, barter_integration::error::SocketError>
    {
        Ok(Self::fetch().await?.usd_m_perpetual_instruments())
    }
}

async fn fetch_exchange_info_url(
    url: String,
) -> Result<BinanceFuturesExchangeInfo, barter_integration::error::SocketError> {
    reqwest::get(url)
        .await
        .map_err(barter_integration::error::SocketError::Http)?
        .error_for_status()
        .map_err(barter_integration::error::SocketError::Http)?
        .json::<BinanceFuturesExchangeInfo>()
        .await
        .map_err(barter_integration::error::SocketError::Http)
}
```

Modify `barter-data/src/exchange/binance/futures/mod.rs` to export the module near the other REST modules:

```rust
/// Instrument reference discovery REST types and fetcher.
pub mod instrument;
```

Place it next to `funding`, `mark_price`, `index_price`, and `open_interest` exports.

- [ ] **Step 4: Run focused tests and module check**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-reference-discovery rtk cargo test -p barter-data exchange_info -- --nocapture
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-reference-discovery rtk cargo check -p barter-data
```

Expected: PASS tests and check with 0 errors.

- [ ] **Step 5: Format and commit**

Run:

```bash
rtk cargo fmt --all -- --check
rtk git diff --check
rtk git status --short
```

Commit:

```bash
rtk git add barter-data/src/exchange/binance/futures/instrument.rs barter-data/src/exchange/binance/futures/mod.rs
rtk git commit -m "feat(data): fetch binance futures exchange info"
```

## Task 4: No-credential example

**Files:**
- Create: `barter-data/examples/binance_futures_reference_discovery.rs`

- [ ] **Step 1: Write example**

Create `barter-data/examples/binance_futures_reference_discovery.rs`:

```rust
use barter_data::exchange::binance::futures::instrument::BinanceFuturesUsdExchangeInfoFetcher;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let instruments = BinanceFuturesUsdExchangeInfoFetcher::fetch_usd_m_perpetual_instruments()
        .await?;

    info!(
        count = instruments.len(),
        "discovered Binance USD-M perpetual instruments"
    );

    for instrument in instruments.iter().take(5) {
        let spec = instrument
            .spec
            .as_ref()
            .expect("discovered futures instruments include specs");
        info!(
            exchange = ?instrument.exchange,
            symbol = instrument.name_exchange.as_ref(),
            internal = instrument.name_internal.as_ref(),
            base = instrument.underlying.base.as_ref(),
            quote = instrument.underlying.quote.as_ref(),
            tick_size = %spec.price.tick_size,
            step_size = %spec.quantity.increment,
            min_notional = %spec.notional.min,
            "instrument reference"
        );
    }

    Ok(())
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init()
}
```

- [ ] **Step 2: Run example check**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-reference-discovery rtk cargo check -p barter-data --examples
```

Expected: PASS with 0 errors.

If `Box<dyn std::error::Error>` does not accept `SocketError` with `?`, change main return type to:

```rust
async fn main() -> Result<(), barter_integration::error::SocketError>
```

and keep the rest of the example unchanged.

- [ ] **Step 3: Format and commit**

Run:

```bash
rtk cargo fmt --all -- --check
rtk git diff --check
rtk git status --short
```

Commit:

```bash
rtk git add barter-data/examples/binance_futures_reference_discovery.rs
rtk git commit -m "docs(data): add futures reference discovery example"
```

## Task 5: Final verification and review readiness

**Files:**
- Verify all changed files.
- No new code unless verification exposes a bug.

- [ ] **Step 1: Run focused verification**

Run:

```bash
rtk cargo fmt --all -- --check
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-reference-discovery rtk cargo test -p barter-data exchange_info -- --nocapture
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-reference-discovery rtk cargo check -p barter-data --examples
rtk git diff --check
```

Expected:

- fmt check exits 0.
- `exchange_info` tests pass.
- examples check exits 0.
- diff check exits 0.

- [ ] **Step 2: Run broader package verification if disk allows**

Run:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-reference-discovery-full rtk cargo test -p barter-data -- --nocapture
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/tmp/barter-rs-target-reference-discovery-full rtk cargo check -p barter-data
```

Expected:

- `barter-data` tests pass.
- `barter-data` check exits 0.

If `/tmp` space is exhausted, remove only this task's temporary targets:

```bash
rm -rf /tmp/barter-rs-target-reference-discovery /tmp/barter-rs-target-reference-discovery-full
```

Then rerun the broader verification with `CARGO_PROFILE_DEV_DEBUG=0` as shown above.

- [ ] **Step 3: Inspect final diff for scope control**

Run:

```bash
rtk git diff --stat HEAD~3..HEAD
rtk git status --short
```

Expected:

- Diff includes only `instrument.rs`, `futures/mod.rs`, and the example, plus earlier design/plan docs if the plan is committed in the same branch.
- `git status --short` is clean after any final commit.

- [ ] **Step 4: Commit any verification-only fixes**

If verification required fixes, commit them with a focused message:

```bash
rtk git add <fixed-files>
rtk git commit -m "fix(data): finalize futures reference discovery"
```

If no fixes were needed, do not create an empty commit.

- [ ] **Step 5: Request code review**

Use the code review process after all tests pass. The review checklist should cover:

- Raw Binance fixture fidelity.
- Decimal parsing without `f64`.
- `PERPETUAL + TRADING` filter semantics.
- Instrument name/base/quote/settlement mapping.
- Required filter error handling.
- HTTP `error_for_status()` before JSON decode.
- Scope control: no registry/cache/DynamicStreams/MDB bridge added.

Address Critical and Important review findings before claiming completion. Minor findings can be fixed immediately if low-risk, or documented as follow-up.

---

## Plan Self-Review

- Spec coverage: Tasks cover raw fetcher, raw metadata, filter decimal parsing, normalized `Instrument`/`InstrumentSpec`, tests, example, verification, and non-goal scope control.
- Red-flag scan: No unresolved marker text or vague implementation instructions remain.
- Type consistency: The plan consistently uses `BinanceFuturesUsdExchangeInfoFetcher`, `BinanceFuturesExchangeInfo`, `BinanceFuturesSymbolInfo`, `BinanceFuturesSymbolFilter`, and `try_into_usd_m_perpetual_instrument` across tasks.
- Known implementation risk: Serde enum handling for unknown filter types may need the fallback shown in Task 1. The plan gives the exact replacement if the first enum form fails.
