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
use rust_decimal::Decimal;
use serde::Deserialize;

/// Error normalising Binance USD-M Futures symbol metadata into Barter instruments.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum BinanceFuturesInstrumentError {
    #[error("missing required Binance futures symbol filter {filter} for {symbol}")]
    MissingFilter {
        symbol: String,
        filter: &'static str,
    },
    #[error(
        "Binance futures symbol {symbol} is not a trading perpetual: contract_type={contract_type}, status={status}"
    )]
    NotTradingPerpetual {
        symbol: String,
        contract_type: String,
        status: String,
    },
}

/// Binance USD-M Futures exchangeInfo HTTP url.
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#exchange-information>
pub const HTTP_EXCHANGE_INFO_URL_BINANCE_FUTURES_USD: &str =
    "https://fapi.binance.com/fapi/v1/exchangeInfo";

pub fn exchange_info_url() -> String {
    HTTP_EXCHANGE_INFO_URL_BINANCE_FUTURES_USD.to_owned()
}

pub async fn fetch_exchange_info()
-> Result<BinanceFuturesExchangeInfo, barter_integration::error::SocketError> {
    fetch_exchange_info_url(exchange_info_url()).await
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

/// Binance USD-M Futures exchangeInfo response.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
pub struct BinanceFuturesExchangeInfo {
    pub symbols: Vec<BinanceFuturesSymbolInfo>,
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

/// Binance USD-M Futures symbol metadata from exchangeInfo.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
pub struct BinanceFuturesSymbolInfo {
    pub symbol: String,
    pub pair: String,
    #[serde(alias = "contractType")]
    pub contract_type: String,
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
    pub filters: Vec<BinanceFuturesSymbolFilter>,
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

        let price_filter =
            self.price_filter()
                .ok_or_else(|| BinanceFuturesInstrumentError::MissingFilter {
                    symbol: self.symbol.clone(),
                    filter: "PRICE_FILTER",
                })?;
        let lot_size_filter =
            self.lot_size_filter()
                .ok_or_else(|| BinanceFuturesInstrumentError::MissingFilter {
                    symbol: self.symbol.clone(),
                    filter: "LOT_SIZE",
                })?;
        let min_notional_filter = self.min_notional_filter().ok_or_else(|| {
            BinanceFuturesInstrumentError::MissingFilter {
                symbol: self.symbol.clone(),
                filter: "MIN_NOTIONAL",
            }
        })?;

        Ok(Instrument::new(
            ExchangeId::BinanceFuturesUsd,
            InstrumentNameInternal::new_from_exchange(
                ExchangeId::BinanceFuturesUsd,
                self.symbol.as_str(),
            ),
            self.symbol.as_str(),
            Underlying::new(
                AssetNameInternal::from(self.base_asset.as_str()),
                AssetNameInternal::from(self.quote_asset.as_str()),
            ),
            InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Perpetual(PerpetualContract {
                contract_size: Decimal::ONE,
                settlement_asset: AssetNameInternal::from(self.margin_asset.as_str()),
            }),
            Some(InstrumentSpec {
                price: InstrumentSpecPrice {
                    min: price_filter.min_price,
                    tick_size: price_filter.tick_size,
                },
                quantity: InstrumentSpecQuantity {
                    unit: OrderQuantityUnits::Asset(AssetNameInternal::from(
                        self.base_asset.as_str(),
                    )),
                    min: lot_size_filter.min_qty,
                    increment: lot_size_filter.step_size,
                },
                notional: InstrumentSpecNotional {
                    min: min_notional_filter.notional,
                },
            }),
        ))
    }

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

/// Binance USD-M Futures symbol filters.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
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

#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
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

#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
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

#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
pub struct BinanceFuturesMinNotionalFilter {
    #[serde(deserialize_with = "rust_decimal::serde::str::deserialize")]
    pub notional: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
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
                InstrumentSpec, InstrumentSpecNotional, InstrumentSpecPrice,
                InstrumentSpecQuantity, OrderQuantityUnits,
            },
        },
    };
    use rust_decimal_macros::dec;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn exchange_info_fixture() -> &'static str {
        r#"
        {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "pair": "BTCUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                    "onboardDate": 1569398400000,
                    "deliveryDate": 4133404800000,
                    "filters": [
                        {
                            "filterType": "PRICE_FILTER",
                            "minPrice": "0.10",
                            "maxPrice": "1000000",
                            "tickSize": "0.10"
                        },
                        {
                            "filterType": "LOT_SIZE",
                            "minQty": "0.001",
                            "maxQty": "1000",
                            "stepSize": "0.001"
                        },
                        {
                            "filterType": "MARKET_LOT_SIZE",
                            "minQty": "0.001",
                            "maxQty": "1000",
                            "stepSize": "0.001"
                        },
                        {
                            "filterType": "MIN_NOTIONAL",
                            "notional": "100.00000000"
                        }
                    ]
                }
            ]
        }
        "#
    }

    #[test]
    fn exchange_info_endpoint_constant_matches_binance_futures_usd() {
        assert_eq!(
            HTTP_EXCHANGE_INFO_URL_BINANCE_FUTURES_USD,
            "https://fapi.binance.com/fapi/v1/exchangeInfo"
        );
    }

    #[test]
    fn exchange_info_public_crate_path_exports_instrument_module() {
        assert_eq!(
            crate::exchange::binance::futures::instrument::HTTP_EXCHANGE_INFO_URL_BINANCE_FUTURES_USD,
            "https://fapi.binance.com/fapi/v1/exchangeInfo"
        );
    }

    #[test]
    fn exchange_info_url_helper_targets_binance_futures_usd_endpoint() {
        assert_eq!(
            exchange_info_url(),
            HTTP_EXCHANGE_INFO_URL_BINANCE_FUTURES_USD
        );
    }

    #[tokio::test]
    async fn exchange_info_fetch_url_surfaces_non_success_http_status() {
        let url = spawn_http_response(
            "HTTP/1.1 418 I'm a teapot\r\ncontent-type: application/json\r\ncontent-length: 8\r\n\r\nnot-json".to_owned(),
        )
        .await;

        let actual = fetch_exchange_info_url(url).await.unwrap_err();

        match actual {
            barter_integration::error::SocketError::Http(error) => {
                assert_eq!(error.status(), Some(reqwest::StatusCode::IM_A_TEAPOT));
            }
            error => panic!("expected HTTP status error, got {error:?}"),
        }
    }

    #[tokio::test]
    async fn exchange_info_fetch_url_decodes_valid_fixture_response() {
        let body = exchange_info_fixture();
        let url = spawn_http_response(format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        ))
        .await;

        let actual = fetch_exchange_info_url(url).await.unwrap();

        assert_eq!(actual.symbols.len(), 1);
        assert_eq!(actual.symbols[0].symbol, "BTCUSDT");
        assert_eq!(
            actual.symbols[0].price_filter().unwrap().tick_size,
            dec!(0.10)
        );
    }

    async fn spawn_http_response(response: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            stream.write_all(response.as_bytes()).await.unwrap();
        });

        format!("http://{address}/fapi/v1/exchangeInfo")
    }

    #[test]
    fn exchange_info_deserializes_symbol_metadata() {
        let actual =
            serde_json::from_str::<BinanceFuturesExchangeInfo>(exchange_info_fixture()).unwrap();
        let symbol = actual.symbols.into_iter().next().unwrap();

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
    }

    #[test]
    fn exchange_info_deserializes_decimal_filter_precision_and_ignores_unknown_filters() {
        let actual =
            serde_json::from_str::<BinanceFuturesExchangeInfo>(exchange_info_fixture()).unwrap();
        let symbol = actual.symbols.into_iter().next().unwrap();

        let price_filter = symbol.price_filter().unwrap();
        assert_eq!(price_filter.min_price, dec!(0.10));
        assert_eq!(price_filter.tick_size, dec!(0.10));

        let lot_size_filter = symbol.lot_size_filter().unwrap();
        assert_eq!(lot_size_filter.min_qty, dec!(0.001));
        assert_eq!(lot_size_filter.step_size, dec!(0.001));

        let min_notional_filter = symbol.min_notional_filter().unwrap();
        assert_eq!(min_notional_filter.notional, dec!(100.00000000));

        assert!(
            symbol
                .filters
                .iter()
                .any(|filter| matches!(filter, BinanceFuturesSymbolFilter::Other))
        );
    }

    #[test]
    fn exchange_info_trading_perpetual_symbol_normalises_to_barter_instrument_with_specs() {
        let actual = serde_json::from_str::<BinanceFuturesExchangeInfo>(exchange_info_fixture())
            .unwrap()
            .symbols
            .into_iter()
            .next()
            .unwrap()
            .try_into_usd_m_perpetual_instrument()
            .unwrap();

        let expected = Instrument::new(
            ExchangeId::BinanceFuturesUsd,
            InstrumentNameInternal::new_from_exchange(ExchangeId::BinanceFuturesUsd, "BTCUSDT"),
            "BTCUSDT",
            Underlying::new(
                AssetNameInternal::from("BTC"),
                AssetNameInternal::from("USDT"),
            ),
            InstrumentQuoteAsset::UnderlyingQuote,
            InstrumentKind::Perpetual(PerpetualContract {
                contract_size: Decimal::ONE,
                settlement_asset: AssetNameInternal::from("USDT"),
            }),
            Some(InstrumentSpec {
                price: InstrumentSpecPrice {
                    min: dec!(0.10),
                    tick_size: dec!(0.10),
                },
                quantity: InstrumentSpecQuantity {
                    unit: OrderQuantityUnits::Asset(AssetNameInternal::from("BTC")),
                    min: dec!(0.001),
                    increment: dec!(0.001),
                },
                notional: InstrumentSpecNotional {
                    min: dec!(100.00000000),
                },
            }),
        );

        assert_eq!(actual, expected);
    }

    #[test]
    fn exchange_info_discovers_only_trading_perpetual_instruments() {
        let fixture = r#"
        {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "pair": "BTCUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                    "onboardDate": 1569398400000,
                    "deliveryDate": 4133404800000,
                    "filters": [
                        { "filterType": "PRICE_FILTER", "minPrice": "0.10", "tickSize": "0.10" },
                        { "filterType": "LOT_SIZE", "minQty": "0.001", "stepSize": "0.001" },
                        { "filterType": "MIN_NOTIONAL", "notional": "100.00000000" }
                    ]
                },
                {
                    "symbol": "ETHUSDT",
                    "pair": "ETHUSDT",
                    "contractType": "PERPETUAL",
                    "status": "BREAK",
                    "baseAsset": "ETH",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                    "onboardDate": 1569398400000,
                    "deliveryDate": 4133404800000,
                    "filters": []
                },
                {
                    "symbol": "BTCUSDT_240927",
                    "pair": "BTCUSDT",
                    "contractType": "CURRENT_QUARTER",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                    "onboardDate": 1569398400000,
                    "deliveryDate": 1727424000000,
                    "filters": []
                },
                {
                    "symbol": "SOLUSDT",
                    "pair": "SOLUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "baseAsset": "SOL",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "pricePrecision": 2,
                    "quantityPrecision": 0,
                    "onboardDate": 1569398400000,
                    "deliveryDate": 4133404800000,
                    "filters": []
                }
            ]
        }
        "#;
        let exchange_info = serde_json::from_str::<BinanceFuturesExchangeInfo>(fixture).unwrap();

        let actual = exchange_info.usd_m_perpetual_instruments();

        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].name_exchange.name().as_str(), "BTCUSDT");
    }

    #[test]
    fn exchange_info_missing_required_filters_return_deterministic_conversion_error() {
        let symbol = serde_json::from_str::<BinanceFuturesExchangeInfo>(exchange_info_fixture())
            .unwrap()
            .symbols
            .into_iter()
            .next()
            .unwrap();

        let mut missing_price_filter = symbol.clone();
        missing_price_filter.filters.remove(0);
        assert_eq!(
            missing_price_filter.try_into_usd_m_perpetual_instrument(),
            Err(BinanceFuturesInstrumentError::MissingFilter {
                symbol: "BTCUSDT".to_owned(),
                filter: "PRICE_FILTER"
            })
        );

        let mut missing_lot_size = symbol.clone();
        missing_lot_size.filters.remove(1);
        assert_eq!(
            missing_lot_size.try_into_usd_m_perpetual_instrument(),
            Err(BinanceFuturesInstrumentError::MissingFilter {
                symbol: "BTCUSDT".to_owned(),
                filter: "LOT_SIZE"
            })
        );

        let mut missing_min_notional = symbol;
        missing_min_notional
            .filters
            .retain(|filter| !matches!(filter, BinanceFuturesSymbolFilter::MinNotional(_)));
        assert_eq!(
            missing_min_notional.try_into_usd_m_perpetual_instrument(),
            Err(BinanceFuturesInstrumentError::MissingFilter {
                symbol: "BTCUSDT".to_owned(),
                filter: "MIN_NOTIONAL"
            })
        );
    }

    #[test]
    fn exchange_info_non_trading_perpetual_returns_conversion_error() {
        let mut symbol =
            serde_json::from_str::<BinanceFuturesExchangeInfo>(exchange_info_fixture())
                .unwrap()
                .symbols
                .into_iter()
                .next()
                .unwrap();
        symbol.status = "BREAK".to_owned();

        assert!(!symbol.is_trading_perpetual());
        assert_eq!(
            symbol.try_into_usd_m_perpetual_instrument(),
            Err(BinanceFuturesInstrumentError::NotTradingPerpetual {
                symbol: "BTCUSDT".to_owned(),
                contract_type: "PERPETUAL".to_owned(),
                status: "BREAK".to_owned(),
            })
        );
    }
}
