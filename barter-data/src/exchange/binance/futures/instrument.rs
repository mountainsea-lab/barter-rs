use rust_decimal::Decimal;
use serde::Deserialize;

/// Binance USD-M Futures exchangeInfo HTTP url.
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#exchange-information>
pub const HTTP_EXCHANGE_INFO_URL_BINANCE_FUTURES_USD: &str =
    "https://fapi.binance.com/fapi/v1/exchangeInfo";

/// Binance USD-M Futures exchangeInfo response.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
pub struct BinanceFuturesExchangeInfo {
    pub symbols: Vec<BinanceFuturesSymbolInfo>,
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
    use rust_decimal_macros::dec;

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
}
