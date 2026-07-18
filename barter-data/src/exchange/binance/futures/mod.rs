use self::{candle::BinanceFuturesKline, liquidation::BinanceLiquidation};
use super::{Binance, ExchangeServer};
use crate::{
    NoInitialSnapshots,
    exchange::{
        StreamSelector,
        binance::{
            BinanceWsStream,
            futures::l2::{
                BinanceFuturesUsdOrderBooksL2SnapshotFetcher,
                BinanceFuturesUsdOrderBooksL2Transformer,
            },
        },
    },
    instrument::InstrumentData,
    subscription::{book::OrderBooksL2, candle::Candles, liquidation::Liquidations},
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use std::fmt::{Display, Formatter};

/// Kline/candlestick types.
pub mod candle;

/// Funding rate REST types and fetcher.
pub mod funding;

/// Level 2 OrderBook types.
pub mod l2;

/// Liquidation types.
pub mod liquidation;

/// [`BinanceFuturesUsd`] WebSocket server base url.
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams>
pub const WEBSOCKET_BASE_URL_BINANCE_FUTURES_USD: &str = "wss://fstream.binance.com/market/ws";

/// [`Binance`] perpetual usd exchange.
pub type BinanceFuturesUsd = Binance<BinanceServerFuturesUsd>;

/// [`Binance`] perpetual usd [`ExchangeServer`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct BinanceServerFuturesUsd;

impl ExchangeServer for BinanceServerFuturesUsd {
    const ID: ExchangeId = ExchangeId::BinanceFuturesUsd;

    fn websocket_url() -> &'static str {
        WEBSOCKET_BASE_URL_BINANCE_FUTURES_USD
    }
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL2> for BinanceFuturesUsd
where
    Instrument: InstrumentData,
{
    type SnapFetcher = BinanceFuturesUsdOrderBooksL2SnapshotFetcher;
    type Stream = BinanceWsStream<BinanceFuturesUsdOrderBooksL2Transformer<Instrument::Key>>;
}

impl<Instrument> StreamSelector<Instrument, Candles> for BinanceFuturesUsd
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        BinanceWsStream<StatelessTransformer<Self, Instrument::Key, Candles, BinanceFuturesKline>>;
}

impl<Instrument> StreamSelector<Instrument, Liquidations> for BinanceFuturesUsd
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = BinanceWsStream<
        StatelessTransformer<Self, Instrument::Key, Liquidations, BinanceLiquidation>,
    >;
}

impl Display for BinanceFuturesUsd {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BinanceFuturesUsd")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::Connector;

    #[test]
    fn binance_futures_usd_uses_market_websocket_path() {
        assert_eq!(
            BinanceFuturesUsd::url().unwrap().as_str(),
            "wss://fstream.binance.com/market/ws"
        );
    }

    #[test]
    fn binance_futures_usd_websocket_base_url_matches_official_market_path() {
        assert_eq!(
            WEBSOCKET_BASE_URL_BINANCE_FUTURES_USD,
            "wss://fstream.binance.com/market/ws"
        );
    }
}
