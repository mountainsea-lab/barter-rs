use barter_data::{
    exchange::binance::futures::BinanceFuturesUsd,
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscription::index_price::IndexPrices,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures_util::StreamExt;
use tracing::{info, warn};

#[rustfmt::skip]
#[tokio::main]
async fn main() {
    // Initialise INFO Tracing log subscriber.
    init_logging();

    // Initialise Binance USD-M Futures index price streams.
    // Binance exposes index price updates in the mark price stream payload field `i`.
    // '--> each call to StreamBuilder::subscribe() creates a separate WebSocket connection.
    let streams = Streams::<IndexPrices>::builder()

        // Separate WebSocket connection for BTC_USDT index price stream.
        .subscribe([
            (BinanceFuturesUsd::default(), "btc", "usdt", MarketDataInstrumentKind::Perpetual, IndexPrices),
        ])

        // Lower volume instruments can share a WebSocket connection.
        .subscribe([
            (BinanceFuturesUsd::default(), "eth", "usdt", MarketDataInstrumentKind::Perpetual, IndexPrices),
            (BinanceFuturesUsd::default(), "sol", "usdt", MarketDataInstrumentKind::Perpetual, IndexPrices),
        ])
        .init()
        .await
        .unwrap();

    // Select and merge every exchange Stream using futures_util::stream::select_all.
    // Note: use `Streams.select(ExchangeId)` to interact with individual exchange streams.
    let mut joined_stream = streams
        .select_all()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

    while let Some(event) = joined_stream.next().await {
        info!(?event, "Binance USD-M Futures index price");
    }
}

// Initialise an INFO `Subscriber` for `Tracing` Json logs and install it as the global default.
fn init_logging() {
    tracing_subscriber::fmt()
        // Filter messages based on the INFO.
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        // Disable colours on release builds.
        .with_ansi(cfg!(debug_assertions))
        // Enable Json formatting.
        .json()
        // Install this Tracing subscriber as global default.
        .init()
}
