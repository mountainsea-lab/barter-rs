use barter_data::{
    exchange::binance::futures::{
        BinanceFuturesUsd, index_price::BinanceFuturesUsdIndexPriceFetcher,
    },
    subscription::{Subscription, index_price::IndexPrices},
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let subscriptions = vec![Subscription::from((
        BinanceFuturesUsd::default(),
        "btc",
        "usdt",
        MarketDataInstrumentKind::Perpetual,
        IndexPrices,
    ))];

    let events = BinanceFuturesUsdIndexPriceFetcher::fetch_latest(&subscriptions)
        .await
        .expect("fetch Binance USD-M Futures index prices");

    for event in events {
        tracing::info!(?event, "received Binance USD-M Futures index price");
    }
}
