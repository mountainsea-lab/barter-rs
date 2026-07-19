use barter_data::{
    exchange::binance::futures::{
        BinanceFuturesUsd, open_interest::BinanceFuturesUsdOpenInterestFetcher,
    },
    subscription::{Subscription, open_interest::OpenInterests},
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
        OpenInterests,
    ))];

    let events = BinanceFuturesUsdOpenInterestFetcher::fetch_latest(&subscriptions)
        .await
        .expect("fetch Binance USD-M Futures open interest");

    for event in events {
        tracing::info!(?event, "received Binance USD-M Futures open interest");
    }
}
