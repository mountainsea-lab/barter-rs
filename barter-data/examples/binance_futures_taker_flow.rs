use barter_data::{
    exchange::binance::futures::{
        BinanceFuturesUsd, taker_flow::BinanceFuturesUsdTakerFlowFetcher,
    },
    subscription::{Subscription, taker_flow::TakerFlows},
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
        TakerFlows,
    ))];

    let events = BinanceFuturesUsdTakerFlowFetcher::fetch_recent(&subscriptions, "5m", Some(3))
        .await
        .expect("fetch Binance USD-M Futures taker flow");

    for event in events {
        tracing::info!(?event, "received Binance USD-M Futures taker flow");
    }
}
