use barter_data::{
    exchange::binance::futures::{
        BinanceFuturesUsd,
        funding::{BinanceFuturesFundingRateRequest, BinanceFuturesUsdFundingRateFetcher},
    },
    subscription::{Subscription, funding::FundingRates},
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
        FundingRates,
    ))];

    let events = BinanceFuturesUsdFundingRateFetcher::fetch(
        &subscriptions,
        BinanceFuturesFundingRateRequest {
            start_time: None,
            end_time: None,
            limit: Some(3),
        },
    )
    .await
    .expect("fetch Binance USD-M Futures funding rates");

    for event in events {
        tracing::info!(?event, "received Binance USD-M Futures funding rate");
    }
}
