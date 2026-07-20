use barter_data::exchange::binance::futures::instrument::fetch_exchange_info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let exchange_info = fetch_exchange_info()
        .await
        .expect("fetch Binance USD-M Futures exchangeInfo");
    let instruments = exchange_info.usd_m_perpetual_instruments();

    tracing::info!(
        symbols = exchange_info.symbols.len(),
        instruments = instruments.len(),
        "received Binance USD-M Futures exchangeInfo"
    );

    for symbol in exchange_info.symbols.iter().take(5) {
        tracing::info!(
            raw_symbol = %symbol.symbol,
            pair = %symbol.pair,
            contract_type = %symbol.contract_type,
            status = %symbol.status,
            "discovered raw Binance USD-M Futures symbol"
        );
    }

    for instrument in instruments.iter().take(5) {
        tracing::info!(?instrument, "discovered Binance USD-M Futures instrument");
    }
}
