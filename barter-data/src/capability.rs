use crate::subscription::SubKind;
use barter_instrument::{
    exchange::ExchangeId, instrument::market_data::kind::MarketDataInstrumentKind,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ProviderCapabilityDescriptor {
    pub exchange: ExchangeId,
    pub instrument_kinds: &'static [MarketDataInstrumentKind],
    pub capabilities: &'static [DataCapability],
    pub reference_capabilities: &'static [ReferenceCapability],
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DataCapability {
    pub sub_kind: SubKind,
    pub data_kind: CapabilityDataKind,
    pub transports: &'static [TransportKind],
    pub dynamic_stream: DynamicStreamCapability,
    pub history: HistoryCapability,
    pub notes: &'static str,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ReferenceCapability {
    pub data_kind: CapabilityDataKind,
    pub transports: &'static [TransportKind],
    pub history: HistoryCapability,
    pub notes: &'static str,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CapabilityDataKind {
    PublicTrade,
    OrderBookL1,
    OrderBookL2,
    Candle,
    Liquidation,
    MarkPrice,
    IndexPrice,
    FundingRate,
    OpenInterest,
    Instrument,
    TakerFlow,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TransportKind {
    WebSocket,
    RestFetch,
    RestPoll,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DynamicStreamCapability {
    Supported,
    Unsupported,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum HistoryCapability {
    Unsupported,
    LatestOnly,
    RecentWindow,
    HistoricalRange,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CapabilityErrorKind {
    UnsupportedCapability,
    UnsupportedDynamicStream,
    RestFetchFailed,
    DecodeFailed,
    TransportDisconnected,
}

const BINANCE_FUTURES_USD_INSTRUMENT_KINDS: &[MarketDataInstrumentKind] =
    &[MarketDataInstrumentKind::Perpetual];

const WS: &[TransportKind] = &[TransportKind::WebSocket];
const REST_FETCH: &[TransportKind] = &[TransportKind::RestFetch];
const WS_REST_FETCH: &[TransportKind] = &[TransportKind::WebSocket, TransportKind::RestFetch];

const BINANCE_FUTURES_USD_CAPABILITIES: &[DataCapability] = &[
    DataCapability {
        sub_kind: SubKind::PublicTrades,
        data_kind: CapabilityDataKind::PublicTrade,
        transports: WS,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::Unsupported,
        notes: "",
    },
    DataCapability {
        sub_kind: SubKind::OrderBooksL1,
        data_kind: CapabilityDataKind::OrderBookL1,
        transports: WS,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::Unsupported,
        notes: "",
    },
    DataCapability {
        sub_kind: SubKind::OrderBooksL2,
        data_kind: CapabilityDataKind::OrderBookL2,
        transports: WS,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::Unsupported,
        notes: "",
    },
    DataCapability {
        sub_kind: SubKind::Candles,
        data_kind: CapabilityDataKind::Candle,
        transports: WS,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::Unsupported,
        notes: "",
    },
    DataCapability {
        sub_kind: SubKind::Liquidations,
        data_kind: CapabilityDataKind::Liquidation,
        transports: WS,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::Unsupported,
        notes: "",
    },
    DataCapability {
        sub_kind: SubKind::MarkPrices,
        data_kind: CapabilityDataKind::MarkPrice,
        transports: WS_REST_FETCH,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::LatestOnly,
        notes: "",
    },
    DataCapability {
        sub_kind: SubKind::IndexPrices,
        data_kind: CapabilityDataKind::IndexPrice,
        transports: WS_REST_FETCH,
        dynamic_stream: DynamicStreamCapability::Supported,
        history: HistoryCapability::LatestOnly,
        notes: "",
    },
    DataCapability {
        sub_kind: SubKind::FundingRates,
        data_kind: CapabilityDataKind::FundingRate,
        transports: REST_FETCH,
        dynamic_stream: DynamicStreamCapability::Unsupported,
        history: HistoryCapability::HistoricalRange,
        notes: "",
    },
    DataCapability {
        sub_kind: SubKind::OpenInterests,
        data_kind: CapabilityDataKind::OpenInterest,
        transports: REST_FETCH,
        dynamic_stream: DynamicStreamCapability::Unsupported,
        history: HistoryCapability::LatestOnly,
        notes: "",
    },
    DataCapability {
        sub_kind: SubKind::TakerFlows,
        data_kind: CapabilityDataKind::TakerFlow,
        transports: REST_FETCH,
        dynamic_stream: DynamicStreamCapability::Unsupported,
        history: HistoryCapability::RecentWindow,
        notes: "",
    },
];

const BINANCE_FUTURES_USD_REFERENCE_CAPABILITIES: &[ReferenceCapability] = &[ReferenceCapability {
    data_kind: CapabilityDataKind::Instrument,
    transports: REST_FETCH,
    history: HistoryCapability::LatestOnly,
    notes: "",
}];

pub const BINANCE_FUTURES_USD_DESCRIPTOR: ProviderCapabilityDescriptor =
    ProviderCapabilityDescriptor {
        exchange: ExchangeId::BinanceFuturesUsd,
        instrument_kinds: BINANCE_FUTURES_USD_INSTRUMENT_KINDS,
        capabilities: BINANCE_FUTURES_USD_CAPABILITIES,
        reference_capabilities: BINANCE_FUTURES_USD_REFERENCE_CAPABILITIES,
    };

const PROVIDER_CAPABILITIES: &[ProviderCapabilityDescriptor] = &[BINANCE_FUTURES_USD_DESCRIPTOR];

pub fn provider_capabilities() -> &'static [ProviderCapabilityDescriptor] {
    PROVIDER_CAPABILITIES
}

pub fn provider_capability(exchange: ExchangeId) -> Option<&'static ProviderCapabilityDescriptor> {
    PROVIDER_CAPABILITIES
        .iter()
        .find(|descriptor| descriptor.exchange == exchange)
}

pub fn supports_capability(
    exchange: ExchangeId,
    instrument_kind: MarketDataInstrumentKind,
    sub_kind: SubKind,
) -> bool {
    provider_capability(exchange).is_some_and(|descriptor| {
        descriptor.instrument_kinds.contains(&instrument_kind)
            && descriptor
                .capabilities
                .iter()
                .any(|capability| capability.sub_kind == sub_kind)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_instrument::{
        exchange::ExchangeId, instrument::market_data::kind::MarketDataInstrumentKind,
    };

    #[test]
    fn binance_futures_usd_descriptor_exists() {
        let descriptor = provider_capability(ExchangeId::BinanceFuturesUsd)
            .expect("Binance USD-M Futures capability descriptor should exist");

        assert_eq!(descriptor.exchange, ExchangeId::BinanceFuturesUsd);
        assert!(
            descriptor
                .instrument_kinds
                .contains(&MarketDataInstrumentKind::Perpetual),
            "Binance USD-M Futures descriptor should declare perpetual instruments"
        );
    }
}
