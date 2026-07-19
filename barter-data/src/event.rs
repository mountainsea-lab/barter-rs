use crate::{
    error::DataError,
    streams::consumer::MarketStreamResult,
    subscription::{
        book::{OrderBookEvent, OrderBookL1},
        candle::Candle,
        funding::FundingRate,
        index_price::IndexPrice,
        liquidation::Liquidation,
        mark_price::MarkPrice,
        open_interest::OpenInterest,
        trade::PublicTrade,
    },
};
use barter_instrument::{exchange::ExchangeId, instrument::market_data::MarketDataInstrument};
use chrono::{DateTime, Utc};
use derive_more::From;
use serde::{Deserialize, Serialize};

/// Convenient new type containing a collection of [`MarketEvent<T>`](MarketEvent)s.
#[derive(Debug)]
pub struct MarketIter<InstrumentKey, T>(pub Vec<Result<MarketEvent<InstrumentKey, T>, DataError>>);

impl<InstrumentKey, T> FromIterator<Result<MarketEvent<InstrumentKey, T>, DataError>>
    for MarketIter<InstrumentKey, T>
{
    fn from_iter<Iter>(iter: Iter) -> Self
    where
        Iter: IntoIterator<Item = Result<MarketEvent<InstrumentKey, T>, DataError>>,
    {
        Self(iter.into_iter().collect())
    }
}

/// Normalised Barter [`MarketEvent<T>`](Self) wrapping the `T` data variant in metadata.
///
/// Note: `T` can be an enum such as the [`DataKind`] if required.
///
/// See [`crate::subscription`] for all existing Barter Market event variants.
///
/// ### Examples
/// - [`MarketEvent<PublicTrade>`](PublicTrade)
/// - [`MarketEvent<OrderBookL1>`](OrderBookL1)
/// - [`MarketEvent<DataKind>`](DataKind)
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct MarketEvent<InstrumentKey = MarketDataInstrument, T = DataKind> {
    pub time_exchange: DateTime<Utc>,
    pub time_received: DateTime<Utc>,
    pub exchange: ExchangeId,
    pub instrument: InstrumentKey,
    pub kind: T,
}

impl<InstrumentKey, T> MarketEvent<InstrumentKey, T> {
    pub fn map_kind<F, O>(self, op: F) -> MarketEvent<InstrumentKey, O>
    where
        F: FnOnce(T) -> O,
    {
        MarketEvent {
            time_exchange: self.time_exchange,
            time_received: self.time_received,
            exchange: self.exchange,
            instrument: self.instrument,
            kind: op(self.kind),
        }
    }
}

impl<InstrumentKey> MarketEvent<InstrumentKey, DataKind> {
    pub fn as_public_trade(&self) -> Option<MarketEvent<&InstrumentKey, &PublicTrade>> {
        match &self.kind {
            DataKind::Trade(public_trade) => Some(self.as_event(public_trade)),
            _ => None,
        }
    }

    pub fn as_order_book_l1(&self) -> Option<MarketEvent<&InstrumentKey, &OrderBookL1>> {
        match &self.kind {
            DataKind::OrderBookL1(orderbook) => Some(self.as_event(orderbook)),
            _ => None,
        }
    }

    pub fn as_order_book(&self) -> Option<MarketEvent<&InstrumentKey, &OrderBookEvent>> {
        match &self.kind {
            DataKind::OrderBook(orderbook) => Some(self.as_event(orderbook)),
            _ => None,
        }
    }

    pub fn as_candle(&self) -> Option<MarketEvent<&InstrumentKey, &Candle>> {
        match &self.kind {
            DataKind::Candle(candle) => Some(self.as_event(candle)),
            _ => None,
        }
    }

    pub fn as_funding_rate(&self) -> Option<MarketEvent<&InstrumentKey, &FundingRate>> {
        match &self.kind {
            DataKind::FundingRate(funding_rate) => Some(self.as_event(funding_rate)),
            _ => None,
        }
    }

    pub fn as_mark_price(&self) -> Option<MarketEvent<&InstrumentKey, &MarkPrice>> {
        match &self.kind {
            DataKind::MarkPrice(mark_price) => Some(self.as_event(mark_price)),
            _ => None,
        }
    }

    pub fn as_index_price(&self) -> Option<MarketEvent<&InstrumentKey, &IndexPrice>> {
        match &self.kind {
            DataKind::IndexPrice(index_price) => Some(self.as_event(index_price)),
            _ => None,
        }
    }

    pub fn as_open_interest(&self) -> Option<MarketEvent<&InstrumentKey, &OpenInterest>> {
        match &self.kind {
            DataKind::OpenInterest(open_interest) => Some(self.as_event(open_interest)),
            _ => None,
        }
    }

    pub fn as_liquidation(&self) -> Option<MarketEvent<&InstrumentKey, &Liquidation>> {
        match &self.kind {
            DataKind::Liquidation(liquidation) => Some(self.as_event(liquidation)),
            _ => None,
        }
    }

    fn as_event<'a, K>(&'a self, kind: &'a K) -> MarketEvent<&'a InstrumentKey, &'a K> {
        MarketEvent {
            time_exchange: self.time_exchange,
            time_received: self.time_received,
            exchange: self.exchange,
            instrument: &self.instrument,
            kind,
        }
    }
}

/// Available kinds of normalised Barter [`MarketEvent<T>`](MarketEvent).
///
/// ### Notes
/// - [`Self`] is only used as the [`MarketEvent<DataKind>`](MarketEvent) `Output` when combining
///   several [`Streams<SubscriptionKind::Event>`](crate::streams::Streams) using the
///   [`MultiStreamBuilder<Output>`](crate::streams::builder::multi::MultiStreamBuilder), or via
///   the [`DynamicStreams::select_all`](crate::streams::builder::dynamic::DynamicStreams) method.
/// - [`Self`] is purposefully not supported in any
///   [`Subscription`](crate::subscription::Subscription)s directly, it is only used to
///   make ergonomic [`Streams`](crate::streams::Streams) containing many
///   [`MarketEvent<T>`](MarketEvent) kinds.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize, From)]
pub enum DataKind {
    Trade(PublicTrade),
    OrderBookL1(OrderBookL1),
    OrderBook(OrderBookEvent),
    Candle(Candle),
    FundingRate(FundingRate),
    MarkPrice(MarkPrice),
    IndexPrice(IndexPrice),
    OpenInterest(OpenInterest),
    Liquidation(Liquidation),
}

impl DataKind {
    pub fn kind_name(&self) -> &str {
        match self {
            DataKind::Trade(_) => "public_trade",
            DataKind::OrderBookL1(_) => "l1",
            DataKind::OrderBook(_) => "l2",
            DataKind::Candle(_) => "candle",
            DataKind::FundingRate(_) => "funding_rate",
            DataKind::MarkPrice(_) => "mark_price",
            DataKind::IndexPrice(_) => "index_price",
            DataKind::OpenInterest(_) => "open_interest",
            DataKind::Liquidation(_) => "liquidation",
        }
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, PublicTrade>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, PublicTrade>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, PublicTrade>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, PublicTrade>) -> Self {
        value.map_kind(PublicTrade::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, OrderBookL1>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, OrderBookL1>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, OrderBookL1>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, OrderBookL1>) -> Self {
        value.map_kind(OrderBookL1::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, OrderBookEvent>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, OrderBookEvent>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, OrderBookEvent>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, OrderBookEvent>) -> Self {
        value.map_kind(OrderBookEvent::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, Candle>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, Candle>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, Candle>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, Candle>) -> Self {
        value.map_kind(Candle::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, FundingRate>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, FundingRate>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, FundingRate>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, FundingRate>) -> Self {
        value.map_kind(FundingRate::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, MarkPrice>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, MarkPrice>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, MarkPrice>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, MarkPrice>) -> Self {
        value.map_kind(MarkPrice::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, IndexPrice>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, IndexPrice>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, IndexPrice>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, IndexPrice>) -> Self {
        value.map_kind(IndexPrice::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, OpenInterest>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, OpenInterest>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, OpenInterest>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, OpenInterest>) -> Self {
        value.map_kind(OpenInterest::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, Liquidation>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, Liquidation>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, Liquidation>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, Liquidation>) -> Self {
        value.map_kind(Liquidation::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn funding_rate_event_converts_to_data_kind() {
        let time = Utc::now();
        let event = MarketEvent {
            time_exchange: time,
            time_received: time,
            exchange: ExchangeId::BinanceFuturesUsd,
            instrument: "btc-usdt-perp",
            kind: FundingRate {
                funding_time: time,
                funding_rate: dec!(0.00010000),
                mark_price: Some(dec!(65000.25)),
            },
        };

        let data_kind_event = MarketEvent::<_, DataKind>::from(event);

        assert_eq!(data_kind_event.kind.kind_name(), "funding_rate");
        let funding_rate_event = data_kind_event.as_funding_rate().unwrap();
        assert_eq!(funding_rate_event.kind.funding_rate, dec!(0.00010000));
        assert_eq!(funding_rate_event.kind.mark_price, Some(dec!(65000.25)));
    }

    #[test]
    fn mark_price_event_converts_to_data_kind() {
        let time = Utc::now();
        let event = MarketEvent {
            time_exchange: time,
            time_received: time,
            exchange: ExchangeId::BinanceFuturesUsd,
            instrument: "btc-usdt-perp",
            kind: MarkPrice {
                event_time: time,
                mark_price: dec!(11793.63104562),
                index_price: dec!(11791.23456789),
                estimated_settle_price: Some(dec!(11790.11111111)),
                last_funding_rate: Some(dec!(0.00010000)),
                interest_rate: Some(dec!(0.00010000)),
                next_funding_time: Some(time),
            },
        };

        let data_kind_event = MarketEvent::<_, DataKind>::from(event);

        assert_eq!(data_kind_event.kind.kind_name(), "mark_price");
        let mark_price_event = data_kind_event.as_mark_price().unwrap();
        assert_eq!(mark_price_event.kind.mark_price, dec!(11793.63104562));
        assert_eq!(mark_price_event.kind.index_price, dec!(11791.23456789));
    }

    #[test]
    fn index_price_event_converts_to_data_kind() {
        let time = Utc::now();
        let event = MarketEvent {
            time_exchange: time,
            time_received: time,
            exchange: ExchangeId::BinanceFuturesUsd,
            instrument: "btc-usdt-perp",
            kind: IndexPrice {
                event_time: time,
                index_price: dec!(11791.23456789),
            },
        };

        let data_kind_event = MarketEvent::<_, DataKind>::from(event);

        assert_eq!(data_kind_event.kind.kind_name(), "index_price");
        let index_price_event = data_kind_event.as_index_price().unwrap();
        assert_eq!(index_price_event.kind.event_time, time);
        assert_eq!(index_price_event.kind.index_price, dec!(11791.23456789));
    }

    #[test]
    fn data_kind_supports_open_interest_events() {
        use crate::subscription::open_interest::OpenInterest;

        let event_time = Utc::now();
        let input = MarketEvent {
            time_exchange: event_time,
            time_received: event_time,
            exchange: ExchangeId::BinanceFuturesUsd,
            instrument: "BTCUSDT",
            kind: OpenInterest {
                event_time,
                open_interest: dec!(10659.509),
            },
        };

        let actual = MarketEvent::<_, DataKind>::from(input);

        assert_eq!(actual.kind.kind_name(), "open_interest");
        let open_interest = actual.as_open_interest().unwrap();
        assert_eq!(open_interest.kind.open_interest, dec!(10659.509));
    }
}
