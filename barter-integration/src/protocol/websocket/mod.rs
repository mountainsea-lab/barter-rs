use crate::{Message, error::SocketError, protocol::StreamParser};
use bytes::Bytes;
use rustls::{ClientConfig, RootCertStore};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};
use tokio::net::TcpStream;
use tokio_socks::tcp::Socks5Stream;
use tokio_tungstenite::{
    Connector, client_async_tls_with_config, connect_async,
    tungstenite::{
        Utf8Bytes,
        client::IntoClientRequest,
        error::ProtocolError,
        protocol::{CloseFrame, frame::Frame},
    },
};
use tracing::debug;
use url::Url;
use webpki_roots::TLS_SERVER_ROOTS;

use super::WebSocketStreamExt;

/// Convenient type alias for a tungstenite `WebSocketStream`.
pub type WebSocket = Box<dyn WebSocketStreamExt>;

/// Convenient type alias for the `Sink` half of a tungstenite [`WebSocket`].
pub type WsSink = futures::stream::SplitSink<WebSocket, WsMessage>;

/// Convenient type alias for the `Stream` half of a tungstenite [`WebSocket`].
pub type WsStream = futures::stream::SplitStream<WebSocket>;

/// Communicative type alias for a tungstenite [`WebSocket`] `Message`.
pub type WsMessage = tokio_tungstenite::tungstenite::Message;

/// Communicative type alias for a tungstenite [`WebSocket`] `Error`.
pub type WsError = tokio_tungstenite::tungstenite::Error;

/// [`WebSocket`] administration message variants.
#[derive(Debug)]
pub enum AdminWs {
    Ping(Bytes),
    Pong(Bytes),
    Close(Option<CloseFrame>),
    WsError(WsError),
}

/// [`WebSocket`] parser.
///
/// Translates a `Result<WsMessage, WsError>` to a `Message<AdminWs, Bytes>`.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct WsParser;

impl WsParser {
    pub fn parse(ws_result: Result<WsMessage, WsError>) -> Message<AdminWs, Bytes> {
        match ws_result {
            Ok(WsMessage::Text(utf8)) => Message::Payload(Bytes::from(utf8)),
            Ok(WsMessage::Binary(bytes)) => Message::Payload(bytes),
            Ok(WsMessage::Frame(frame)) => Message::Payload(frame.into_payload()),
            Ok(WsMessage::Ping(bytes)) => Message::Admin(AdminWs::Ping(bytes)),
            Ok(WsMessage::Pong(bytes)) => Message::Admin(AdminWs::Pong(bytes)),
            Ok(WsMessage::Close(close)) => Message::Admin(AdminWs::Close(close)),
            Err(error) => Message::Admin(AdminWs::WsError(error)),
        }
    }
}

/// Default [`StreamParser`] implementation for a [`WebSocket`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct WebSocketSerdeParser;

impl<Output> StreamParser<Output> for WebSocketSerdeParser
where
    Output: for<'de> Deserialize<'de>,
{
    type Stream = WebSocket;
    type Message = WsMessage;
    type Error = WsError;

    fn parse(input: Result<Self::Message, Self::Error>) -> Option<Result<Output, SocketError>> {
        match input {
            Ok(ws_message) => match ws_message {
                WsMessage::Text(text) => process_text(text),
                WsMessage::Binary(binary) => process_binary(binary),
                WsMessage::Ping(ping) => process_ping(ping),
                WsMessage::Pong(pong) => process_pong(pong),
                WsMessage::Close(close_frame) => process_close_frame(close_frame),
                WsMessage::Frame(frame) => process_frame(frame),
            },
            Err(ws_err) => Some(Err(SocketError::WebSocket(Box::new(ws_err)))),
        }
    }
}

/// [`StreamParser`] implementation for a [`WebSocket`] that decodes protobuf
/// binary payloads.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct WebSocketProtobufParser;

impl<Output> StreamParser<Output> for WebSocketProtobufParser
where
    Output: prost::Message + Default,
{
    type Stream = WebSocket;
    type Message = WsMessage;
    type Error = WsError;

    fn parse(input: Result<Self::Message, Self::Error>) -> Option<Result<Output, SocketError>> {
        match input {
            Ok(ws_message) => match ws_message {
                WsMessage::Text(payload) => {
                    debug!(?payload, "received Text WebSocket message");
                    None
                }
                WsMessage::Binary(binary) => {
                    Some(Output::decode(binary.as_ref()).map_err(|error| {
                        SocketError::DeserialiseProtobuf {
                            error,
                            payload: binary.to_vec(),
                        }
                    }))
                }
                WsMessage::Ping(ping) => process_ping::<Output>(ping),
                WsMessage::Pong(pong) => process_pong::<Output>(pong),
                WsMessage::Close(close_frame) => process_close_frame::<Output>(close_frame),
                WsMessage::Frame(frame) => process_frame::<Output>(frame),
            },
            Err(ws_err) => Some(Err(SocketError::WebSocket(Box::new(ws_err)))),
        }
    }
}

/// Process a payload of `String` by deserialising into an `ExchangeMessage`.
pub fn process_text<ExchangeMessage>(
    payload: Utf8Bytes,
) -> Option<Result<ExchangeMessage, SocketError>>
where
    ExchangeMessage: for<'de> Deserialize<'de>,
{
    Some(
        serde_json::from_str::<ExchangeMessage>(&payload).map_err(|error| {
            debug!(
                ?error,
                ?payload,
                action = "returning Some(Err(err))",
                "failed to deserialize WebSocket Message into domain specific Message"
            );
            SocketError::Deserialise {
                error,
                payload: payload.to_string(),
            }
        }),
    )
}

/// Process a payload of `Vec<u8>` bytes by deserialising into an `ExchangeMessage`.
pub fn process_binary<ExchangeMessage>(
    payload: Bytes,
) -> Option<Result<ExchangeMessage, SocketError>>
where
    ExchangeMessage: for<'de> Deserialize<'de>,
{
    Some(
        serde_json::from_slice::<ExchangeMessage>(&payload).map_err(|error| {
            debug!(
                ?error,
                ?payload,
                action = "returning Some(Err(err))",
                "failed to deserialize WebSocket Message into domain specific Message"
            );
            SocketError::Deserialise {
                error,
                payload: String::from_utf8(payload.into()).unwrap_or_else(|x| x.to_string()),
            }
        }),
    )
}

/// Basic process for a [`WebSocket`] ping message. Logs the payload at `trace` level.
pub fn process_ping<ExchangeMessage>(ping: Bytes) -> Option<Result<ExchangeMessage, SocketError>> {
    debug!(payload = ?ping, "received Ping WebSocket message");
    None
}

/// Basic process for a [`WebSocket`] pong message. Logs the payload at `trace` level.
pub fn process_pong<ExchangeMessage>(pong: Bytes) -> Option<Result<ExchangeMessage, SocketError>> {
    debug!(payload = ?pong, "received Pong WebSocket message");
    None
}

/// Basic process for a [`WebSocket`] CloseFrame message. Logs the payload at `trace` level.
pub fn process_close_frame<ExchangeMessage>(
    close_frame: Option<CloseFrame>,
) -> Option<Result<ExchangeMessage, SocketError>> {
    let close_frame = format!("{close_frame:?}");
    debug!(payload = %close_frame, "received CloseFrame WebSocket message");
    Some(Err(SocketError::Terminated(close_frame)))
}

/// Basic process for a [`WebSocket`] Frame message. Logs the payload at `trace` level.
pub fn process_frame<ExchangeMessage>(
    frame: Frame,
) -> Option<Result<ExchangeMessage, SocketError>> {
    let frame = format!("{frame:?}");
    debug!(payload = %frame, "received unexpected Frame WebSocket message");
    None
}

/// Connect asynchronously to a [`WebSocket`] server, falling back to a local SOCKS5 proxy if direct
/// connection fails.
pub async fn connect<R>(request: R) -> Result<WebSocket, SocketError>
where
    R: IntoClientRequest + Unpin + Debug,
{
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("Failed to install ring crypto provider");
    });

    debug!(?request, "attempting to establish WebSocket connection");

    let req = request.into_client_request()?;
    let uri = req.uri();
    let url = Url::parse(&uri.to_string())?;

    if let Ok((stream, _)) = connect_async(url.as_str()).await {
        return Ok(Box::new(stream));
    }

    let host = uri
        .host()
        .ok_or_else(|| SocketError::Subscribe("websocket request missing host".to_owned()))?;
    let port = uri.port_u16().unwrap_or_else(|| {
        if uri.scheme_str() == Some("wss") {
            443
        } else {
            80
        }
    });
    let target_host = (host, port);
    let proxy_addr = "127.0.0.1:8888";

    debug!(
        ?target_host,
        proxy_addr, "attempting proxied WebSocket connection"
    );

    let socks_stream = Socks5Stream::<TcpStream>::connect(proxy_addr, target_host).await?;

    let mut root_store = RootCertStore::empty();
    root_store.extend(TLS_SERVER_ROOTS.iter().cloned());

    let tls_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let tls_connector = Connector::Rustls(Arc::new(tls_config));

    let (stream, _) =
        client_async_tls_with_config(url, socks_stream, None, Some(tls_connector)).await?;

    Ok(Box::new(stream))
}

/// Determine whether a [`WsError`] indicates the [`WebSocket`] has disconnected.
pub fn is_websocket_disconnected(error: &WsError) -> bool {
    matches!(
        error,
        WsError::ConnectionClosed
            | WsError::AlreadyClosed
            | WsError::Io(_)
            | WsError::Protocol(ProtocolError::SendAfterClosing)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    #[test]
    fn test_ws_parser_text_message() {
        let msg = Ok(WsMessage::Text("hello".into()));
        let result = WsParser::parse(msg);
        assert!(matches!(result, Message::Payload(bytes) if bytes == Bytes::from("hello")));
    }

    #[test]
    fn test_ws_parser_binary_message() {
        let msg = Ok(WsMessage::Binary(Bytes::from_static(b"\x01\x02")));
        let result = WsParser::parse(msg);
        assert!(
            matches!(result, Message::Payload(bytes) if bytes == Bytes::from_static(b"\x01\x02"))
        );
    }

    #[test]
    fn test_ws_parser_ping() {
        let msg = Ok(WsMessage::Ping(Bytes::from_static(b"ping")));
        let result = WsParser::parse(msg);
        assert!(
            matches!(result, Message::Admin(AdminWs::Ping(bytes)) if bytes == Bytes::from_static(b"ping"))
        );
    }

    #[test]
    fn test_ws_parser_pong() {
        let msg = Ok(WsMessage::Pong(Bytes::from_static(b"pong")));
        let result = WsParser::parse(msg);
        assert!(
            matches!(result, Message::Admin(AdminWs::Pong(bytes)) if bytes == Bytes::from_static(b"pong"))
        );
    }

    #[test]
    fn test_ws_parser_close() {
        let close = CloseFrame {
            code: CloseCode::Normal,
            reason: "bye".into(),
        };
        let msg = Ok(WsMessage::Close(Some(close)));
        let result = WsParser::parse(msg);
        assert!(matches!(result, Message::Admin(AdminWs::Close(Some(_)))));
    }

    #[test]
    fn test_ws_parser_error() {
        let msg = Err(WsError::ConnectionClosed);
        let result = WsParser::parse(msg);
        assert!(matches!(result, Message::Admin(AdminWs::WsError(_))));
    }
}
