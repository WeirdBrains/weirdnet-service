//! The `net` feature: serving and dialing over `weirdnet-net` streams.
//!
//! [`Server::builder`] registers services and feeds on a node, each on its own
//! [`ProtocolId`], and optionally announces them as an app (`Node::serve_app`) so clients can
//! see what the node serves. Each accepted stream is served with its Noise-authenticated peer
//! as the caller. [`NetDialer`] opens streams for a [`Client`](crate::Client).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use weirdnet_net::member::Membership;
use weirdnet_net::node::Message;
use weirdnet_net::user::{Card, NodeCert};
use weirdnet_net::{AppBody, AppResponse, Incoming, Limits, Multiaddr, NetError, Node, PeerId, Stream};

use crate::codec::{FrameDecoder, MAX_FRAME, encode_frame, from_cbor, to_cbor};
use crate::feed::AccessFn;
use crate::{
    Access, Authorize, Channel, Credentials, Dialer, FeedOptions, FrameRx, FrameTx, Hub, ProtocolId, Service, ServiceError, SessionOptions, State,
    Topic,
};

/// Stream limits for services and feeds: `weirdnet-net` defaults (the stream survives a path
/// switch for 30 s), no idle reset (feeds send heartbeats instead).
pub fn stream_limits() -> Limits {
    Limits { idle: None, ..Limits::default() }
}

impl Credentials {
    /// Presents this device's certificate.
    pub fn with_node_cert(self, cert: &NodeCert) -> Self {
        self.with_cert(to_cbor(cert).unwrap_or_default())
    }

    /// Presents the user's card (it must certify this device).
    pub fn with_user_card(self, card: &Card) -> Self {
        self.with_card(to_cbor(card).unwrap_or_default())
    }

    /// Presents a membership.
    pub fn with_membership_of(self, membership: &Membership) -> Self {
        self.with_membership(to_cbor(membership).unwrap_or_default())
    }

    /// The presented certificate, decoded.
    pub fn node_cert(&self) -> Option<Result<NodeCert, ServiceError>> {
        self.cert().map(from_cbor)
    }

    /// The presented card, decoded.
    pub fn user_card(&self) -> Option<Result<Card, ServiceError>> {
        self.card().map(from_cbor)
    }
}

struct Tx(mpsc::Sender<Vec<u8>>);
struct Rx(mpsc::Receiver<Result<Vec<u8>, ServiceError>>);

#[async_trait]
impl FrameTx for Tx {
    async fn send(&mut self, message: Vec<u8>) -> Result<(), ServiceError> {
        self.0.send(message).await.map_err(|_| ServiceError::Closed)
    }
}

#[async_trait]
impl FrameRx for Rx {
    async fn recv(&mut self) -> Result<Option<Vec<u8>>, ServiceError> {
        match self.0.recv().await {
            Some(Ok(m)) => Ok(Some(m)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

/// A [`Channel`] over a stream, with length-prefixed messages of at most [`MAX_FRAME`]
/// bytes. A task drives the stream; dropping the channel closes our side. Call from a Tokio
/// runtime.
pub fn channel(stream: Stream) -> Channel {
    channel_with(stream, MAX_FRAME)
}

/// [`channel`] with another message size limit.
pub fn channel_with(stream: Stream, max_frame: usize) -> Channel {
    let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(16);
    let (in_tx, in_rx) = mpsc::channel::<Result<Vec<u8>, ServiceError>>(64);
    tokio::spawn(async move {
        let (mut r, mut w) = stream.split();
        let mut decoder = FrameDecoder::new(max_frame);
        let mut buf = vec![0u8; 64 * 1024];
        let mut writing = true;
        let mut reader = Some(in_tx);
        loop {
            if !writing && reader.as_ref().is_none_or(|t| t.is_closed()) {
                break;
            }
            tokio::select! {
                m = out_rx.recv(), if writing => match m {
                    Some(message) => {
                        let ok = match encode_frame(&message, max_frame) {
                            Ok(bytes) => w.write_all(&bytes).await.is_ok() && w.flush().await.is_ok(),
                            Err(_) => false,
                        };
                        if !ok {
                            writing = false;
                            if let Some(t) = reader.take() {
                                let _ = t.send(Err(ServiceError::Closed)).await;
                            }
                        }
                    }
                    None => {
                        writing = false;
                        let _ = w.close().await;
                    }
                },
                n = r.read(&mut buf), if reader.is_some() => match n {
                    Ok(0) => reader = None,
                    Ok(n) => {
                        decoder.push(&buf[..n]);
                        loop {
                            match decoder.next_frame() {
                                Ok(Some(m)) => {
                                    if let Some(t) = &reader && t.send(Ok(m)).await.is_err() {
                                        reader = None;
                                        break;
                                    }
                                }
                                Ok(None) => break,
                                Err(e) => {
                                    if let Some(t) = reader.take() {
                                        let _ = t.send(Err(e)).await;
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if let Some(t) = reader.take() {
                            let _ = t.send(Err(ServiceError::Unreachable { message: e.to_string() })).await;
                        }
                    }
                },
                _ = async { match &reader { Some(t) => t.closed().await, None => std::future::pending().await } }, if !writing => {
                    reader = None;
                }
            }
        }
    });
    Channel::new(Box::new(Tx(out_tx)), Box::new(Rx(in_rx)))
}

/// A stream open error: a peer that does not accept the protocol is
/// [`ServiceError::Unsupported`], anything else unreachable.
pub fn open_error(e: NetError) -> ServiceError {
    let message = e.to_string();
    if message.contains("does not accept") { ServiceError::Unsupported { message } } else { ServiceError::Unreachable { message } }
}

/// Opens streams from a node. Peers are found the node's usual way; [`hint`](Self::hint)
/// adds addresses to try (LAN, fixed or relay circuit addresses).
pub struct NetDialer<Q: Message, R: Message> {
    node: Arc<Node<Q, R>>,
    hints: Mutex<HashMap<PeerId, Vec<Multiaddr>>>,
}

impl<Q: Message, R: Message> NetDialer<Q, R> {
    pub fn new(node: Arc<Node<Q, R>>) -> Self {
        Self { node, hints: Mutex::new(HashMap::new()) }
    }

    pub fn hint(&self, peer: PeerId, addrs: Vec<Multiaddr>) {
        self.hints.lock().unwrap_or_else(|e| e.into_inner()).insert(peer, addrs);
    }
}

#[async_trait]
impl<Q: Message, R: Message> Dialer for NetDialer<Q, R> {
    async fn open(&self, node: &str, protocol: &str) -> Result<Channel, ServiceError> {
        let peer: PeerId = node.parse().map_err(|_| ServiceError::Unreachable { message: format!("{node} is not a peer id") })?;
        let addrs = self.hints.lock().unwrap_or_else(|e| e.into_inner()).get(&peer).cloned().unwrap_or_default();
        let stream = self.node.open_stream_async(peer, &addrs, protocol, stream_limits()).await.map_err(open_error)?;
        Ok(channel(stream))
    }
}

type Start<Q, R> = Box<dyn FnOnce(&Arc<Node<Q, R>>) -> Result<tokio::task::JoinHandle<()>, ServiceError> + Send>;

/// Registers what a [`Server`] serves.
pub struct ServerBuilder<Q: Message, R: Message> {
    node: Arc<Node<Q, R>>,
    starts: Vec<Start<Q, R>>,
    protocols: Vec<ProtocolId>,
    announce: Option<String>,
}

fn accept_loop(mut incoming: Incoming, serve: impl Fn(Channel, String) + Send + 'static) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(stream) = incoming.next().await {
            let peer = stream.peer().to_string();
            serve(channel(stream), peer);
        }
    })
}

fn accept<Q: Message, R: Message>(node: &Arc<Node<Q, R>>, protocol: &ProtocolId) -> Result<Incoming, ServiceError> {
    node.accept_streams(protocol.as_str(), stream_limits()).map_err(|e| ServiceError::failed(e.to_string()))
}

impl<Q: Message, R: Message> ServerBuilder<Q, R> {
    /// Serves sessions of `service` on `protocol`, each call checked by `auth`.
    pub fn service<S: Service>(mut self, protocol: &ProtocolId, service: Arc<S>, auth: Arc<dyn Authorize>, options: SessionOptions) -> Self {
        let p = protocol.clone();
        self.protocols.push(p.clone());
        self.starts.push(Box::new(move |node| {
            let incoming = accept(node, &p)?;
            let options = Arc::new(options);
            Ok(accept_loop(incoming, move |ch, peer| {
                let (svc, auth, options) = (service.clone(), auth.clone(), options.clone());
                tokio::spawn(async move {
                    let _ = crate::serve_session(svc, auth, ch, peer, &options).await;
                });
            }))
        }));
        self
    }

    /// Serves subscriptions to `hub`'s topics on `protocol`. `access` says what a topic needs;
    /// `auth` decides.
    pub fn feed<T: Topic, S: State>(
        mut self,
        protocol: &ProtocolId,
        hub: Arc<Hub<T, S>>,
        access: impl Fn(&T) -> Access + Send + Sync + 'static,
        auth: Arc<dyn Authorize>,
        options: FeedOptions,
    ) -> Self {
        let p = protocol.clone();
        self.protocols.push(p.clone());
        let access: AccessFn<T> = Arc::new(access);
        self.starts.push(Box::new(move |node| {
            let incoming = accept(node, &p)?;
            let options = Arc::new(options);
            Ok(accept_loop(incoming, move |ch, peer| {
                let (hub, access, auth, options) = (hub.clone(), access.clone(), auth.clone(), options.clone());
                tokio::spawn(async move {
                    let _ = crate::serve_feed(hub, access, auth, ch, peer, &options).await;
                });
            }))
        }));
        self
    }

    /// Announces `app` (`Node::serve_app`); its answer is the CBOR list of protocol ids served.
    pub fn announce(mut self, app: &str) -> Self {
        self.announce = Some(app.to_string());
        self
    }

    /// Starts accepting. Call from a Tokio runtime. Two registrations on one protocol id fail.
    pub fn start(self) -> Result<Server, ServiceError> {
        let mut seen = std::collections::HashSet::new();
        if let Some(dup) = self.protocols.iter().find(|p| !seen.insert(p.as_str())) {
            return Err(ServiceError::bad_request(format!("{dup} is registered twice")));
        }
        let mut server = Server { tasks: Vec::new(), stop: None, protocols: self.protocols.clone() };
        for s in self.starts {
            server.tasks.push(s(&self.node)?);
        }
        if let Some(app) = self.announce {
            let ids: Vec<&str> = self.protocols.iter().map(|p| p.as_str()).collect();
            let body = to_cbor(&ids)?;
            self.node
                .serve_app(
                    &app,
                    Arc::new(move |_peer, _req| {
                        let body = body.clone();
                        async move { Ok(AppResponse::new(200).header("content-type", "application/cbor").body(AppBody::bytes(body))) }
                    }),
                )
                .map_err(|e| ServiceError::failed(e.to_string()))?;
            let node = self.node.clone();
            server.stop = Some(Box::new(move || {
                node.stop_serving_app(&app);
            }));
        }
        Ok(server)
    }
}

/// Services and feeds served on a node. Dropping it stops accepting new streams and the
/// announcement; sessions and subscriptions under way carry on until they end.
pub struct Server {
    tasks: Vec<tokio::task::JoinHandle<()>>,
    stop: Option<Box<dyn FnOnce() + Send>>,
    protocols: Vec<ProtocolId>,
}

impl Server {
    pub fn builder<Q: Message, R: Message>(node: &Arc<Node<Q, R>>) -> ServerBuilder<Q, R> {
        ServerBuilder { node: node.clone(), starts: Vec::new(), protocols: Vec::new(), announce: None }
    }

    /// The protocol ids served, in registration order.
    pub fn protocols(&self) -> &[ProtocolId] {
        &self.protocols
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
        if let Some(stop) = self.stop.take() {
            stop();
        }
    }
}
