//! Message channels: the transport-neutral pipe sessions and feeds run over.

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;

use crate::ServiceError;
use crate::codec::{from_cbor, to_cbor};

/// The sending half of a channel: one message per call.
#[async_trait]
pub trait FrameTx: Send {
    async fn send(&mut self, message: Vec<u8>) -> Result<(), ServiceError>;
}

/// The receiving half of a channel. `recv` must be cancel-safe (it is raced in `select!`):
/// back it with a reader task and a queue. `Ok(None)` means the peer closed its side.
#[async_trait]
pub trait FrameRx: Send {
    async fn recv(&mut self) -> Result<Option<Vec<u8>>, ServiceError>;
}

/// A two-way, ordered, reliable message channel to one peer.
pub struct Channel {
    pub tx: Box<dyn FrameTx>,
    pub rx: Box<dyn FrameRx>,
}

impl Channel {
    pub fn new(tx: Box<dyn FrameTx>, rx: Box<dyn FrameRx>) -> Self {
        Self { tx, rx }
    }

    /// Sends one CBOR message.
    pub async fn send_cbor<T: Serialize + Sync>(&mut self, msg: &T) -> Result<(), ServiceError> {
        let bytes = to_cbor(msg)?;
        self.tx.send(bytes).await
    }

    /// Receives one CBOR message; `None` when the peer closed.
    pub async fn recv_cbor<T: DeserializeOwned>(&mut self) -> Result<Option<T>, ServiceError> {
        match self.rx.recv().await? {
            None => Ok(None),
            Some(bytes) => from_cbor(&bytes).map(Some),
        }
    }
}

/// Opens channels to servers. [`NetDialer`](crate::net::NetDialer) does it over
/// `weirdnet-net`. A server that does not serve `protocol` answers
/// [`ServiceError::Unsupported`].
#[async_trait]
pub trait Dialer: Send + Sync {
    async fn open(&self, node: &str, protocol: &str) -> Result<Channel, ServiceError>;
}

struct MemTx(mpsc::Sender<Vec<u8>>);
struct MemRx(mpsc::Receiver<Vec<u8>>);

#[async_trait]
impl FrameTx for MemTx {
    async fn send(&mut self, message: Vec<u8>) -> Result<(), ServiceError> {
        self.0.send(message).await.map_err(|_| ServiceError::Closed)
    }
}

#[async_trait]
impl FrameRx for MemRx {
    async fn recv(&mut self) -> Result<Option<Vec<u8>>, ServiceError> {
        Ok(self.0.recv().await)
    }
}

/// Two connected in-memory channel ends, each buffering `capacity` messages. For tests and
/// in-process clients.
pub fn memory_pair(capacity: usize) -> (Channel, Channel) {
    let (a_tx, b_rx) = mpsc::channel(capacity.max(1));
    let (b_tx, a_rx) = mpsc::channel(capacity.max(1));
    (Channel::new(Box::new(MemTx(a_tx)), Box::new(MemRx(a_rx))), Channel::new(Box::new(MemTx(b_tx)), Box::new(MemRx(b_rx))))
}
