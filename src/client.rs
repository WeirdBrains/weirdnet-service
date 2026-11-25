use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{Credentials, Dialer, Live, ProtocolId, ServiceError, Session, State, Subscription, Topic};

/// Opens sessions and subscriptions to servers through a [`Dialer`], presenting the same
/// [`Credentials`] on each. Cheap to clone.
#[derive(Clone)]
pub struct Client {
    dialer: Arc<dyn Dialer>,
    credentials: Credentials,
}

impl Client {
    pub fn new(dialer: Arc<dyn Dialer>) -> Self {
        Self { dialer, credentials: Credentials::default() }
    }

    pub fn with_credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = credentials;
        self
    }

    pub fn credentials(&self) -> &Credentials {
        &self.credentials
    }

    /// A session on `protocol`. Call from a Tokio runtime.
    pub async fn session<Req: Serialize + Sync, Resp: DeserializeOwned>(
        &self,
        node: &str,
        protocol: &ProtocolId,
    ) -> Result<Session<Req, Resp>, ServiceError> {
        let ch = self.dialer.open(node, protocol.as_str()).await?;
        Session::start(ch, self.credentials.clone()).await
    }

    /// A session on the first of `protocols` (newest first) the server serves; the others
    /// answer [`ServiceError::Unsupported`]. Returns the one used.
    pub async fn session_any<Req: Serialize + Sync, Resp: DeserializeOwned>(
        &self,
        node: &str,
        protocols: &[ProtocolId],
    ) -> Result<(ProtocolId, Session<Req, Resp>), ServiceError> {
        let mut last = ServiceError::unsupported("no protocol given");
        for p in protocols {
            match self.session(node, p).await {
                Ok(s) => return Ok((p.clone(), s)),
                Err(e @ ServiceError::Unsupported { .. }) => last = e,
                Err(e) => return Err(e),
            }
        }
        Err(last)
    }

    /// Subscribes to `topic`, resuming from `resume` when given.
    pub async fn subscribe<T: Topic, S: State>(
        &self,
        node: &str,
        protocol: &ProtocolId,
        topic: T,
        resume: Option<crate::Position>,
    ) -> Result<Subscription<T, S>, ServiceError> {
        let ch = self.dialer.open(node, protocol.as_str()).await?;
        Subscription::start(ch, self.credentials.clone(), topic, resume).await
    }

    /// A [`Live`] copy of `topic` that reconnects on its own.
    pub fn live<T: Topic, S: State>(&self, node: &str, protocol: &ProtocolId, topic: T) -> Live<T, S> {
        Live::new(self.clone(), node, protocol.clone(), topic)
    }
}
