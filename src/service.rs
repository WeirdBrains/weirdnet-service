//! Request/response services and sessions.
//!
//! A session is one stream. The client sends `Hello` with its [`Credentials`]; the server
//! checks [`Access::session`] and answers `Welcome` (or `Failed`, then closes). After that the
//! client sends `Call { id, body }` frames and the server answers each with `Ok { id, body }`
//! or `Err { id, error }`, in completion order. Up to [`SessionOptions::max_in_flight`] calls
//! run at once and up to [`SessionOptions::max_queued`] more wait for a slot; beyond that a
//! call is answered [`ServiceError::Busy`], so a fast client never queues without bound.
//! `Cancel { id }` stops a running or waiting call.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::codec::{from_cbor, to_cbor};
use crate::{Access, Admitted, Authorize, Caller, Channel, Credentials, ServiceError, WIRE};

/// A typed request/response service. The request and response types are usually enums shared
/// by the server and its clients (a `myapp-proto` crate).
///
/// ```
/// use weirdnet_service::{Access, Admitted, Service, ServiceError};
///
/// struct Echo;
///
/// impl Service for Echo {
///     type Request = String;
///     type Response = String;
///
///     fn access(&self, _request: &String) -> Access {
///         Access::new("echo")
///     }
///
///     async fn call(&self, _who: Admitted, request: String) -> Result<String, ServiceError> {
///         Ok(request)
///     }
/// }
/// ```
pub trait Service: Send + Sync + 'static {
    type Request: DeserializeOwned + Send + 'static;
    type Response: Serialize + Send + 'static;

    /// What the request needs; the session asks the [`Authorize`] hook before [`call`](Self::call).
    fn access(&self, request: &Self::Request) -> Access;

    /// Handles one authorized request. `who` is what the hook returned.
    fn call(&self, who: Admitted, request: Self::Request) -> impl Future<Output = Result<Self::Response, ServiceError>> + Send;
}

/// How a server runs sessions.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SessionOptions {
    /// Calls running at once per session (default 32).
    pub max_in_flight: usize,
    /// Calls waiting for a free slot (default 256); more are answered [`ServiceError::Busy`].
    pub max_queued: usize,
    /// How long a new stream may take to say `Hello` (default 10 s).
    pub hello_timeout: Duration,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self { max_in_flight: 32, max_queued: 256, hello_timeout: Duration::from_secs(10) }
    }
}

impl SessionOptions {
    pub fn with_max_in_flight(mut self, n: usize) -> Self {
        self.max_in_flight = n.max(1);
        self
    }

    pub fn with_max_queued(mut self, n: usize) -> Self {
        self.max_queued = n;
        self
    }

    pub fn with_hello_timeout(mut self, d: Duration) -> Self {
        self.hello_timeout = d;
        self
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClientFrame {
    Hello {
        wire: u8,
        credentials: Credentials,
    },
    Call {
        id: u64,
        #[serde(with = "serde_bytes")]
        body: Vec<u8>,
    },
    Cancel {
        id: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServerFrame {
    Welcome {
        wire: u8,
    },
    Ok {
        id: u64,
        #[serde(with = "serde_bytes")]
        body: Vec<u8>,
    },
    Err {
        id: u64,
        error: ServiceError,
    },
    Failed(ServiceError),
}

impl ServerFrame {
    fn id(&self) -> Option<u64> {
        match self {
            ServerFrame::Ok { id, .. } | ServerFrame::Err { id, .. } => Some(*id),
            _ => None,
        }
    }
}

/// Reads the `Hello` (or a feed's first message) within the timeout.
pub(crate) async fn first<T: DeserializeOwned>(ch: &mut Channel, timeout: Duration) -> Result<Option<T>, ServiceError> {
    match tokio::time::timeout(timeout, ch.recv_cbor::<T>()).await {
        Ok(r) => r,
        Err(_) => Err(ServiceError::Timeout),
    }
}

/// Serves one session on `ch`. `peer` must be the transport-authenticated peer id. Returns
/// when the client closes its side and every call has answered, or on a protocol error.
pub async fn serve_session<S: Service>(
    service: Arc<S>,
    auth: Arc<dyn Authorize>,
    mut ch: Channel,
    peer: String,
    options: &SessionOptions,
) -> Result<(), ServiceError> {
    let credentials = match first::<ClientFrame>(&mut ch, options.hello_timeout).await {
        Ok(Some(ClientFrame::Hello { wire, credentials })) if wire == WIRE => credentials,
        Ok(Some(ClientFrame::Hello { wire, .. })) => {
            return ch.send_cbor(&ServerFrame::Failed(ServiceError::unsupported(format!("wire version {wire}")))).await;
        }
        Ok(Some(_)) => return ch.send_cbor(&ServerFrame::Failed(ServiceError::protocol("expected hello"))).await,
        Ok(None) => return Ok(()),
        Err(e) => {
            let _ = ch.send_cbor(&ServerFrame::Failed(e.clone())).await;
            return Err(e);
        }
    };
    let caller = Caller::new(peer, credentials);
    if let Err(e) = auth.authorize(&caller, &Access::session()) {
        return ch.send_cbor(&ServerFrame::Failed(e)).await;
    }
    ch.send_cbor(&ServerFrame::Welcome { wire: WIRE }).await?;
    let caller = Arc::new(caller);
    let max = options.max_in_flight.max(1);
    let Channel { mut tx, mut rx } = ch;
    let (done_tx, mut done_rx) = mpsc::channel::<ServerFrame>(max);
    let mut running: HashMap<u64, tokio::task::AbortHandle> = HashMap::new();
    let mut queued: VecDeque<(u64, Admitted, S::Request)> = VecDeque::new();
    let spawn = |id: u64, who: Admitted, request: S::Request| {
        let (svc, done) = (service.clone(), done_tx.clone());
        tokio::spawn(async move {
            let frame = match svc.call(who, request).await {
                Ok(resp) => match to_cbor(&resp) {
                    Ok(body) => ServerFrame::Ok { id, body },
                    Err(error) => ServerFrame::Err { id, error },
                },
                Err(error) => ServerFrame::Err { id, error },
            };
            let _ = done.send(frame).await;
        })
        .abort_handle()
    };
    let mut reading = true;
    let result = loop {
        if !reading && running.is_empty() && queued.is_empty() {
            break Ok(());
        }
        tokio::select! {
            frame = rx.recv(), if reading => {
                let bytes = match frame {
                    Ok(Some(b)) => b,
                    Ok(None) => { reading = false; continue }
                    Err(e) => break Err(e),
                };
                let reply = match from_cbor::<ClientFrame>(&bytes) {
                    Ok(ClientFrame::Call { id, body }) => {
                        if running.contains_key(&id) || queued.iter().any(|(q, _, _)| *q == id) {
                            Some(ServerFrame::Err { id, error: ServiceError::bad_request("a call with this id is running") })
                        } else {
                            match from_cbor::<S::Request>(&body) {
                                Err(e) => Some(ServerFrame::Err { id, error: ServiceError::bad_request(e.to_string()) }),
                                Ok(request) => {
                                    let access = service.access(&request);
                                    match auth.authorize(&caller, &access) {
                                        Err(error) => Some(ServerFrame::Err { id, error }),
                                        Ok(who) if running.len() < max => {
                                            running.insert(id, spawn(id, who, request));
                                            None
                                        }
                                        Ok(who) if queued.len() < options.max_queued => {
                                            queued.push_back((id, who, request));
                                            None
                                        }
                                        Ok(_) => Some(ServerFrame::Err { id, error: ServiceError::Busy }),
                                    }
                                }
                            }
                        }
                    }
                    Ok(ClientFrame::Cancel { id }) => {
                        let was_queued = queued.iter().position(|(q, _, _)| *q == id).map(|i| queued.remove(i)).is_some();
                        let was_running = running.remove(&id).map(|h| h.abort()).is_some();
                        (was_queued || was_running).then_some(ServerFrame::Err { id, error: ServiceError::Cancelled })
                    }
                    Ok(ClientFrame::Hello { .. }) => {
                        let _ = tx.send(to_cbor(&ServerFrame::Failed(ServiceError::protocol("hello twice")))?).await;
                        break Err(ServiceError::protocol("hello twice"));
                    }
                    Err(e) => {
                        let _ = tx.send(to_cbor(&ServerFrame::Failed(e.clone()))?).await;
                        break Err(e);
                    }
                };
                if let Some(reply) = reply && let Err(e) = tx.send(to_cbor(&reply)?).await {
                    break Err(e);
                }
            }
            Some(frame) = done_rx.recv() => {
                // A cancelled call that finished anyway has already been answered.
                if frame.id().is_some_and(|id| running.remove(&id).is_some()) && let Err(e) = tx.send(to_cbor(&frame)?).await {
                    break Err(e);
                }
            }
        }
        while running.len() < max
            && let Some((id, who, request)) = queued.pop_front()
        {
            running.insert(id, spawn(id, who, request));
        }
    };
    for h in running.values() {
        h.abort();
    }
    result
}

enum Out {
    Call { id: u64, body: Vec<u8> },
    Cancel { id: u64 },
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Vec<u8>, ServiceError>>>>>;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A client's open session: concurrent typed calls over one stream. Cheap to share by
/// reference; dropping it closes the stream once calls under way have answered or been
/// dropped.
pub struct Session<Req, Resp> {
    out: mpsc::Sender<Out>,
    pending: Pending,
    next: AtomicU64,
    closed: tokio::sync::watch::Receiver<bool>,
    task: tokio::task::JoinHandle<()>,
    _types: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> Drop for Session<Req, Resp> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct CancelGuard<'a> {
    id: u64,
    out: &'a mpsc::Sender<Out>,
    pending: &'a Pending,
    armed: bool,
}

impl Drop for CancelGuard<'_> {
    fn drop(&mut self) {
        if self.armed && lock(self.pending).remove(&self.id).is_some() {
            let _ = self.out.try_send(Out::Cancel { id: self.id });
        }
    }
}

impl<Req: Serialize + Sync, Resp: DeserializeOwned> Session<Req, Resp> {
    /// Says `Hello` on `ch` and waits for `Welcome`. Call from a Tokio runtime.
    pub async fn start(mut ch: Channel, credentials: Credentials) -> Result<Self, ServiceError> {
        ch.send_cbor(&ClientFrame::Hello { wire: WIRE, credentials }).await?;
        match ch.recv_cbor::<ServerFrame>().await? {
            Some(ServerFrame::Welcome { .. }) => {}
            Some(ServerFrame::Failed(e)) => return Err(e),
            Some(other) => return Err(ServiceError::protocol(format!("expected welcome, got {other:?}"))),
            None => return Err(ServiceError::Closed),
        }
        let (out, mut out_rx) = mpsc::channel::<Out>(64);
        let pending: Pending = Arc::default();
        let (closed_tx, closed) = tokio::sync::watch::channel(false);
        let p = pending.clone();
        let Channel { mut tx, mut rx } = ch;
        let task = tokio::spawn(async move {
            let mut fail = ServiceError::Closed;
            loop {
                tokio::select! {
                    o = out_rx.recv() => {
                        let frame = match o {
                            Some(Out::Call { id, body }) => ClientFrame::Call { id, body },
                            Some(Out::Cancel { id }) => ClientFrame::Cancel { id },
                            None => break,
                        };
                        let sent = match to_cbor(&frame) {
                            Ok(bytes) => tx.send(bytes).await,
                            Err(e) => Err(e),
                        };
                        if sent.is_err() {
                            break;
                        }
                    }
                    m = rx.recv() => {
                        let frame = match m {
                            Ok(Some(bytes)) => match from_cbor::<ServerFrame>(&bytes) {
                                Ok(f) => f,
                                Err(e) => { fail = e; break }
                            },
                            Ok(None) => break,
                            Err(e) => { fail = e; break }
                        };
                        match frame {
                            ServerFrame::Ok { id, body } => {
                                if let Some(t) = lock(&p).remove(&id) {
                                    let _ = t.send(Ok(body));
                                }
                            }
                            ServerFrame::Err { id, error } => {
                                if let Some(t) = lock(&p).remove(&id) {
                                    let _ = t.send(Err(error));
                                }
                            }
                            ServerFrame::Failed(e) => { fail = e; break }
                            ServerFrame::Welcome { .. } => { fail = ServiceError::protocol("welcome twice"); break }
                        }
                    }
                }
            }
            // Close the queue first, so a call racing the end fails to send instead of waiting.
            out_rx.close();
            let _ = closed_tx.send(true);
            for (_, t) in lock(&p).drain() {
                let _ = t.send(Err(fail.clone()));
            }
        });
        Ok(Self { out, pending, next: AtomicU64::new(1), closed, task, _types: PhantomData })
    }

    /// One call. Dropping the future cancels the call on the server.
    pub async fn call(&self, request: &Req) -> Result<Resp, ServiceError> {
        if self.is_closed() {
            return Err(ServiceError::Closed);
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let body = to_cbor(request)?;
        let (t, r) = oneshot::channel();
        lock(&self.pending).insert(id, t);
        let mut guard = CancelGuard { id, out: &self.out, pending: &self.pending, armed: true };
        if self.out.send(Out::Call { id, body }).await.is_err() {
            lock(&self.pending).remove(&id);
            guard.armed = false;
            return Err(ServiceError::Closed);
        }
        let answer = r.await;
        guard.armed = false;
        let bytes = answer.map_err(|_| ServiceError::Closed)??;
        from_cbor(&bytes)
    }

    /// [`call`](Self::call) with a deadline; on timeout the call is cancelled on the server.
    pub async fn call_timeout(&self, request: &Req, timeout: Duration) -> Result<Resp, ServiceError> {
        tokio::time::timeout(timeout, self.call(request)).await.unwrap_or(Err(ServiceError::Timeout))
    }

    /// The stream has ended; every further call fails with `Closed`.
    pub fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    /// Calls sent and not yet answered.
    pub fn in_flight(&self) -> usize {
        lock(&self.pending).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AllowAll, memory_pair};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum Req {
        Add(i64, i64),
        Sleep(u64),
        Secret,
    }

    struct Calc;

    impl Service for Calc {
        type Request = Req;
        type Response = i64;

        fn access(&self, r: &Req) -> Access {
            match r {
                Req::Secret => Access::new("secret"),
                _ => Access::new("calc"),
            }
        }

        async fn call(&self, _who: Admitted, r: Req) -> Result<i64, ServiceError> {
            match r {
                Req::Add(a, b) => Ok(a + b),
                Req::Sleep(ms) => {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    Ok(ms as i64)
                }
                Req::Secret => Ok(42),
            }
        }
    }

    fn no_secrets() -> Arc<dyn Authorize> {
        Arc::new(|c: &Caller, a: &Access| if a.permission == "secret" { Err(ServiceError::refused("role")) } else { AllowAll.authorize(c, a) })
    }

    async fn pair(auth: Arc<dyn Authorize>, opts: SessionOptions) -> Session<Req, i64> {
        let (client, server) = memory_pair(8);
        tokio::spawn(async move { serve_session(Arc::new(Calc), auth, server, "peer-a".into(), &opts).await });
        Session::start(client, Credentials::new()).await.unwrap()
    }

    #[tokio::test]
    async fn calls_run_concurrently_and_answer_in_completion_order() {
        let s = pair(no_secrets(), SessionOptions::default()).await;
        let (slow, fast) = tokio::join!(s.call(&Req::Sleep(200)), async {
            let v = s.call(&Req::Add(2, 3)).await;
            (v, std::time::Instant::now())
        });
        assert_eq!((slow.unwrap(), fast.0.unwrap()), (200, 5));
        assert_eq!(s.call(&Req::Secret).await, Err(ServiceError::refused("role")));
        assert_eq!(s.call(&Req::Add(1, 1)).await.unwrap(), 2, "a refusal does not end the session");
        assert_eq!(s.in_flight(), 0);
    }

    #[tokio::test]
    async fn timeouts_cancel_and_the_session_carries_on() {
        let s = pair(no_secrets(), SessionOptions::default().with_max_in_flight(1)).await;
        assert_eq!(s.call_timeout(&Req::Sleep(5_000), Duration::from_millis(50)).await, Err(ServiceError::Timeout));
        // With one slot, this only runs once the cancelled call is gone.
        let started = std::time::Instant::now();
        assert_eq!(s.call_timeout(&Req::Add(4, 4), Duration::from_secs(2)).await.unwrap(), 8);
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn a_full_queue_answers_busy() {
        let s = pair(no_secrets(), SessionOptions::default().with_max_in_flight(1).with_max_queued(1)).await;
        let (a, b, c) = tokio::join!(s.call(&Req::Sleep(150)), s.call(&Req::Sleep(150)), s.call(&Req::Sleep(150)));
        let mut got = [a, b, c].map(|r| r.map_err(|e| e == ServiceError::Busy));
        got.sort();
        assert_eq!(got, [Ok(150), Ok(150), Err(true)]);
    }

    #[tokio::test]
    async fn refused_sessions_and_bad_bodies() {
        let (client, server) = memory_pair(8);
        let deny: Arc<dyn Authorize> = Arc::new(|_: &Caller, _: &Access| Err(ServiceError::refused("unknown-device")));
        tokio::spawn(async move { serve_session(Arc::new(Calc), deny, server, "p".into(), &SessionOptions::default()).await });
        assert_eq!(Session::<Req, i64>::start(client, Credentials::new()).await.err(), Some(ServiceError::refused("unknown-device")));

        // A body of the wrong type is a bad request for that call only.
        let s: Session<String, i64> = {
            let (client, server) = memory_pair(8);
            tokio::spawn(async move { serve_session(Arc::new(Calc), Arc::new(AllowAll), server, "p".into(), &SessionOptions::default()).await });
            Session::start(client, Credentials::new()).await.unwrap()
        };
        assert!(matches!(s.call(&"nonsense".to_string()).await, Err(ServiceError::BadRequest { .. })));
        assert!(!s.is_closed());
    }

    #[tokio::test]
    async fn a_silent_client_times_out_and_a_closed_server_fails_calls() {
        let (_client, server) = memory_pair(8);
        let opts = SessionOptions::default().with_hello_timeout(Duration::from_millis(50));
        assert_eq!(serve_session(Arc::new(Calc), Arc::new(AllowAll), server, "p".into(), &opts).await, Err(ServiceError::Timeout));

        let (client, mut server) = memory_pair(8);
        let srv = tokio::spawn(async move {
            let _hello = server.recv_cbor::<ClientFrame>().await;
            server.send_cbor(&ServerFrame::Welcome { wire: WIRE }).await.unwrap();
            let _call = server.recv_cbor::<ClientFrame>().await;
            drop(server);
        });
        let s = Session::<Req, i64>::start(client, Credentials::new()).await.unwrap();
        assert_eq!(s.call(&Req::Add(1, 2)).await, Err(ServiceError::Closed));
        srv.await.unwrap();
        assert!(s.is_closed());
    }
}
