//! Live topic feeds: snapshot, then deltas, with resume and backpressure.
//!
//! A [`Hub`] holds the current [`State`] of each topic and numbers every delta published to
//! it. A subscriber first gets a [`Event::Snapshot`] at a [`Position`] (the hub's epoch and the
//! topic's sequence number), then [`Event::Deltas`] in order. Applying the deltas to the
//! snapshot always gives the topic's state at the last sequence number received.
//!
//! - **Resume.** A subscriber that reconnects passes its last position. If the hub still holds
//!   every delta after it (the last [`HubOptions::retain`] per topic) and the epoch matches
//!   (the hub was not restarted), it gets [`Event::Resumed`] and the missed deltas; otherwise a
//!   fresh snapshot.
//! - **Backpressure.** Each subscriber has its own queue of at most [`HubOptions::queue`]
//!   deltas. Publishing never waits for subscribers. Deltas with the same
//!   [`State::coalesce_key`] replace each other in a queue (the latest price of a product, the
//!   latest state of an order). A queue that still overflows is cleared and the subscriber
//!   gets a fresh snapshot ([`Overflow::Resnapshot`], the default) or is ended with reason
//!   `lagged` so it resumes ([`Overflow::Disconnect`]).
//! - **Throttling.** [`FeedOptions::min_interval`] spaces out what a network subscriber is sent;
//!   deltas published in between are coalesced in its queue.
//!
//! [`serve_feed`] serves one subscription on a [`Channel`]; [`Subscription`] is the client
//! side and [`Live`] keeps a local copy of the state that reconnects and resumes on its own.

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::service::first;
use crate::{Access, Authorize, Caller, Channel, Client, Credentials, ProtocolId, ServiceError, WIRE};

/// A topic's state. Deltas change it; the hub and [`Live`] apply them the same way.
pub trait State: Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static {
    type Delta: Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Applies one delta.
    fn apply(&mut self, delta: &Self::Delta);

    /// Deltas with the same key supersede each other: applying only the latest of them gives
    /// the same state as applying them all. `None` (the default) never coalesces.
    fn coalesce_key(_delta: &Self::Delta) -> Option<String> {
        None
    }
}

/// A topic key, such as `Ticker { account, product }`.
pub trait Topic: Clone + Eq + Hash + Debug + Serialize + DeserializeOwned + Send + Sync + 'static {}

impl<T: Clone + Eq + Hash + Debug + Serialize + DeserializeOwned + Send + Sync + 'static> Topic for T {}

/// Where a subscriber is: the hub's epoch (new on every hub, so a restarted server is
/// noticed) and the topic's sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Position {
    pub epoch: u64,
    pub seq: u64,
}

/// What happens to a subscriber whose queue overflows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Overflow {
    /// Clear the queue and send a fresh snapshot.
    #[default]
    Resnapshot,
    /// Clear the queue and end the subscription with reason `lagged`; the client resumes.
    Disconnect,
}

/// How a hub keeps history and queues.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HubOptions {
    /// Deltas kept per topic for resuming (default 1024).
    pub retain: usize,
    /// Deltas queued per subscriber before [`Overflow`] applies (default 1024).
    pub queue: usize,
    pub overflow: Overflow,
}

impl Default for HubOptions {
    fn default() -> Self {
        Self { retain: 1024, queue: 1024, overflow: Overflow::Resnapshot }
    }
}

impl HubOptions {
    pub fn with_retain(mut self, n: usize) -> Self {
        self.retain = n;
        self
    }

    pub fn with_queue(mut self, n: usize) -> Self {
        self.queue = n.max(1);
        self
    }

    pub fn with_overflow(mut self, o: Overflow) -> Self {
        self.overflow = o;
        self
    }
}

/// What a subscriber receives.
#[derive(Debug, Clone)]
pub enum Event<S: State> {
    /// The whole state at `at`. Replace any local copy.
    Snapshot { at: Position, state: S },
    /// Resumed at `at`: the local copy is current as of `at`, missed deltas follow.
    Resumed { at: Position },
    /// Deltas in order, each with its sequence number. Sequence numbers increase but may skip
    /// (coalesced deltas).
    Deltas { epoch: u64, items: Vec<(u64, S::Delta)> },
    /// The topic is gone (`removed`) or the subscriber fell behind (`lagged`).
    Ended { reason: String },
}

struct Queue<S: State> {
    items: VecDeque<(u64, Option<String>, S::Delta)>,
    resnapshot: bool,
    ended: Option<String>,
}

struct Sub<S: State> {
    q: Mutex<Queue<S>>,
    notify: Notify,
}

impl<S: State> Sub<S> {
    fn new() -> Self {
        Self { q: Mutex::new(Queue { items: VecDeque::new(), resnapshot: false, ended: None }), notify: Notify::new() }
    }

    fn push(&self, seq: u64, delta: &S::Delta, options: &HubOptions) {
        let mut q = lock(&self.q);
        if q.resnapshot || q.ended.is_some() {
            return;
        }
        let key = S::coalesce_key(delta);
        if let Some(k) = &key
            && let Some(i) = q.items.iter().position(|(_, kk, _)| kk.as_ref() == Some(k))
        {
            q.items.remove(i);
        }
        q.items.push_back((seq, key, delta.clone()));
        if q.items.len() > options.queue {
            q.items.clear();
            match options.overflow {
                Overflow::Resnapshot => q.resnapshot = true,
                Overflow::Disconnect => q.ended = Some("lagged".into()),
            }
        }
        drop(q);
        self.notify.notify_one();
    }

    fn resnapshot(&self) {
        let mut q = lock(&self.q);
        if q.ended.is_none() {
            q.items.clear();
            q.resnapshot = true;
        }
        drop(q);
        self.notify.notify_one();
    }

    fn end(&self, reason: &str) {
        let mut q = lock(&self.q);
        q.items.clear();
        q.ended.get_or_insert_with(|| reason.to_string());
        drop(q);
        self.notify.notify_one();
    }
}

struct Entry<S: State> {
    state: S,
    seq: u64,
    ring: VecDeque<(u64, S::Delta)>,
    subs: Vec<Weak<Sub<S>>>,
}

impl<S: State> Entry<S> {
    fn live_subs(&mut self) -> Vec<Arc<Sub<S>>> {
        let live: Vec<Arc<Sub<S>>> = self.subs.iter().filter_map(Weak::upgrade).collect();
        self.subs = live.iter().map(Arc::downgrade).collect();
        live
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn new_epoch() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or_default());
    h.finish().max(1)
}

/// Topics, their current state and their subscribers. Share it in an `Arc` between the code
/// that publishes and the [`Server`](crate::net::Server) that serves it.
pub struct Hub<T: Topic, S: State> {
    epoch: u64,
    options: HubOptions,
    topics: Mutex<HashMap<T, Entry<S>>>,
}

impl<T: Topic, S: State> Hub<T, S> {
    /// A hub with a new random epoch.
    pub fn new(options: HubOptions) -> Self {
        Self::with_epoch(new_epoch(), options)
    }

    /// A hub with a chosen epoch: only if sequence numbers are restored with it (for example
    /// from an event log), so positions from before a restart stay valid.
    pub fn with_epoch(epoch: u64, options: HubOptions) -> Self {
        Self { epoch, options, topics: Mutex::new(HashMap::new()) }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Creates `topic` with `state`, or replaces its state: the sequence number moves on, the
    /// history is dropped and every subscriber gets a fresh snapshot.
    pub fn insert(&self, topic: T, state: S) -> Position {
        let mut topics = lock(&self.topics);
        match topics.get_mut(&topic) {
            Some(e) => {
                e.state = state;
                e.seq += 1;
                e.ring.clear();
                for s in e.live_subs() {
                    s.resnapshot();
                }
                Position { epoch: self.epoch, seq: e.seq }
            }
            None => {
                topics.insert(topic, Entry { state, seq: 1, ring: VecDeque::new(), subs: Vec::new() });
                Position { epoch: self.epoch, seq: 1 }
            }
        }
    }

    /// Applies `delta` to the topic and sends it to its subscribers. Never waits for them.
    pub fn publish(&self, topic: &T, delta: S::Delta) -> Result<Position, ServiceError> {
        self.publish_all(topic, [delta])
    }

    /// Publishes several deltas at once (one lock, consecutive sequence numbers).
    pub fn publish_all(&self, topic: &T, deltas: impl IntoIterator<Item = S::Delta>) -> Result<Position, ServiceError> {
        let mut topics = lock(&self.topics);
        let e = topics.get_mut(topic).ok_or_else(|| ServiceError::not_found(format!("topic {topic:?}")))?;
        let subs = e.live_subs();
        for delta in deltas {
            e.state.apply(&delta);
            e.seq += 1;
            for s in &subs {
                s.push(e.seq, &delta, &self.options);
            }
            if self.options.retain > 0 {
                if e.ring.len() == self.options.retain {
                    e.ring.pop_front();
                }
                e.ring.push_back((e.seq, delta));
            }
        }
        Ok(Position { epoch: self.epoch, seq: e.seq })
    }

    /// Removes the topic; its subscribers get [`Event::Ended`] with reason `removed`.
    pub fn remove(&self, topic: &T) -> bool {
        let Some(mut e) = lock(&self.topics).remove(topic) else { return false };
        for s in e.live_subs() {
            s.end("removed");
        }
        true
    }

    /// The topic's state and position.
    pub fn snapshot(&self, topic: &T) -> Option<(Position, S)> {
        lock(&self.topics).get(topic).map(|e| (Position { epoch: self.epoch, seq: e.seq }, e.state.clone()))
    }

    pub fn position(&self, topic: &T) -> Option<Position> {
        lock(&self.topics).get(topic).map(|e| Position { epoch: self.epoch, seq: e.seq })
    }

    pub fn contains(&self, topic: &T) -> bool {
        lock(&self.topics).contains_key(topic)
    }

    pub fn topics(&self) -> Vec<T> {
        lock(&self.topics).keys().cloned().collect()
    }

    /// Subscribers currently attached to the topic.
    pub fn subscribers(&self, topic: &T) -> usize {
        lock(&self.topics).get_mut(topic).map(|e| e.live_subs().len()).unwrap_or(0)
    }

    /// Subscribes in process (the network side uses this too). With `resume`, continues from
    /// that position when the history allows, else starts with a snapshot.
    pub fn subscribe(self: &Arc<Self>, topic: &T, resume: Option<Position>) -> Result<Watcher<T, S>, ServiceError> {
        let mut topics = lock(&self.topics);
        let e = topics.get_mut(topic).ok_or_else(|| ServiceError::not_found(format!("topic {topic:?}")))?;
        let sub = Arc::new(Sub::<S>::new());
        let at = Position { epoch: self.epoch, seq: e.seq };
        let resumable = resume
            .filter(|p| p.epoch == self.epoch && p.seq <= e.seq && (p.seq == e.seq || e.ring.front().is_some_and(|(first, _)| *first <= p.seq + 1)));
        let first = match resumable {
            Some(p) => {
                for (seq, d) in e.ring.iter().filter(|(s, _)| *s > p.seq) {
                    sub.push(*seq, d, &self.options);
                }
                Event::Resumed { at: p }
            }
            None => Event::Snapshot { at, state: e.state.clone() },
        };
        e.live_subs();
        e.subs.push(Arc::downgrade(&sub));
        drop(topics);
        Ok(Watcher { hub: self.clone(), topic: topic.clone(), sub, first: Some(first), done: false })
    }
}

/// One subscriber's view of a topic on the hub.
pub struct Watcher<T: Topic, S: State> {
    hub: Arc<Hub<T, S>>,
    topic: T,
    sub: Arc<Sub<S>>,
    first: Option<Event<S>>,
    done: bool,
}

impl<T: Topic, S: State> Watcher<T, S> {
    /// The next event; `None` after [`Event::Ended`]. Cancel-safe.
    pub async fn next(&mut self) -> Option<Event<S>> {
        if let Some(f) = self.first.take() {
            return Some(f);
        }
        if self.done {
            return None;
        }
        loop {
            let sub = self.sub.clone();
            let notified = sub.notify.notified();
            {
                let mut q = lock(&sub.q);
                if let Some(reason) = q.ended.take() {
                    self.done = true;
                    return Some(Event::Ended { reason });
                }
                if q.resnapshot {
                    drop(q);
                    return Some(self.resnapshot());
                }
                if !q.items.is_empty() {
                    let items = q.items.drain(..).map(|(s, _, d)| (s, d)).collect();
                    return Some(Event::Deltas { epoch: self.hub.epoch, items });
                }
            }
            notified.await;
        }
    }

    fn resnapshot(&mut self) -> Event<S> {
        let topics = lock(&self.hub.topics);
        let Some(e) = topics.get(&self.topic) else {
            self.done = true;
            return Event::Ended { reason: "removed".into() };
        };
        let mut q = lock(&self.sub.q);
        q.resnapshot = false;
        let seq = e.seq;
        q.items.retain(|(s, _, _)| *s > seq);
        Event::Snapshot { at: Position { epoch: self.hub.epoch, seq }, state: e.state.clone() }
    }

    pub fn topic(&self) -> &T {
        &self.topic
    }
}

/// How [`serve_feed`] runs a subscription.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FeedOptions {
    /// A heartbeat when nothing else was sent for this long (default 15 s), so clients notice
    /// a dead stream.
    pub heartbeat: Duration,
    /// At most one message per this interval (default none). Deltas in between coalesce.
    pub min_interval: Option<Duration>,
    /// Ask the [`Authorize`] hook again this often (default 30 s), so a revoked device or
    /// membership loses its live stream. `None` checks only at the start.
    pub recheck: Option<Duration>,
    /// How long a new stream may take to send `Subscribe` (default 10 s).
    pub hello_timeout: Duration,
}

impl Default for FeedOptions {
    fn default() -> Self {
        Self {
            heartbeat: Duration::from_secs(15),
            min_interval: None,
            recheck: Some(Duration::from_secs(30)),
            hello_timeout: Duration::from_secs(10),
        }
    }
}

impl FeedOptions {
    pub fn with_heartbeat(mut self, d: Duration) -> Self {
        self.heartbeat = d;
        self
    }

    pub fn with_min_interval(mut self, d: Option<Duration>) -> Self {
        self.min_interval = d;
        self
    }

    pub fn with_recheck(mut self, d: Option<Duration>) -> Self {
        self.recheck = d;
        self
    }

    pub fn with_hello_timeout(mut self, d: Duration) -> Self {
        self.hello_timeout = d;
        self
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct SubscribeFrame<T> {
    wire: u8,
    credentials: Credentials,
    topic: T,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resume: Option<Position>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FeedFrame<S, D> {
    Snapshot { at: Position, state: S },
    Resumed { at: Position },
    Deltas { epoch: u64, items: Vec<(u64, D)> },
    Heartbeat { at: Position },
    Ended { reason: String },
    Failed(ServiceError),
}

/// What a feed needs for a topic (usually a read permission on the topic's account).
pub type AccessFn<T> = Arc<dyn Fn(&T) -> Access + Send + Sync>;

/// Serves one subscription on `ch`. `peer` must be the transport-authenticated peer id.
pub async fn serve_feed<T: Topic, S: State>(
    hub: Arc<Hub<T, S>>,
    access: AccessFn<T>,
    auth: Arc<dyn Authorize>,
    mut ch: Channel,
    peer: String,
    options: &FeedOptions,
) -> Result<(), ServiceError> {
    type Frame<S> = FeedFrame<S, <S as State>::Delta>;
    let hello = match first::<SubscribeFrame<T>>(&mut ch, options.hello_timeout).await {
        Ok(Some(h)) => h,
        Ok(None) => return Ok(()),
        Err(e) => {
            let _ = ch.send_cbor(&Frame::<S>::Failed(e.clone())).await;
            return Err(e);
        }
    };
    if hello.wire != WIRE {
        return ch.send_cbor(&Frame::<S>::Failed(ServiceError::unsupported(format!("wire version {}", hello.wire)))).await;
    }
    let caller = Caller::new(peer, hello.credentials);
    let need = access(&hello.topic);
    if let Err(e) = auth.authorize(&caller, &need) {
        return ch.send_cbor(&Frame::<S>::Failed(e)).await;
    }
    let mut watcher = match hub.subscribe(&hello.topic, hello.resume) {
        Ok(w) => w,
        Err(e) => return ch.send_cbor(&Frame::<S>::Failed(e)).await,
    };
    let mut at = Position { epoch: hub.epoch(), seq: 0 };
    let mut beat = tokio::time::interval(options.heartbeat.max(Duration::from_millis(10)));
    beat.reset();
    let mut recheck = tokio::time::interval(options.recheck.unwrap_or(Duration::from_secs(3600)).max(Duration::from_millis(10)));
    recheck.reset();
    loop {
        tokio::select! {
            ev = watcher.next() => {
                let Some(ev) = ev else { return Ok(()) };
                let (frame, end) = match ev {
                    Event::Snapshot { at: p, state } => { at = p; (Frame::<S>::Snapshot { at: p, state }, false) }
                    Event::Resumed { at: p } => { at = p; (Frame::<S>::Resumed { at: p }, false) }
                    Event::Deltas { epoch, items } => {
                        if let Some((s, _)) = items.last() { at.seq = *s; }
                        (Frame::<S>::Deltas { epoch, items }, false)
                    }
                    Event::Ended { reason } => (Frame::<S>::Ended { reason }, true),
                };
                ch.send_cbor(&frame).await?;
                if end {
                    return Ok(());
                }
                beat.reset();
                if let Some(d) = options.min_interval {
                    tokio::time::sleep(d).await;
                }
            }
            _ = beat.tick() => ch.send_cbor(&Frame::<S>::Heartbeat { at }).await?,
            _ = recheck.tick(), if options.recheck.is_some() => {
                if let Err(e) = auth.authorize(&caller, &need) {
                    return ch.send_cbor(&Frame::<S>::Failed(e)).await;
                }
            }
            r = ch.rx.recv() => match r {
                Ok(None) | Err(_) => return Ok(()),
                Ok(Some(_)) => return ch.send_cbor(&Frame::<S>::Failed(ServiceError::protocol("unexpected message on a subscription"))).await,
            },
        }
    }
}

/// A client's subscription to one topic. Checks that positions only move forward.
pub struct Subscription<T, S: State> {
    ch: Channel,
    at: Option<Position>,
    stall: Option<Duration>,
    pending: Option<FeedFrame<S, S::Delta>>,
    done: bool,
    _topic: PhantomData<fn() -> T>,
}

impl<T: Topic, S: State> Subscription<T, S> {
    /// Sends `Subscribe` on `ch` and waits for the first answer, so a refusal comes back here.
    pub async fn start(mut ch: Channel, credentials: Credentials, topic: T, resume: Option<Position>) -> Result<Self, ServiceError> {
        ch.send_cbor(&SubscribeFrame { wire: WIRE, credentials, topic, resume }).await?;
        let first = match ch.recv_cbor::<FeedFrame<S, S::Delta>>().await? {
            None => return Err(ServiceError::Closed),
            Some(FeedFrame::Failed(e)) => return Err(e),
            Some(FeedFrame::Resumed { at }) if Some(at) != resume => return Err(ServiceError::protocol("resumed at another position")),
            Some(f) => f,
        };
        Ok(Self { ch, at: None, stall: Some(Duration::from_secs(45)), pending: Some(first), done: false, _topic: PhantomData })
    }

    /// Fail with [`ServiceError::Timeout`] when nothing (not even a heartbeat) arrives for this
    /// long (default 45 s, three server heartbeats). `None` waits forever.
    pub fn with_stall(mut self, d: Option<Duration>) -> Self {
        self.stall = d;
        self
    }

    /// The position of the last event received.
    pub fn position(&self) -> Option<Position> {
        self.at
    }

    async fn recv(&mut self) -> Result<Option<FeedFrame<S, S::Delta>>, ServiceError> {
        if let Some(f) = self.pending.take() {
            return Ok(Some(f));
        }
        match self.stall {
            None => self.ch.recv_cbor().await,
            Some(d) => tokio::time::timeout(d, self.ch.recv_cbor()).await.unwrap_or(Err(ServiceError::Timeout)),
        }
    }

    /// The next event; `None` once the stream ended. Cancel-safe only between events (don't
    /// race it against something that may drop it mid-frame on a slow link).
    pub async fn next(&mut self) -> Option<Result<Event<S>, ServiceError>> {
        loop {
            if self.done {
                return None;
            }
            let frame = match self.recv().await {
                Ok(Some(f)) => f,
                Ok(None) => {
                    self.done = true;
                    return None;
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            };
            let out = match frame {
                FeedFrame::Snapshot { at, state } => {
                    self.at = Some(at);
                    Ok(Event::Snapshot { at, state })
                }
                FeedFrame::Resumed { at } => {
                    self.at = Some(at);
                    Ok(Event::Resumed { at })
                }
                FeedFrame::Deltas { epoch, items } => match self.at {
                    Some(mut p) if p.epoch == epoch => {
                        let mut ok = true;
                        for (s, _) in &items {
                            ok &= *s > p.seq;
                            p.seq = *s;
                        }
                        if ok {
                            self.at = Some(p);
                            Ok(Event::Deltas { epoch, items })
                        } else {
                            Err(ServiceError::protocol("feed: sequence went backwards"))
                        }
                    }
                    _ => Err(ServiceError::protocol("feed: deltas before a snapshot, or from another epoch")),
                },
                FeedFrame::Heartbeat { at } => {
                    if self.at.is_some_and(|p| p.epoch != at.epoch) {
                        Err(ServiceError::protocol("feed: heartbeat from another epoch"))
                    } else {
                        continue;
                    }
                }
                FeedFrame::Ended { reason } => {
                    self.done = true;
                    Ok(Event::Ended { reason })
                }
                FeedFrame::Failed(e) => Err(e),
            };
            if out.is_err() {
                self.done = true;
            }
            return Some(out);
        }
    }
}

/// What changed in a [`Live`] state.
#[derive(Debug, Clone)]
pub enum Change<S: State> {
    /// A new snapshot replaced the state.
    Snapshot,
    /// Reconnected without losing anything.
    Resumed,
    /// These deltas were applied.
    Deltas(Vec<S::Delta>),
    /// The stream dropped; the next call reconnects (after a backoff) and resumes.
    Disconnected(ServiceError),
    /// The topic ended for good (`removed`).
    Ended(String),
}

/// A local copy of a topic's state that follows the server, reconnecting and resuming on its
/// own. Drive it with [`next`](Self::next) from a background task and push each change into
/// the UI.
pub struct Live<T: Topic, S: State> {
    client: Client,
    node: String,
    protocol: ProtocolId,
    topic: T,
    state: Option<S>,
    at: Option<Position>,
    sub: Option<Subscription<T, S>>,
    backoff: Duration,
    wait: Option<Duration>,
    max_backoff: Duration,
    ended: bool,
}

impl<T: Topic, S: State> Live<T, S> {
    pub fn new(client: Client, node: impl Into<String>, protocol: ProtocolId, topic: T) -> Self {
        Self {
            client,
            node: node.into(),
            protocol,
            topic,
            state: None,
            at: None,
            sub: None,
            backoff: Duration::from_millis(250),
            wait: None,
            max_backoff: Duration::from_secs(30),
            ended: false,
        }
    }

    /// Reconnect backoff: starts at `first`, doubles up to `max`.
    pub fn with_backoff(mut self, first: Duration, max: Duration) -> Self {
        self.backoff = first;
        self.max_backoff = max.max(first);
        self
    }

    /// The state, once the first snapshot arrived.
    pub fn state(&self) -> Option<&S> {
        self.state.as_ref()
    }

    pub fn position(&self) -> Option<Position> {
        self.at
    }

    pub fn is_connected(&self) -> bool {
        self.sub.is_some()
    }

    fn dropped(&mut self, e: ServiceError) -> Result<Change<S>, ServiceError> {
        self.sub = None;
        let next = self.wait.map(|w| (w * 2).min(self.max_backoff)).unwrap_or(self.backoff);
        self.wait = Some(next);
        Ok(Change::Disconnected(e))
    }

    /// The next change. Errors are permanent (refused, not found, unsupported); transient
    /// failures come back as [`Change::Disconnected`] and the next call retries.
    pub async fn next(&mut self) -> Result<Change<S>, ServiceError> {
        if self.ended {
            return Err(ServiceError::not_found("the topic ended"));
        }
        if self.sub.is_none() {
            if let Some(w) = self.wait {
                tokio::time::sleep(w).await;
            }
            match self.client.subscribe::<T, S>(&self.node, &self.protocol, self.topic.clone(), self.at).await {
                Ok(s) => self.sub = Some(s),
                Err(e) if e.is_permanent() => return Err(e),
                Err(e) => return self.dropped(e),
            }
        }
        let Some(sub) = self.sub.as_mut() else { return self.dropped(ServiceError::Closed) };
        match sub.next().await {
            Some(Ok(Event::Snapshot { at, state })) => {
                self.wait = None;
                self.state = Some(state);
                self.at = Some(at);
                Ok(Change::Snapshot)
            }
            Some(Ok(Event::Resumed { at })) => {
                self.wait = None;
                self.at = Some(at);
                Ok(Change::Resumed)
            }
            Some(Ok(Event::Deltas { items, .. })) => {
                let mut applied = Vec::with_capacity(items.len());
                if let Some(state) = self.state.as_mut() {
                    for (seq, d) in items {
                        state.apply(&d);
                        if let Some(p) = self.at.as_mut() {
                            p.seq = seq;
                        }
                        applied.push(d);
                    }
                }
                Ok(Change::Deltas(applied))
            }
            Some(Ok(Event::Ended { reason })) if reason == "lagged" => self.dropped(ServiceError::unavailable("lagged")),
            Some(Ok(Event::Ended { reason })) => {
                self.sub = None;
                self.ended = true;
                Ok(Change::Ended(reason))
            }
            Some(Err(e)) if e.is_permanent() => {
                self.sub = None;
                Err(e)
            }
            Some(Err(e)) => self.dropped(e),
            None => self.dropped(ServiceError::Closed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AllowAll, memory_pair};

    #[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
    struct Prices(std::collections::BTreeMap<String, i64>);

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum P {
        Set(String, i64),
        Clear,
    }

    impl State for Prices {
        type Delta = P;
        fn apply(&mut self, d: &P) {
            match d {
                P::Set(k, v) => {
                    self.0.insert(k.clone(), *v);
                }
                P::Clear => self.0.clear(),
            }
        }
        fn coalesce_key(d: &P) -> Option<String> {
            match d {
                P::Set(k, _) => Some(k.clone()),
                P::Clear => None,
            }
        }
    }

    fn set(k: &str, v: i64) -> P {
        P::Set(k.into(), v)
    }

    fn hub(options: HubOptions) -> Arc<Hub<String, Prices>> {
        let h = Arc::new(Hub::new(options));
        h.insert("t".into(), Prices::default());
        h
    }

    async fn next(w: &mut Watcher<String, Prices>) -> Event<Prices> {
        tokio::time::timeout(Duration::from_secs(2), w.next()).await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn snapshot_then_deltas_and_resume() {
        let h = hub(HubOptions::default().with_retain(3));
        h.publish(&"t".into(), set("btc", 1)).unwrap();
        let mut w = h.subscribe(&"t".into(), None).unwrap();
        let Event::Snapshot { at, state } = next(&mut w).await else { panic!() };
        assert_eq!((at.seq, state.0.get("btc")), (2, Some(&1)));
        h.publish(&"t".into(), set("eth", 2)).unwrap();
        let Event::Deltas { items, .. } = next(&mut w).await else { panic!() };
        assert_eq!(items, [(3, set("eth", 2))]);
        drop(w);
        assert_eq!(h.subscribers(&"t".into()), 0);

        // Resume from seq 3 after two more deltas: both arrive, no snapshot.
        h.publish_all(&"t".into(), [set("sol", 3), set("ada", 4)]).unwrap();
        let mut w = h.subscribe(&"t".into(), Some(Position { epoch: h.epoch(), seq: 3 })).unwrap();
        assert!(matches!(next(&mut w).await, Event::Resumed { at } if at.seq == 3));
        let Event::Deltas { items, .. } = next(&mut w).await else { panic!() };
        assert_eq!(items.iter().map(|(s, _)| *s).collect::<Vec<_>>(), [4, 5]);

        // Too old for the history (3 kept), another epoch, or ahead of the hub: a snapshot.
        h.publish_all(&"t".into(), [set("a", 1), set("b", 1), set("c", 1)]).unwrap();
        for p in [Position { epoch: h.epoch(), seq: 3 }, Position { epoch: h.epoch() ^ 1, seq: 8 }, Position { epoch: h.epoch(), seq: 99 }] {
            let mut w = h.subscribe(&"t".into(), Some(p)).unwrap();
            assert!(matches!(next(&mut w).await, Event::Snapshot { at, .. } if at.seq == 8), "{p:?}");
        }
        let mut w = h.subscribe(&"t".into(), Some(Position { epoch: h.epoch(), seq: 8 })).unwrap();
        assert!(matches!(next(&mut w).await, Event::Resumed { .. }), "up to date");
        assert!(h.subscribe(&"nope".into(), None).is_err());
    }

    #[tokio::test]
    async fn slow_subscribers_coalesce_then_resnapshot_or_disconnect() {
        let h = hub(HubOptions::default().with_queue(3));
        let mut w = h.subscribe(&"t".into(), None).unwrap();
        next(&mut w).await;
        for i in 0..100 {
            h.publish(&"t".into(), set("btc", i)).unwrap();
        }
        h.publish(&"t".into(), set("eth", 7)).unwrap();
        let Event::Deltas { items, .. } = next(&mut w).await else { panic!() };
        assert_eq!(items, [(101, set("btc", 99)), (102, set("eth", 7))], "latest per key");

        for _ in 0..4 {
            h.publish(&"t".into(), P::Clear).unwrap();
        }
        let Event::Snapshot { at, state } = next(&mut w).await else { panic!("overflow re-snapshots") };
        assert_eq!((at.seq, state), (106, Prices::default()));

        let h = hub(HubOptions::default().with_queue(1).with_overflow(Overflow::Disconnect));
        let mut w = h.subscribe(&"t".into(), None).unwrap();
        next(&mut w).await;
        h.publish_all(&"t".into(), [P::Clear, P::Clear]).unwrap();
        assert!(matches!(next(&mut w).await, Event::Ended { reason } if reason == "lagged"));
        assert!(w.next().await.is_none());
    }

    #[tokio::test]
    async fn replacing_and_removing_topics() {
        let h = hub(HubOptions::default());
        let mut w = h.subscribe(&"t".into(), None).unwrap();
        next(&mut w).await;
        h.publish(&"t".into(), set("x", 1)).unwrap();
        let mut fresh = Prices::default();
        fresh.apply(&set("y", 2));
        h.insert("t".into(), fresh.clone());
        assert!(matches!(next(&mut w).await, Event::Snapshot { state, .. } if state == fresh));
        assert!(h.remove(&"t".into()));
        assert!(matches!(next(&mut w).await, Event::Ended { reason } if reason == "removed"));
        assert!(!h.contains(&"t".into()) && h.topics().is_empty());
    }

    #[tokio::test]
    async fn served_over_a_channel_with_heartbeats_and_rechecks() {
        let h = hub(HubOptions::default());
        let allowed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let a = allowed.clone();
        let auth: Arc<dyn Authorize> =
            Arc::new(
                move |c: &Caller, x: &Access| {
                    if a.load(std::sync::atomic::Ordering::SeqCst) { AllowAll.authorize(c, x) } else { Err(ServiceError::refused("revoked")) }
                },
            );
        let access: AccessFn<String> = Arc::new(|t: &String| Access::new("read").on(t.clone()));
        let (client, server) = memory_pair(8);
        let opts = FeedOptions::default().with_heartbeat(Duration::from_millis(30)).with_recheck(Some(Duration::from_millis(100)));
        let (h2, auth2, access2) = (h.clone(), auth.clone(), access.clone());
        tokio::spawn(async move { serve_feed(h2, access2, auth2, server, "p".into(), &opts).await });
        let mut sub = Subscription::<String, Prices>::start(client, Credentials::new(), "t".into(), None)
            .await
            .unwrap()
            .with_stall(Some(Duration::from_millis(500)));
        assert!(matches!(sub.next().await, Some(Ok(Event::Snapshot { .. }))));
        tokio::time::sleep(Duration::from_millis(80)).await; // heartbeats are swallowed
        h.publish(&"t".into(), set("btc", 5)).unwrap();
        assert!(matches!(sub.next().await, Some(Ok(Event::Deltas { items, .. })) if items == [(2, set("btc", 5))]));
        allowed.store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(sub.next().await.unwrap().unwrap_err(), ServiceError::refused("revoked"));
        assert!(sub.next().await.is_none());

        let (client, server) = memory_pair(8);
        let opts = FeedOptions::default();
        tokio::spawn(async move { serve_feed(h, access, auth, server, "p".into(), &opts).await });
        let refused = Subscription::<String, Prices>::start(client, Credentials::new(), "t".into(), None).await;
        assert_eq!(refused.err(), Some(ServiceError::refused("revoked")));
    }
}
