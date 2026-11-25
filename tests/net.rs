//! Services and feeds between in-process `weirdnet-net` nodes (memory transport): one user's
//! linked devices through `RosterAuth`, and several people on one server through
//! `MembershipAuth`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use weirdnet_access::{Access as Gate, Catalog, Settings, Visibility, presets};
use weirdnet_net::member::{MemberRevocation, Membership, Revocations};
use weirdnet_net::node::{Handler, Node, NodeConfig};
use weirdnet_net::user::{Card, NodeCert, user_id};
use weirdnet_net::{Keypair, Multiaddr, PeerId, WEIRDNET};
use weirdnet_service::access::{MembershipAuth, Resource};
use weirdnet_service::net::{NetDialer, Server};
use weirdnet_service::roster::RosterAuth;
use weirdnet_service::{
    Access, Admitted, Authorize, Change, Client, Credentials, Event, FeedOptions, Hub, HubOptions, ProtocolId, Service, ServiceError, SessionOptions,
    State,
};
use weirdnet_sync::Roster;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Q;
#[derive(Debug, Clone, Serialize, Deserialize)]
struct R;
struct H;
impl Handler<Q, R> for H {
    fn request(&self, _: &PeerId, _: Q) -> R {
        R
    }
}
type N = Node<Q, R>;

fn start(kp: Keypair) -> Arc<N> {
    let cfg = NodeConfig { listen: vec!["/memory/0".parse().unwrap()], memory: true, ..Default::default() };
    Arc::new(Node::start(&WEIRDNET, kp, cfg, Arc::new(H)).unwrap())
}

fn addr(n: &N) -> Multiaddr {
    n.reachable_addrs().into_iter().find(|a| a.starts_with("/memory/") && !a.contains("p2p-circuit")).unwrap().parse().unwrap()
}

fn certify(user: &Keypair, n: &N, label: &str) -> NodeCert {
    Roster::self_certify(&WEIRDNET, user, &n.peer_id().to_string(), label, weirdnet_net::now_ms()).unwrap()
}

fn card(user: &Keypair, n: &N) -> Card {
    Card { id: user_id(&user.public()), label: "person".into(), key: user.public().encode_protobuf(), nodes: vec![certify(user, n, "device")] }
}

/// A balance per account; adding publishes a delta on the account's topic.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Balance {
    value: i64,
}

impl State for Balance {
    type Delta = i64;
    fn apply(&mut self, d: &i64) {
        self.value += d;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Req {
    Add(String, i64),
    Get(String),
    WhoAmI(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Resp {
    Value(i64),
    Who { user: Option<String>, role: Option<String>, device_roles: Option<Vec<String>> },
}

struct Accounts {
    hub: Arc<Hub<String, Balance>>,
}

impl Service for Accounts {
    type Request = Req;
    type Response = Resp;

    fn access(&self, r: &Req) -> Access {
        match r {
            Req::Add(a, _) => Access::new("trade").on(a.clone()),
            Req::Get(a) | Req::WhoAmI(a) => Access::new("read").on(a.clone()),
        }
    }

    async fn call(&self, who: Admitted, r: Req) -> Result<Resp, ServiceError> {
        match r {
            Req::Add(a, n) => {
                self.hub.publish(&a, n)?;
                Ok(Resp::Value(self.hub.snapshot(&a).map(|(_, b)| b.value).unwrap_or_default()))
            }
            Req::Get(a) => self.hub.snapshot(&a).map(|(_, b)| Resp::Value(b.value)).ok_or_else(|| ServiceError::not_found(a)),
            Req::WhoAmI(_) => Ok(Resp::Who { user: who.user, role: who.role, device_roles: who.device_roles }),
        }
    }
}

fn api(v: u32) -> ProtocolId {
    ProtocolId::new("/weirdnet-service-test/api", v).unwrap()
}

fn feed() -> ProtocolId {
    ProtocolId::new("/weirdnet-service-test/feed", 1).unwrap()
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap()
}

async fn within<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), f).await.expect("timed out")
}

#[test]
fn linked_devices_call_and_follow_a_feed() {
    let user = Keypair::generate_ed25519();
    let (server, app, stranger) = (start(Keypair::generate_ed25519()), start(Keypair::generate_ed25519()), start(Keypair::generate_ed25519()));
    let app_cert = certify(&user, &app, "desktop");
    let mut roster = Roster::new();
    roster.upsert(app_cert.clone());
    roster.upsert(certify(&user, &server, "server"));
    let roster = Arc::new(roster);
    let view: Arc<dyn Fn() -> Roster + Send + Sync> = Arc::new(move || (*roster).clone());

    rt().block_on(within(async {
        let auth: Arc<dyn Authorize> = Arc::new(RosterAuth::new(&WEIRDNET, user.public(), view.clone()));
        let hub = Arc::new(Hub::new(HubOptions::default()));
        hub.insert("acct".to_string(), Balance::default());
        let svc = Arc::new(Accounts { hub: hub.clone() });
        let s = Server::builder(&server)
            .service(&api(1), svc, auth.clone(), SessionOptions::default())
            .feed(
                &feed(),
                hub.clone(),
                |t: &String| Access::new("read").on(t.clone()),
                auth.clone(),
                FeedOptions::default().with_heartbeat(Duration::from_millis(200)),
            )
            .announce("weirdnet-service-test")
            .start()
            .unwrap();
        assert_eq!(s.protocols().len(), 2);
        let node = server.peer_id().to_string();

        let dialer = Arc::new(NetDialer::new(app.clone()));
        dialer.hint(server.peer_id(), vec![addr(&server)]);
        let client = Client::new(dialer).with_credentials(Credentials::new().with_node_cert(&app_cert));

        // Newest version first: v2 is not served, v1 is.
        let (used, session) = client.session_any::<Req, Resp>(&node, &[api(2), api(1)]).await.unwrap();
        assert_eq!(used, api(1));
        assert_eq!(session.call(&Req::Add("acct".into(), 2)).await.unwrap(), Resp::Value(2));
        let one = Req::Add("acct".into(), 1);
        let adds: Vec<_> = (0..10).map(|_| session.call(&one)).collect();
        for r in futures::future::join_all(adds).await {
            r.unwrap();
        }
        assert_eq!(session.call(&Req::Get("acct".into())).await.unwrap(), Resp::Value(12));
        let Resp::Who { user: u, device_roles, .. } = session.call(&Req::WhoAmI("acct".into())).await.unwrap() else { panic!() };
        assert_eq!((u, device_roles), (Some(user_id(&user.public())), None));
        assert_eq!(client.session::<Req, Resp>(&node, &api(3)).await.err().map(|e| matches!(e, ServiceError::Unsupported { .. })), Some(true));

        // A stranger is refused when the session opens.
        let sd = Arc::new(NetDialer::new(stranger.clone()));
        sd.hint(server.peer_id(), vec![addr(&server)]);
        let refused = Client::new(sd.clone()).session::<Req, Resp>(&node, &api(1)).await.err();
        assert_eq!(refused, Some(ServiceError::refused("unknown-device")));
        let forged = Client::new(sd).with_credentials(Credentials::new().with_node_cert(&app_cert));
        assert_eq!(forged.subscribe::<String, Balance>(&node, &feed(), "acct".into(), None).await.err(), Some(ServiceError::refused("wrong-user")));

        // Snapshot, then deltas.
        let mut sub = client.subscribe::<String, Balance>(&node, &feed(), "acct".into(), None).await.unwrap();
        let Some(Ok(Event::Snapshot { at, state })) = sub.next().await else { panic!() };
        assert_eq!(state.value, 12);
        session.call(&Req::Add("acct".into(), 5)).await.unwrap();
        let Some(Ok(Event::Deltas { items, .. })) = sub.next().await else { panic!() };
        assert_eq!(items, [(at.seq + 1, 5)]);
        let last = sub.position().unwrap();
        drop(sub);

        // Missed deltas arrive on resume, without a snapshot.
        session.call(&Req::Add("acct".into(), 1)).await.unwrap();
        session.call(&Req::Add("acct".into(), 1)).await.unwrap();
        let mut sub = client.subscribe::<String, Balance>(&node, &feed(), "acct".into(), Some(last)).await.unwrap();
        assert!(matches!(sub.next().await, Some(Ok(Event::Resumed { at })) if at == last));
        let mut got = Vec::new();
        while got.len() < 2 {
            let Some(Ok(Event::Deltas { items, .. })) = sub.next().await else { panic!() };
            got.extend(items.into_iter().map(|(_, d)| d));
        }
        assert_eq!(got, [1, 1]);
        // Heartbeats keep a quiet stream alive past the stall timeout.
        let mut sub = sub.with_stall(Some(Duration::from_millis(600)));
        let quiet = tokio::time::timeout(Duration::from_millis(1500), sub.next()).await;
        assert!(quiet.is_err(), "nothing but heartbeats: {quiet:?}");

        // A live copy follows the topic.
        let mut live = client.live::<String, Balance>(&node, &feed(), "acct".into());
        assert!(matches!(live.next().await.unwrap(), Change::Snapshot));
        assert_eq!(live.state().unwrap().value, 19);
        session.call(&Req::Add("acct".into(), -9)).await.unwrap();
        assert!(matches!(live.next().await.unwrap(), Change::Deltas(d) if d == [-9]));
        assert_eq!(live.state().unwrap().value, 10);
        hub.remove(&"acct".to_string());
        assert!(matches!(live.next().await.unwrap(), Change::Ended(r) if r == "removed"));
        assert!(matches!(client.subscribe::<String, Balance>(&node, &feed(), "acct".into(), None).await.err(), Some(ServiceError::NotFound { .. })));
        drop(s);
    }));
}

#[test]
fn memberships_and_roles_on_one_server() {
    let owner = Keypair::generate_ed25519();
    let (viewer, trader, outsider) = (Keypair::generate_ed25519(), Keypair::generate_ed25519(), Keypair::generate_ed25519());
    let server = start(Keypair::generate_ed25519());
    let nodes: Vec<Arc<N>> = (0..4).map(|_| start(Keypair::generate_ed25519())).collect();
    let (o, v, t, x) = (&nodes[0], &nodes[1], &nodes[2], &nodes[3]);
    let owner_id = user_id(&owner.public());
    let now = weirdnet_net::now_ms();
    let member =
        |who: &Keypair, resource: &str, role: &str| Membership::issue(&owner, resource, &user_id(&who.public()), role, now - 1000, None).unwrap();

    let mut catalog = Catalog::core();
    catalog.add("trade", "place orders", true).unwrap();
    let roles = Arc::new(presets::ranked(&catalog, &["trade"], &[]).unwrap());
    let revoked = Arc::new(Mutex::new(Revocations::new(&owner_id)));
    let resources: HashMap<String, Resource> =
        [("acct".to_string(), Resource::new(owner.public(), Settings { visibility: Visibility::Private, access: Gate::Gated }, roles.clone()))]
            .into();
    let r2 = revoked.clone();
    let view = Arc::new(move |id: &str| resources.get(id).cloned().map(|r| r.with_revocations(Arc::new(r2.lock().unwrap().clone()))));

    rt().block_on(within(async {
        let auth: Arc<dyn Authorize> = Arc::new(MembershipAuth::new(&WEIRDNET, Arc::new(catalog), view));
        let hub = Arc::new(Hub::new(HubOptions::default()));
        hub.insert("acct".to_string(), Balance::default());
        let _s = Server::builder(&server)
            .service(&api(1), Arc::new(Accounts { hub: hub.clone() }), auth.clone(), SessionOptions::default())
            .feed(
                &feed(),
                hub.clone(),
                |t: &String| Access::new("read").on(t.clone()),
                auth,
                FeedOptions::default().with_recheck(Some(Duration::from_millis(100))),
            )
            .start()
            .unwrap();
        let node = server.peer_id().to_string();
        let client = |n: &Arc<N>, creds: Credentials| {
            let d = Arc::new(NetDialer::new(n.clone()));
            d.hint(server.peer_id(), vec![addr(&server)]);
            Client::new(d).with_credentials(creds)
        };
        let owner_c = client(o, Credentials::new().with_user_card(&card(&owner, o)));
        let viewer_c = client(v, Credentials::new().with_user_card(&card(&viewer, v)).with_membership_of(&member(&viewer, "acct", "viewer")));
        let trader_m = member(&trader, "acct", "user");
        let trader_c = client(t, Credentials::new().with_user_card(&card(&trader, t)).with_membership_of(&trader_m));
        let outsider_c = client(x, Credentials::new().with_user_card(&card(&outsider, x)).with_membership_of(&member(&outsider, "other", "admin")));

        let os = owner_c.session::<Req, Resp>(&node, &api(1)).await.unwrap();
        assert_eq!(os.call(&Req::Add("acct".into(), 10)).await.unwrap(), Resp::Value(10));
        let Resp::Who { user, role, .. } = os.call(&Req::WhoAmI("acct".into())).await.unwrap() else { panic!() };
        assert_eq!((user.as_deref(), role.as_deref()), (Some(owner_id.as_str()), Some("owner")));
        assert!(matches!(os.call(&Req::Get("nope".into())).await, Err(ServiceError::NotFound { .. })));

        let vs = viewer_c.session::<Req, Resp>(&node, &api(1)).await.unwrap();
        assert_eq!(vs.call(&Req::Get("acct".into())).await.unwrap(), Resp::Value(10));
        assert_eq!(vs.call(&Req::Add("acct".into(), 1)).await, Err(ServiceError::refused("role")));

        let ts = trader_c.session::<Req, Resp>(&node, &api(1)).await.unwrap();
        assert_eq!(ts.call(&Req::Add("acct".into(), 1)).await.unwrap(), Resp::Value(11));
        let Resp::Who { role, .. } = ts.call(&Req::WhoAmI("acct".into())).await.unwrap() else { panic!() };
        assert_eq!(role.as_deref(), Some("user"));

        // A membership of another resource, or none: the private account does not exist.
        let xs = outsider_c.session::<Req, Resp>(&node, &api(1)).await.unwrap();
        assert!(matches!(xs.call(&Req::Get("acct".into())).await, Err(ServiceError::NotFound { .. })));
        // Someone else's card for this device is refused outright.
        let thief = client(x, Credentials::new().with_user_card(&card(&trader, t)).with_membership_of(&trader_m));
        assert_eq!(thief.session::<Req, Resp>(&node, &api(1)).await.err(), Some(ServiceError::refused("wrong-user")));

        // Revoking the trader ends their live stream at the next recheck.
        let mut sub = trader_c.subscribe::<String, Balance>(&node, &feed(), "acct".into(), None).await.unwrap();
        assert!(matches!(sub.next().await, Some(Ok(Event::Snapshot { .. }))));
        let rev = MemberRevocation::revoke(&owner, "acct", &user_id(&trader.public()), weirdnet_net::now_ms(), weirdnet_net::now_ms()).unwrap();
        revoked.lock().unwrap().add(rev).unwrap();
        let end = loop {
            match sub.next().await {
                Some(Ok(Event::Deltas { .. })) => continue,
                other => break other,
            }
        };
        assert!(matches!(end, Some(Err(ServiceError::NotFound { .. }))), "{end:?}");
        assert!(matches!(ts.call(&Req::Get("acct".into())).await, Err(ServiceError::NotFound { .. })));
        assert_eq!(vs.call(&Req::Get("acct".into())).await.unwrap(), Resp::Value(11), "others are unaffected");
    }));
}
