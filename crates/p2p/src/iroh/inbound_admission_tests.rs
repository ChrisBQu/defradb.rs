//! A stream is refused before its body is read when its tag is unknown or its
//! message is larger than its tag allows, and reset when its body stalls.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use super::endpoint_streams::admit_stream;
use super::inbound_budget::{InboundBudget, InboundDeadlines, InboundLease};
use super::mux_tests::spawn_transport;
use super::protocols;
use super::services::Priority;
use crate::transport::{InboundClassSnapshot, P2PTransport, PeerAddr, PeerId};

const STOPPED: Duration = Duration::from_secs(5);

async fn bare_endpoint() -> iroh::Endpoint {
    iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(iroh::RelayMode::Disabled)
        .alpns(vec![protocols::ALPN_MUX.to_vec()])
        .bind_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .expect("bind addr")
        .bind()
        .await
        .expect("bind endpoint")
}

/// What the opener of a stream sends after its tag and length.
enum Body {
    Nothing,
    Whole,
    /// One byte every given interval, never the whole body.
    Trickle(Duration),
}

/// Write `tag`, a length prefix of `len`, then `body`, and return the code the
/// stream was stopped with, if any.
async fn open_stream(
    send: &mut iroh::endpoint::SendStream,
    tag: &[u8],
    len: u32,
    body: Body,
) -> Option<u32> {
    protocols::write_stream_tag(send, tag).await.ok()?;
    send.write_all(&len.to_be_bytes()).await.ok()?;
    match body {
        Body::Nothing => {}
        Body::Whole => send.write_all(&vec![0; len as usize]).await.ok()?,
        Body::Trickle(every) => {
            for _ in 1..len {
                tokio::time::sleep(every).await;
                if send.write_all(&[0]).await.is_err() {
                    break;
                }
            }
        }
    }
    let code = send.stopped().await.ok()??;
    Some(u32::try_from(code.into_inner()).expect("small stop code"))
}

/// Dial a bare peer that opens one stream back with `tag` and a length prefix
/// of `len`, and return the code our node stopped that stream with, if it did
/// within `wait`.
async fn stop_code_for(tag: &[u8], len: u32, wait: Duration) -> Option<u32> {
    let bare = bare_endpoint().await;
    let bare_id = PeerId::new(bare.id().to_string());
    let bare_addr = *bare.addr().ip_addrs().next().expect("listener address");

    let tag = tag.to_vec();
    let opener = n0_future::task::spawn({
        let bare = bare.clone();
        async move {
            let connection = bare.accept().await?.await.ok()?;
            let (mut send, _recv) = connection.open_bi().await.ok()?;
            open_stream(&mut send, &tag, len, Body::Nothing).await
        }
    });

    let (transport, endpoint_task) = spawn_transport().await;
    transport
        .dial(&bare_id, vec![PeerAddr::new(bare_addr.to_string())])
        .await
        .expect("dial bare peer");
    let code = n0_future::time::timeout(wait, opener)
        .await
        .ok()
        .and_then(|opened| opened.expect("opener task"));

    transport.shutdown().await.unwrap();
    endpoint_task.await.unwrap();
    code
}

/// What `admit_stream` made of one stream, and how its opener saw it end.
struct Admitted {
    budget: Arc<InboundBudget>,
    lease: Option<(Vec<u8>, InboundLease)>,
    stop_code: Option<u32>,
}

impl Admitted {
    fn class(&self, priority: Priority) -> InboundClassSnapshot {
        let snapshot = self.budget.snapshot();
        snapshot
            .into_iter()
            .find(|class| class.class == priority.name())
            .expect("class snapshot")
    }
}

/// Run `admit_stream` against one stream opened with `tag`, `len` and `body`,
/// under `deadlines`.
async fn admit_one(tag: &[u8], len: u32, body: Body, deadlines: InboundDeadlines) -> Admitted {
    let listener = bare_endpoint().await;
    let opener = bare_endpoint().await;
    let budget = Arc::new(InboundBudget::with_deadlines(deadlines));

    let tag = tag.to_vec();
    let listener_addr = listener.addr();
    let opened = n0_future::task::spawn(async move {
        let connection = opener
            .connect(listener_addr, protocols::ALPN_MUX)
            .await
            .expect("connect");
        let (mut send, _recv) = connection.open_bi().await.expect("open stream");
        let code = open_stream(&mut send, &tag, len, body).await;
        (code, connection, opener)
    });

    let connection = listener
        .accept()
        .await
        .expect("incoming")
        .await
        .expect("accepted");
    let (_send, mut recv) = connection.accept_bi().await.expect("accept stream");
    let share = budget.peer_share(connection.remote_id());
    let peer_id = PeerId::new(connection.remote_id().to_string());
    let lease = admit_stream(&mut recv, &budget, &share, &peer_id)
        .await
        .map(|(_, payload, lease)| (payload, lease));

    let stop_code = if lease.is_some() {
        None
    } else {
        n0_future::time::timeout(STOPPED, opened)
            .await
            .expect("opener sees the stop")
            .expect("opener task")
            .0
    };
    Admitted {
        budget,
        lease,
        stop_code,
    }
}

#[tokio::test]
async fn an_unknown_tag_is_stopped() {
    assert_eq!(
        stop_code_for(b"/defra-iroh/nope/0.1", 1, STOPPED).await,
        Some(2)
    );
}

#[tokio::test]
async fn an_oversized_message_is_stopped_before_its_body_is_read() {
    let too_large = protocols::MAX_MESSAGE_SIZE as u32 + 1;
    assert_eq!(
        stop_code_for(protocols::STREAM_PUSHLOG, too_large, STOPPED).await,
        Some(3)
    );
}

#[tokio::test]
async fn a_car_response_may_be_larger_than_a_general_message() {
    let over_car_limit = protocols::MAX_CAR_SIZE as u32 + 1;
    assert_eq!(
        stop_code_for(protocols::STREAM_CAR_RESP, over_car_limit, STOPPED).await,
        Some(3)
    );

    let car_sized = protocols::MAX_MESSAGE_SIZE as u32 + 1;
    let mut admitted = admit_one(
        protocols::STREAM_CAR_RESP,
        car_sized,
        Body::Whole,
        InboundDeadlines::default(),
    )
    .await;
    let (payload, lease) = admitted.lease.take().expect("a CAR-sized body is admitted");
    assert_eq!(payload.len(), car_sized as usize);
    assert_eq!(
        admitted.class(Priority::Bulk).held_bytes,
        car_sized as usize
    );

    drop(lease);
    assert_eq!(admitted.class(Priority::Bulk).held_bytes, 0);
}

/// Deadlines short enough that a test can wait them out.
fn short_deadlines() -> InboundDeadlines {
    InboundDeadlines {
        admit: Duration::from_millis(200),
        idle: Duration::from_millis(300),
        min_rate: 1000,
        max_body: Duration::from_secs(5),
    }
}

#[tokio::test]
async fn a_body_that_never_arrives_is_stopped_and_frees_its_room() {
    let len = 1000;
    let admitted = admit_one(
        protocols::STREAM_IDENTITY,
        len,
        Body::Nothing,
        short_deadlines(),
    )
    .await;

    assert!(admitted.lease.is_none());
    assert_eq!(admitted.stop_code, Some(5));
    let control = admitted.class(Priority::Control);
    assert_eq!(
        control.held_bytes_high_water, len as usize,
        "the body was admitted"
    );
    assert_eq!(control.held_bytes, 0, "its room was given back");
}

#[tokio::test]
async fn a_body_that_trickles_in_is_stopped_and_frees_its_room() {
    let deadlines = short_deadlines();
    let len = 1000;
    // Often enough that the idle deadline never fires, far too slow for the
    // total one.
    let every = deadlines.idle / 3;
    assert!(every * len > deadlines.body(len as usize));

    let admitted = admit_one(
        protocols::STREAM_IDENTITY,
        len,
        Body::Trickle(every),
        deadlines,
    )
    .await;

    assert!(admitted.lease.is_none());
    assert_eq!(admitted.stop_code, Some(5));
    let control = admitted.class(Priority::Control);
    assert_eq!(
        control.held_bytes_high_water, len as usize,
        "the body was admitted"
    );
    assert_eq!(control.held_bytes, 0, "its room was given back");
}
