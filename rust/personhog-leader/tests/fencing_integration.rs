//! Broker-enforced fencing, proven against a real broker: a partition's
//! new owner initializing its transactional id must make the previous
//! owner's producer unusable. Without the fence, a stale owner's writes
//! land in the changelog silently — the exact zombie hazard this
//! mechanism exists to close.

mod common;

use std::sync::Arc;
use std::time::Duration;

use personhog_coordination::authority::AuthorityClock;
use personhog_leader::fencing::{
    heal_fence, FenceGuard, FencedChangelogProducers, FencedProduceError,
};
use personhog_leader::inflight::InflightTracker;
use personhog_proto::personhog::types::v1::Person;
use tokio::time::sleep;

use common::{test_kafka_config, KAFKA_BOOTSTRAP};

fn test_person(version: i64) -> Person {
    Person {
        id: 7,
        uuid: "00000000-0000-0000-0000-000000000007".to_string(),
        team_id: 1,
        properties: b"{}".to_vec(),
        version,
        ..Default::default()
    }
}

/// Count the records a `read_committed` consumer can see on partition 0
/// — the same isolation the warming path uses.
async fn read_committed_count(topic: &str) -> usize {
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BOOTSTRAP)
        .set(
            "group.id",
            format!("fence-abort-probe-{}", uuid::Uuid::new_v4()),
        )
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .create()
        .expect("probe consumer");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning)
        .expect("assign");
    consumer.assign(&tpl).expect("assign");

    let mut seen = 0;
    // Anything committed is available immediately; the quiet period only
    // has to outlast delivery, not a transaction timeout.
    while let Ok(Ok(message)) =
        tokio::time::timeout(Duration::from_millis(750), consumer.recv()).await
    {
        if message.payload().is_some() {
            seen += 1;
        }
    }
    seen
}

/// Comfortably above the test config's 5s `message.timeout.ms`, which
/// librdkafka requires the broker bound to cover.
const BROKER_TXN_TIMEOUT: Duration = Duration::from_secs(30);

fn fenced_producers_with_window(topic: &str, window: Duration) -> FencedChangelogProducers {
    let mut kafka = test_kafka_config();
    kafka.kafka_hosts = KAFKA_BOOTSTRAP.to_string();
    FencedChangelogProducers::new(
        kafka,
        topic.to_string(),
        Duration::from_secs(10),
        Duration::from_secs(10),
        BROKER_TXN_TIMEOUT,
        window,
    )
}

fn fenced_producers(topic: &str) -> FencedChangelogProducers {
    let mut kafka = test_kafka_config();
    kafka.kafka_hosts = KAFKA_BOOTSTRAP.to_string();
    FencedChangelogProducers::new(
        kafka,
        topic.to_string(),
        Duration::from_secs(10),
        Duration::from_secs(10),
        BROKER_TXN_TIMEOUT,
        Duration::from_millis(5),
    )
}

/// The core fencing guarantee: after a second owner acquires the
/// partition, the first owner's produce fails as fenced instead of
/// landing in the changelog.
#[tokio::test]
async fn second_acquisition_fences_the_first_producer() {
    let topic = format!("fence_test_{}", uuid::Uuid::new_v4().simple());

    let first = fenced_producers(&topic);
    first.acquire(0).await.expect("first owner acquires");
    first
        .produce(0, &test_person(1))
        .await
        .expect("first owner produces while unfenced");

    let second = fenced_producers(&topic);
    second.acquire(0).await.expect("second owner acquires");
    second
        .produce(0, &test_person(2))
        .await
        .expect("new owner produces");

    match first.produce(0, &test_person(3)).await {
        Err(FencedProduceError::Fenced) | Err(FencedProduceError::NotAcquired) => {}
        other => panic!("stale owner must be fenced, got {other:?}"),
    }
}

/// Concurrent same-partition writes share a transaction window: both
/// succeed with distinct offsets, through one producer, without
/// serializing on per-write commits.
#[tokio::test]
async fn concurrent_writes_share_a_window() {
    let topic = format!("fence_test_{}", uuid::Uuid::new_v4().simple());
    let producers = Arc::new(fenced_producers(&topic));
    producers.acquire(0).await.expect("acquire");

    let a = {
        let p = Arc::clone(&producers);
        tokio::spawn(async move { p.produce(0, &test_person(1)).await })
    };
    let b = {
        let p = Arc::clone(&producers);
        tokio::spawn(async move { p.produce(0, &test_person(2)).await })
    };
    let (a, b) = (a.await.unwrap().unwrap(), b.await.unwrap().unwrap());
    assert_ne!(a, b, "each write must get its own offset");
}

/// Sustained open-loop arrivals across many window turnovers: every
/// write must land and every waiter must be woken through dozens of
/// open → drain → commit cycles. A lost `window_closed` wakeup or a
/// dropped commit-outcome waiter hangs this test; the single-window
/// tests above never exercise turnover.
#[tokio::test]
async fn sustained_writes_across_window_boundaries() {
    // Bounded: a lost `window_closed` wakeup parks every writer forever,
    // and an unbounded test reports that as a stuck runner rather than a
    // failure.
    tokio::time::timeout(Duration::from_secs(60), async {
        let topic = format!("fence_test_{}", uuid::Uuid::new_v4().simple());
        let producers = Arc::new(fenced_producers(&topic));
        producers.acquire(0).await.expect("acquire");

        let writes: Vec<_> = (0..200i64)
            .map(|k| {
                let p = Arc::clone(&producers);
                tokio::spawn(async move {
                    sleep(Duration::from_millis(((k * 7) % 97) as u64)).await;
                    p.produce(0, &test_person(k)).await
                })
            })
            .collect();
        for write in writes {
            write
                .await
                .unwrap()
                .expect("every write must land across window boundaries");
        }
    })
    .await
    .expect("writes parked forever — a window_closed wakeup was lost");
}

/// A request that vanishes mid-produce — tonic drops the handler future
/// when the client's deadline expires — must return its seat in the
/// window. Without that, the committer waits on an in-flight count that
/// never reaches zero and every later write on the partition parks
/// forever behind it.
#[tokio::test]
async fn a_cancelled_produce_does_not_wedge_the_partition() {
    let topic = format!("fence_cancel_{}", uuid::Uuid::new_v4().simple());
    let producers = Arc::new(fenced_producers(&topic));
    producers.acquire(0).await.expect("acquire");

    {
        let p = Arc::clone(&producers);
        let mut inflight = Box::pin(async move { p.produce(0, &test_person(1)).await });
        // Far too little time to finish: the future is dropped mid-send.
        tokio::time::timeout(Duration::from_micros(200), &mut inflight)
            .await
            .ok();
    }

    tokio::time::timeout(
        Duration::from_secs(10),
        producers.produce(0, &test_person(2)),
    )
    .await
    .expect("a later write must not hang behind the cancelled one")
    .expect("and must succeed");
}

/// Does a successor's `init_transactions` abort the predecessor's open
/// transaction, or merely stop it from committing?
///
/// The drain does not wait for open transaction windows, which is only
/// safe if a record abandoned in one cannot become visible after the
/// partition moves. This pins that: the successor's acquire — which
/// precedes its warm read — leaves the predecessor unable to commit.
///
/// Without this guarantee the drain would have to wait out every open
/// window before acking, so the assertion is load-bearing rather than
/// incidental.
#[tokio::test]
async fn a_successors_init_aborts_the_predecessors_open_window() {
    let topic = format!("fence_abort_{}", uuid::Uuid::new_v4().simple());

    // Predecessor: open a window and get a record enqueued into it, then
    // abandon it exactly as a cancelled request would. The window is
    // short on purpose — its committer must fire *after* the successor
    // has taken the epoch, because "invisible while uncommitted" proves
    // nothing. What has to be shown is that the record can never become
    // visible once the successor owns the partition.
    let first = Arc::new(fenced_producers_with_window(&topic, Duration::from_secs(1)));
    first.acquire(0).await.expect("first owner acquires");
    {
        let p = Arc::clone(&first);
        let mut inflight = Box::pin(async move { p.produce(0, &test_person(1)).await });
        // Long enough for the send to reach the broker, far too short
        // for the window to close.
        tokio::time::timeout(Duration::from_millis(200), &mut inflight)
            .await
            .ok();
    }

    // Successor takes the partition before that window closes, as a
    // warming new owner does.
    let second = fenced_producers(&topic);
    second.acquire(0).await.expect("successor acquires");

    // Now let the predecessor's committer run. This is the moment the
    // drain's wait exists to prevent: an abandoned record committing
    // after the successor is already the owner.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Read the partition the way warming does.
    let visible = read_committed_count(&topic).await;
    assert_eq!(
        visible, 0,
        "an abandoned record must not become readable after the successor's init — \
         if this fails, the drain must wait for open windows before acking"
    );

    // Zero is also what a partition nothing was ever produced to looks
    // like, so the same sequence without a successor has to show the
    // record arriving. Otherwise this test passes just as well when the
    // send never left the client.
    let control_topic = format!("fence_abort_control_{}", uuid::Uuid::new_v4().simple());
    let lone = Arc::new(fenced_producers_with_window(
        &control_topic,
        Duration::from_secs(1),
    ));
    lone.acquire(0).await.expect("control owner acquires");
    {
        let p = Arc::clone(&lone);
        let mut inflight = Box::pin(async move { p.produce(0, &test_person(1)).await });
        tokio::time::timeout(Duration::from_millis(200), &mut inflight)
            .await
            .ok();
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        read_committed_count(&control_topic).await,
        1,
        "with no successor the abandoned record commits — without this the assertion \
         above cannot tell an aborted window from a send that never happened"
    );
}

/// A warm that never finishes must not leave this process holding the
/// partition's broker epoch.
///
/// The pod records a partition as held only once the warm returns, so a
/// fence taken by a warm whose future is dropped — what a lost lease
/// does to an in-flight attempt — belongs to no partition the local
/// self-fence knows to release. The process would keep the epoch while
/// owning nothing, and the real owner's writes would fail as fenced.
#[tokio::test]
async fn a_fence_taken_for_an_unfinished_warm_is_given_back() {
    let topic = format!("fence_guard_{}", uuid::Uuid::new_v4().simple());
    let producers = Arc::new(fenced_producers(&topic));

    {
        producers.acquire(0).await.expect("acquire");
        let _guard = FenceGuard::new(Arc::clone(&producers), 0);
        // The warm ends here without returning, as a torn-down attempt
        // does.
    }

    match producers.produce(0, &test_person(1)).await {
        Err(FencedProduceError::NotAcquired) => {}
        other => panic!("the fence should have been given back, got {other:?}"),
    }
}

/// A warm that finishes keeps what it took.
#[tokio::test]
async fn a_completed_warm_keeps_its_fence() {
    let topic = format!("fence_guard_{}", uuid::Uuid::new_v4().simple());
    let producers = Arc::new(fenced_producers(&topic));

    producers.acquire(0).await.expect("acquire");
    FenceGuard::new(Arc::clone(&producers), 0).keep();

    producers
        .produce(0, &test_person(1))
        .await
        .expect("a completed warm keeps a usable fence");
}

/// A producer whose abort exhausted its retries, or whose commit outcome
/// stayed unknown, is left in a transaction state it cannot begin another
/// window from. It is still installed, so nothing that checks for the
/// *presence* of a fence can tell it apart from a working one.
///
/// The partition must therefore stop reporting itself as fenced and start
/// answering writes as an ownership question, which is what a router can
/// act on and what a repair pass looks for. Reporting a retryable failure
/// instead leaves every write on the partition failing for as long as the
/// process lives, with reads still served and nothing to escalate.
#[tokio::test]
async fn a_condemned_producer_stops_claiming_the_partition() {
    let topic = format!("fence_condemned_{}", uuid::Uuid::new_v4().simple());
    let producers = fenced_producers(&topic);
    producers.acquire(0).await.expect("acquire the fence");
    producers
        .produce(0, &test_person(1))
        .await
        .expect("a healthy fence writes");

    producers.condemn_for_test(0);

    match producers.produce(0, &test_person(2)).await {
        Err(FencedProduceError::NotAcquired) => {}
        other => panic!("a condemned producer must not answer as a live fence, got {other:?}"),
    }

    // And it must have been given up rather than merely refused once: a
    // re-acquisition is the only thing that makes the partition writable
    // again, and it can only run against a partition this pod no longer
    // claims to fence.
    producers.acquire(0).await.expect("re-acquire the fence");
    producers
        .produce(0, &test_person(3))
        .await
        .expect("a re-acquired fence writes again");
}

/// A guard outlives the fence it was taken for when a warm is abandoned
/// and the partition is re-acquired before the guard drops. Releasing by
/// partition alone would then evict the *replacement* — a live fence, on
/// a partition this pod legitimately owns — and every write would fail as
/// unowned until something re-acquired again.
#[tokio::test]
async fn an_abandoned_guard_does_not_evict_its_replacement() {
    let topic = format!("fence_guard_id_{}", uuid::Uuid::new_v4().simple());
    let producers = Arc::new(fenced_producers(&topic));
    producers.acquire(0).await.expect("first acquire");

    // A warm takes the fence, then never finishes.
    let stale = FenceGuard::new(Arc::clone(&producers), 0);

    // Meanwhile the partition is released and taken again, so what is
    // installed is no longer what the guard is answerable for.
    producers.release(0);
    producers.acquire(0).await.expect("re-acquire");

    drop(stale);

    producers
        .produce(0, &test_person(1))
        .await
        .expect("the replacement fence must survive the stale guard");
}

/// A partition can end up served without a fence — a broker rejection
/// evicted it, an abort exhausted its retries, a stale pod took the
/// epoch and stepped back. Convergence sees such a partition warmed and
/// unfenced and does nothing, so this is what gets it writable again
/// before the next handoff.
#[tokio::test]
async fn healing_retakes_a_fence_for_a_served_partition() {
    let topic = format!("fence_heal_{}", uuid::Uuid::new_v4().simple());
    let producers = Arc::new(fenced_producers(&topic));
    let inflight = InflightTracker::new();
    let clock = AuthorityClock::unclaimed();
    clock.begin_session(Duration::from_secs(30), std::time::Instant::now());

    heal_fence(&producers, &inflight, Some(&clock), 0).await;

    producers
        .produce(0, &test_person(1))
        .await
        .expect("a served partition must regain a usable fence");
}

/// Healing takes the partition's epoch from whoever holds it, so a pod
/// that cannot vouch for its own claim must not start the round trip at
/// all — `init_transactions` cannot be undone once it returns, and the
/// post-acquire check can only stop *this* pod from building on a fence
/// it already stole.
///
/// The assertion is therefore on the victim. Asking only whether we
/// ended up holding a fence cannot tell the pre-check from the
/// post-check: both leave us empty-handed, and only one of them leaves
/// the legitimate owner still able to write.
#[tokio::test]
async fn healing_without_standing_does_not_steal_the_epoch() {
    let topic = format!("fence_heal_{}", uuid::Uuid::new_v4().simple());
    let inflight = InflightTracker::new();

    // The partition's real owner, holding a working fence.
    let owner = Arc::new(fenced_producers(&topic));
    owner.acquire(0).await.expect("the owner takes its fence");
    owner
        .produce(0, &test_person(1))
        .await
        .expect("the owner can write");

    // A pod whose claim is gone tries to heal the same partition.
    let zombie = Arc::new(fenced_producers(&topic));
    let lapsed = AuthorityClock::unclaimed();
    lapsed.begin_session(Duration::from_secs(30), std::time::Instant::now());
    lapsed.surrender();
    heal_fence(&zombie, &inflight, Some(&lapsed), 0).await;

    owner
        .produce(0, &test_person(2))
        .await
        .expect("a pod with no standing must not take the epoch from the real owner");
}

/// A partition a handoff is already moving belongs to the incoming
/// owner, so healing must leave it alone for the same reason.
#[tokio::test]
async fn healing_skips_a_partition_under_handoff() {
    let topic = format!("fence_heal_{}", uuid::Uuid::new_v4().simple());
    let inflight = InflightTracker::new();

    let owner = Arc::new(fenced_producers(&topic));
    owner.acquire(0).await.expect("the owner takes its fence");

    let other = Arc::new(fenced_producers(&topic));
    let valid = AuthorityClock::unclaimed();
    valid.begin_session(Duration::from_secs(30), std::time::Instant::now());
    inflight.fence(0);
    heal_fence(&other, &inflight, Some(&valid), 0).await;

    owner
        .produce(0, &test_person(1))
        .await
        .expect("a partition being handed off is not ours to take");
}

/// Standing can lapse *during* the broker round trip, by which point the
/// fence is installed. Keeping it is not passive: the write path trusts
/// the broker epoch rather than re-checking the claim, so a request
/// landing here would ack a mutation with an epoch taken from the
/// partition's real owner.
#[tokio::test]
async fn healing_gives_back_a_fence_it_lost_standing_for() {
    let topic = format!("fence_heal_{}", uuid::Uuid::new_v4().simple());
    let producers = Arc::new(fenced_producers(&topic));
    let inflight = InflightTracker::new();

    let clock = Arc::new(AuthorityClock::unclaimed());
    clock.begin_session(Duration::from_secs(30), std::time::Instant::now());
    let losing = Arc::clone(&clock);
    let lease_loss = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        losing.surrender();
    });

    heal_fence(&producers, &inflight, Some(&clock), 0).await;
    lease_loss.await.unwrap();

    match producers.produce(0, &test_person(1)).await {
        Err(FencedProduceError::NotAcquired) => {}
        other => panic!("a fence taken without standing must be given back, got {other:?}"),
    }
}

/// A writer parked behind a committing window is woken by the very commit
/// that condemns the producer. Checking usability only before the park
/// skips exactly that writer: it wakes, finds the gate idle, and opens a
/// window on a producer that cannot begin one — answering with a
/// retryable failure the client retries against a pod that cannot write
/// the partition, instead of the ownership bounce that moves it.
#[tokio::test]
async fn a_writer_woken_onto_a_condemned_producer_is_bounced() {
    let topic = format!("fence_woken_{}", uuid::Uuid::new_v4().simple());
    let producers = Arc::new(fenced_producers(&topic));
    producers.acquire(0).await.expect("acquire the fence");

    // Stage the gate exactly as a commit in flight leaves it, so the
    // write below parks rather than opening its own window.
    producers.begin_committing_for_test(0);
    let parked = {
        let p = Arc::clone(&producers);
        tokio::spawn(async move { p.produce(0, &test_person(1)).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The commit resolves badly and condemns the producer, then releases
    // the gate and wakes the parked writer — production's exact order.
    producers.condemn_for_test(0);
    producers.finish_committing_for_test(0);

    match parked.await.expect("the parked task must not panic") {
        Err(FencedProduceError::NotAcquired) => {}
        other => {
            panic!("a writer woken onto a condemned producer must answer as unowned, got {other:?}")
        }
    }
}

/// Healing must leave a partition it already holds alone. Re-acquiring
/// runs `init_transactions`, which bumps the broker epoch — so a healing
/// pass that ignored the fence it is already holding would fence this
/// pod's own producer on every reconcile tick.
///
/// The damage lands on writes already in flight, not on the next one: a
/// fresh write simply uses whichever producer is installed. So the write
/// here is mid-window when the tick runs.
#[tokio::test]
async fn healing_leaves_a_fence_it_already_holds_alone() {
    let topic = format!("fence_heal_noop_{}", uuid::Uuid::new_v4().simple());
    let producers = Arc::new(fenced_producers_with_window(
        &topic,
        Duration::from_millis(600),
    ));
    let inflight = InflightTracker::new();
    let clock = AuthorityClock::unclaimed();
    clock.begin_session(Duration::from_secs(30), std::time::Instant::now());

    producers.acquire(0).await.expect("take the fence");

    // In flight: the window is open and its commit has not fired.
    let writing = {
        let p = Arc::clone(&producers);
        tokio::spawn(async move { p.produce(0, &test_person(1)).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A reconcile tick with everything healthy.
    heal_fence(&producers, &inflight, Some(&clock), 0).await;

    let result = writing.await.expect("the write task must not panic");
    assert!(
        result.is_ok(),
        "a healing pass must not fence the window this pod is already \
         filling, got {result:?}"
    );
}
