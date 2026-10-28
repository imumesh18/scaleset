//! Listener and subscription behavior, against the emulator.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use scaleset::listener::{Event, JobTracker, Listener, Scaler};
use scaleset::testing::{Emulator, Endpoint, Fault, JobState};
use scaleset::{
    BoxError, Client, JitRunnerSetting, RunnerScaleSetSpec, ScaleSetId, ScaleSetStatistics,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const WAIT: Duration = Duration::from_secs(10);

async fn setup(emu: &Emulator) -> (Client, ScaleSetId) {
    let client = emu.client();
    let id = client
        .create_runner_scale_set(&RunnerScaleSetSpec::new("linux"))
        .await
        .unwrap()
        .id;
    (client, id)
}

fn listener(client: &Client, id: ScaleSetId) -> Listener {
    Listener::new(client.clone(), id)
        .owner("test")
        .max_runners(10)
        .backoff(Duration::from_millis(5), Duration::from_millis(20))
}

/// A running listener and a way to stop it.
struct Running<S> {
    stop: oneshot::Sender<()>,
    task: JoinHandle<(Result<(), scaleset::listener::Error>, S)>,
}

impl<S: Scaler + 'static> Running<S> {
    fn spawn(listener: Listener, mut scaler: S) -> Self {
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let result = listener
                .run(&mut scaler, async {
                    let _ = stopped.await;
                })
                .await;
            (result, scaler)
        });
        Self { stop, task }
    }

    async fn stop(self) -> S {
        let _ = self.stop.send(());
        let (result, scaler) = self.task.await.unwrap();
        result.unwrap();
        scaler
    }
}

/// Keeps one runner per assigned job, like a real autoscaler.
struct Autoscaler {
    client: Client,
    scale_set_id: ScaleSetId,
    tracker: JobTracker,
    registered: u32,
}

impl Scaler for Autoscaler {
    type Error = BoxError;

    async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
        if let Some(message) = event.message() {
            self.tracker.observe(message);
            // Ephemeral runners leave with their job.
            let finished = message
                .jobs_completed()
                .filter(|j| !j.runner_name.is_empty())
                .count();
            self.registered -= u32::try_from(finished)?;
        }
        while self.registered < event.statistics().total_assigned_jobs {
            let name = format!("runner-{:08x}", fastrand::u32(..));
            self.client
                .generate_jit_config(self.scale_set_id, &JitRunnerSetting::new(name))
                .await?;
            self.registered += 1;
        }
        Ok(())
    }
}

#[tokio::test]
async fn runs_every_job_and_cleans_up() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(100))
        .auto_run_jobs(Duration::from_millis(30))
        .start()
        .await;
    let (client, id) = setup(&emu).await;
    let running = Running::spawn(
        listener(&client, id),
        Autoscaler {
            client: client.clone(),
            scale_set_id: id,
            tracker: JobTracker::new(),
            registered: 0,
        },
    );

    for _ in 0..5 {
        emu.queue_job(id, ["linux"]);
    }
    emu.wait_until(WAIT, |s| {
        s.jobs.len() == 5
            && s.jobs.iter().all(|j| j.state == JobState::Completed)
            && s.queues_drained()
            && s.runners.is_empty()
    })
    .await;

    let scaler = running.stop().await;
    assert_eq!(scaler.registered, 0);
    assert_eq!(scaler.tracker.running() + scaler.tracker.waiting(), 0);
    // The listener closed its session on the way out.
    assert!(emu.state().sessions.is_empty());
}

#[tokio::test]
async fn acquires_available_jobs_itself() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(50))
        .start()
        .await;
    let (client, id) = setup(&emu).await;

    struct Nothing;
    impl Scaler for Nothing {
        type Error = BoxError;
        async fn scale(&mut self, _: Event<'_>) -> Result<(), BoxError> {
            Ok(())
        }
    }
    let running = Running::spawn(listener(&client, id), Nothing);
    emu.queue_job(id, ["linux"]);
    emu.wait_until(WAIT, |s| s.jobs_in(id, JobState::Assigned).count() == 1)
        .await;
    running.stop().await;
}

/// Fails its first `failures` messages, then records what it sees.
#[derive(Default)]
struct Flaky {
    failures: u32,
    seen: Arc<Mutex<Vec<String>>>,
}

impl Scaler for Flaky {
    type Error = BoxError;

    async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
        let label = match event {
            Event::Initial { .. } => "initial".to_owned(),
            Event::Message { message, .. } => format!("message:{}", message.id),
            _ => "idle".to_owned(),
        };
        self.seen.lock().unwrap().push(label);
        if event.message().is_some() && self.failures > 0 {
            self.failures -= 1;
            return Err("provisioning failed".into());
        }
        Ok(())
    }
}

#[tokio::test]
async fn scaler_failure_is_retried_with_the_same_message() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(50))
        .start()
        .await;
    let (client, id) = setup(&emu).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let running = Running::spawn(
        listener(&client, id),
        Flaky {
            failures: 2,
            seen: Arc::clone(&seen),
        },
    );

    emu.queue_job(id, ["linux"]);
    emu.wait_until(WAIT, |s| s.queues_drained() && !s.jobs.is_empty())
        .await;
    running.stop().await;

    // Delivered three times: twice failed, then handled and acknowledged.
    let seen = seen.lock().unwrap();
    let first = seen.iter().filter(|s| *s == "message:1").count();
    assert_eq!(first, 3, "{seen:?}");
}

#[tokio::test]
async fn gives_up_after_max_failures() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(50))
        .start()
        .await;
    let (client, id) = setup(&emu).await;
    emu.queue_job(id, ["linux"]);

    let mut scaler = Flaky {
        failures: u32::MAX,
        ..Flaky::default()
    };
    let err = listener(&client, id)
        .max_failures(2)
        .run(&mut scaler, std::future::pending())
        .await
        .unwrap_err();
    assert!(matches!(err, scaleset::listener::Error::Scale(_)), "{err}");
    // The message stays for whoever runs next, and the session is closed.
    let state = emu.state();
    assert_eq!(state.unacknowledged[&id].len(), 1);
    assert!(state.sessions.is_empty());
}

#[tokio::test]
async fn waits_out_a_stale_session() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(50))
        .start()
        .await;
    let (client, id) = setup(&emu).await;
    // The session of a listener that crashed is still open.
    let stale = client.create_message_session(id, "crashed").await.unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let running = Running::spawn(
        listener(&client, id),
        Flaky {
            failures: 0,
            seen: Arc::clone(&seen),
        },
    );
    emu.wait_until(WAIT, |_| {
        emu.requests_to(Endpoint::Sessions)
            .iter()
            .filter(|r| r.status == 409)
            .count()
            >= 2
    })
    .await;
    assert!(seen.lock().unwrap().is_empty());

    stale.close().await.unwrap();
    emu.wait_until(WAIT, |s| s.sessions.iter().any(|s| s.owner_name == "test"))
        .await;
    running.stop().await;
    assert_eq!(seen.lock().unwrap()[0], "initial");
}

#[tokio::test]
async fn opens_a_new_session_when_the_old_one_is_gone() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(50))
        .start()
        .await;
    let (client, id) = setup(&emu).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let running = Running::spawn(
        listener(&client, id),
        Flaky {
            failures: 0,
            seen: Arc::clone(&seen),
        },
    );

    let first = emu
        .wait_until(WAIT, |s| s.sessions.len() == 1)
        .await
        .sessions[0]
        .session_id;
    emu.expire_session(id);
    let second = emu
        .wait_until(WAIT, |s| {
            s.sessions.len() == 1 && s.sessions[0].session_id != first
        })
        .await;
    assert_eq!(second.sessions[0].owner_name, "test");

    running.stop().await;
    let initials = seen
        .lock()
        .unwrap()
        .iter()
        .filter(|s| *s == "initial")
        .count();
    assert_eq!(initials, 2);
}

#[tokio::test]
async fn survives_service_errors() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(50))
        .start()
        .await;
    let (client, id) = setup(&emu).await;
    // More 500s than the client's own retries absorb.
    emu.inject(Fault::status(Endpoint::GetMessage, 500).times(12));

    let seen = Arc::new(Mutex::new(Vec::new()));
    let running = Running::spawn(
        listener(&client, id),
        Flaky {
            failures: 0,
            seen: Arc::clone(&seen),
        },
    );
    emu.queue_job(id, ["linux"]);
    emu.wait_until(WAIT, |s| s.queues_drained() && !s.jobs.is_empty())
        .await;
    running.stop().await;
    assert!(seen.lock().unwrap().contains(&"message:1".to_owned()));
}

#[tokio::test]
async fn publishes_statistics_on_a_watch_channel() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(50))
        .start()
        .await;
    let (client, id) = setup(&emu).await;
    let (tx, mut rx) = tokio::sync::watch::channel(ScaleSetStatistics::default());
    let running = Running::spawn(listener(&client, id), tx);

    emu.queue_job(id, ["linux"]);
    emu.queue_job(id, ["linux"]);
    let stats = tokio::time::timeout(
        WAIT,
        rx.wait_for(|s| s.total_assigned_jobs + s.total_available_jobs == 2),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(stats.total_assigned_jobs <= 2);
    drop(stats);
    running.stop().await;
}

#[tokio::test]
async fn capacity_can_change_while_running() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(20))
        .start()
        .await;
    let (client, id) = setup(&emu).await;
    let listener = listener(&client, id).max_runners(3);
    let capacity = listener.capacity();
    let (tx, _rx) = tokio::sync::watch::channel(ScaleSetStatistics::default());
    let running = Running::spawn(listener, tx);

    emu.wait_until(WAIT, |_| !emu.requests_to(Endpoint::GetMessage).is_empty())
        .await;
    capacity.set(7);
    emu.wait_until(WAIT, |_| emu.requests_to(Endpoint::GetMessage).len() >= 3)
        .await;
    running.stop().await;
    assert_eq!(capacity.get(), 7);
}

#[tokio::test]
async fn subscription_redelivers_until_acknowledged() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(50))
        .start()
        .await;
    let (client, id) = setup(&emu).await;
    let session = client.create_message_session(id, "pull").await.unwrap();
    let mut events = session.subscribe(5);

    let initial = events.next().await.unwrap();
    assert!(matches!(initial, Event::Initial { .. }));
    assert!(matches!(events.next().await.unwrap(), Event::Idle { .. }));

    let job = emu.queue_job(id, ["linux"]);
    let first = events.next().await.unwrap().message().unwrap().id;
    // Not acknowledged: the next poll gets it again.
    let event = events.next().await.unwrap();
    let message = event.message().unwrap();
    assert_eq!(message.id, first);
    assert_eq!(
        message
            .jobs_available()
            .next()
            .unwrap()
            .job
            .runner_request_id,
        job
    );
    assert_eq!(event.statistics().total_available_jobs, 1);

    events.ack().await.unwrap();
    assert!(matches!(events.next().await.unwrap(), Event::Idle { .. }));
    // Idle events repeat the last statistics.
    assert_eq!(events.statistics().total_available_jobs, 1);
    session.close().await.unwrap();
}
