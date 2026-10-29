//! Listener and subscription behavior, against the emulator.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use scaleset::listener::{Event, JobTracker, Listener, MaxRunners, Scaler};
use scaleset::testing::{Emulator, Endpoint, Fault, JobState};
use scaleset::{
    BoxError, Client, JitRunnerSetting, RunnerScaleSetSpec, ScaleSetId, ScaleSetStatistics,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const WAIT: Duration = Duration::from_secs(10);

/// An emulator with short long polls.
async fn emulator() -> Emulator {
    Emulator::builder()
        .long_poll(Duration::from_millis(50))
        .start()
        .await
}

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
    Listener::new(client.clone(), id, 10)
        .owner("test")
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

/// Handles every event by doing nothing.
struct Noop;

impl Scaler for Noop {
    type Error = BoxError;

    async fn scale(&mut self, _: Event<'_>) -> Result<(), BoxError> {
        Ok(())
    }
}

/// The README's quick-start scaler, plus a job tracker.
struct Autoscaler {
    client: Client,
    scale_set_id: ScaleSetId,
    runners: HashSet<String>,
    tracker: JobTracker,
}

impl Scaler for Autoscaler {
    type Error = BoxError;

    async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
        if let Some(message) = event.message() {
            self.tracker.observe(message);
            for job in message.jobs_completed() {
                self.runners.remove(&job.runner_name);
            }
        }
        let wanted = event.statistics().total_assigned_jobs as usize;
        while self.runners.len() < wanted {
            let setting = JitRunnerSetting::unique("runner");
            self.client
                .generate_jit_config(self.scale_set_id, &setting)
                .await?;
            self.runners.insert(setting.name);
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
            runners: HashSet::new(),
            tracker: JobTracker::new(),
        },
    );

    // Two waves: the scaler must scale up again after scaling down.
    for _ in 0..3 {
        emu.queue_job(id, ["linux"]);
    }
    emu.wait_until(WAIT, |s| {
        s.jobs.iter().all(|j| j.state == JobState::Completed)
    })
    .await;
    for _ in 0..2 {
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
    assert!(scaler.runners.is_empty());
    assert_eq!(scaler.tracker.running() + scaler.tracker.waiting(), 0);
    // The listener closed its session on the way out.
    assert!(emu.state().sessions.is_empty());
}

#[tokio::test]
async fn acquires_available_jobs_itself() {
    let emu = emulator().await;
    let (client, id) = setup(&emu).await;
    let running = Running::spawn(listener(&client, id), Noop);
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
    let emu = emulator().await;
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
    let emu = emulator().await;
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
    let emu = emulator().await;
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
    // The scaler hears about the new session.
    emu.wait_until(WAIT, |_| !seen.lock().unwrap().is_empty())
        .await;
    running.stop().await;
    assert_eq!(seen.lock().unwrap()[0], "initial");
}

#[tokio::test]
async fn opens_a_new_session_when_the_old_one_is_gone() {
    let emu = emulator().await;
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
        .session_id
        .clone();
    emu.expire_session(id);
    let second = emu
        .wait_until(WAIT, |s| {
            s.sessions.len() == 1 && s.sessions[0].session_id != first
        })
        .await;
    assert_eq!(second.sessions[0].owner_name, "test");

    // Each session starts with an initial event.
    let initials = || {
        seen.lock()
            .unwrap()
            .iter()
            .filter(|s| *s == "initial")
            .count()
    };
    emu.wait_until(WAIT, |_| initials() >= 2).await;
    running.stop().await;
    assert_eq!(initials(), 2);
}

#[tokio::test]
async fn survives_service_errors() {
    let emu = emulator().await;
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
    let emu = emulator().await;
    let (client, id) = setup(&emu).await;
    let (tx, mut rx) = tokio::sync::watch::channel(ScaleSetStatistics::default());
    let running = Running::spawn(listener(&client, id), tx);

    emu.queue_job(id, ["linux"]);
    emu.queue_job(id, ["linux"]);
    // The listener acquires both jobs; later statistics show them assigned.
    tokio::time::timeout(
        WAIT,
        rx.wait_for(|s| s.total_assigned_jobs == 2 && s.total_available_jobs == 0),
    )
    .await
    .expect("statistics with both jobs assigned")
    .unwrap();
    running.stop().await;
}

#[tokio::test]
async fn capacity_can_change_while_running() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(20))
        .start()
        .await;
    let (client, id) = setup(&emu).await;
    // A handle made beforehand, shared with the listener.
    let capacity = MaxRunners::new(3);
    let listener = Listener::new(client.clone(), id, capacity.clone())
        .backoff(Duration::from_millis(5), Duration::from_millis(20));
    let (tx, _rx) = tokio::sync::watch::channel(ScaleSetStatistics::default());
    let running = Running::spawn(listener, tx);

    let polled = |n| {
        emu.requests_to(Endpoint::GetMessage)
            .iter()
            .any(|r| r.max_capacity == Some(n))
    };
    emu.wait_until(WAIT, |_| polled(3)).await;
    capacity.set(7);
    emu.wait_until(WAIT, |_| polled(7)).await;
    running.stop().await;
}

#[tokio::test]
async fn subscription_redelivers_until_acknowledged() {
    let emu = emulator().await;
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

#[tokio::test]
async fn failed_steps_are_retried() {
    let emu = emulator().await;
    let (client, id) = setup(&emu).await;
    // Beyond the client's own retries: one acquisition and one
    // acknowledgment fail outright.
    emu.inject(Fault::status(Endpoint::AcquireJobs, 400).times(1));
    emu.inject(Fault::status(Endpoint::DeleteMessage, 400).times(1));

    let seen = Arc::new(Mutex::new(Vec::new()));
    let running = Running::spawn(
        listener(&client, id),
        Flaky {
            failures: 0,
            seen: Arc::clone(&seen),
        },
    );
    emu.queue_job(id, ["linux"]);
    emu.wait_until(WAIT, |s| {
        s.queues_drained() && s.jobs_in(id, JobState::Assigned).count() == 1
    })
    .await;
    running.stop().await;

    // Handled once per failure: the acquisition failed before the scaler
    // ran; the acknowledgment failed after.
    let seen = seen.lock().unwrap();
    let first = seen.iter().filter(|s| *s == "message:1").count();
    assert_eq!(first, 2, "{seen:?}");
}

#[tokio::test]
async fn ack_404_keeps_the_session() {
    let emu = emulator().await;
    let (client, id) = setup(&emu).await;
    // A 404 on acknowledgment means the message is already gone, as after a
    // retry whose first attempt deleted it.
    emu.inject(Fault::status(Endpoint::DeleteMessage, 404).times(1));
    let running = Running::spawn(listener(&client, id).max_failures(0), Noop);
    emu.queue_job(id, ["linux"]);
    emu.wait_until(WAIT, |s| s.jobs_in(id, JobState::Assigned).count() == 1)
        .await;
    // The listener goes on with the same session: the next message is
    // delivered and acknowledged.
    emu.queue_job(id, ["linux"]);
    let acked = || {
        emu.requests_to(Endpoint::DeleteMessage)
            .iter()
            .filter(|r| r.status == 204)
            .count()
    };
    emu.wait_until(WAIT, |_| acked() > 0).await;
    running.stop().await;

    let created = emu
        .requests_to(Endpoint::Sessions)
        .into_iter()
        .filter(|r| r.method == "POST")
        .count();
    assert_eq!(created, 1, "the session was replaced");
}

#[tokio::test]
async fn shutdown_during_scale_leaves_the_message() {
    let emu = emulator().await;
    let (client, id) = setup(&emu).await;
    emu.queue_job(id, ["linux"]);

    struct Stuck(Option<oneshot::Sender<()>>);
    impl Scaler for Stuck {
        type Error = BoxError;
        async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
            if event.message().is_some() {
                if let Some(entered) = self.0.take() {
                    let _ = entered.send(());
                }
                std::future::pending::<()>().await;
            }
            Ok(())
        }
    }
    let (entered, in_scale) = oneshot::channel();
    let running = Running::spawn(listener(&client, id), Stuck(Some(entered)));
    in_scale.await.unwrap();
    running.stop().await;

    let state = emu.state();
    assert_eq!(state.unacknowledged[&id].len(), 1);
    assert!(state.sessions.is_empty());
}

#[tokio::test]
async fn conflicts_are_not_failures() {
    let emu = emulator().await;
    let (client, id) = setup(&emu).await;
    let _other = client.create_message_session(id, "other").await.unwrap();

    let (tx, _rx) = tokio::sync::watch::channel(ScaleSetStatistics::default());
    // Gives up on the first failure, yet waits through the conflicts.
    let running = Running::spawn(listener(&client, id).max_failures(0), tx);
    emu.wait_until(WAIT, |_| {
        emu.requests_to(Endpoint::Sessions)
            .iter()
            .filter(|r| r.status == 409)
            .count()
            >= 3
    })
    .await;
    assert!(!running.task.is_finished());
    running.stop().await;
}

#[tokio::test]
async fn failed_initial_event_is_retried() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_secs(5))
        .start()
        .await;
    let (client, id) = setup(&emu).await;

    struct FailFirst(Arc<Mutex<u32>>);
    impl Scaler for FailFirst {
        type Error = BoxError;
        async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
            if matches!(event, Event::Initial { .. }) {
                let mut calls = self.0.lock().unwrap();
                *calls += 1;
                if *calls == 1 {
                    return Err("not ready".into());
                }
            }
            Ok(())
        }
    }
    let initials = Arc::new(Mutex::new(0));
    let running = Running::spawn(listener(&client, id), FailFirst(Arc::clone(&initials)));
    // Retried at once, not after the next 5 s poll.
    emu.wait_until(Duration::from_secs(2), |_| *initials.lock().unwrap() >= 2)
        .await;
    running.stop().await;
}

#[tokio::test]
async fn subscription_is_cancel_safe() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(300))
        .start()
        .await;
    let (client, id) = setup(&emu).await;
    let session = client.create_message_session(id, "pull").await.unwrap();
    let mut events = session.subscribe(1);
    events.next().await.unwrap(); // Initial

    // Abandon a poll halfway.
    let abandoned = tokio::time::timeout(Duration::from_millis(50), events.next()).await;
    assert!(abandoned.is_err());

    let job = emu.queue_job(id, ["linux"]);
    let event = events.next().await.unwrap();
    let message = event.message().expect("the message");
    assert_eq!(
        message
            .jobs_available()
            .next()
            .unwrap()
            .job
            .runner_request_id,
        job
    );
    session.close().await.unwrap();
}
