# scaleset

A Rust client for the GitHub Actions **Runner Scale Set** APIs. Build your own
autoscaler for self-hosted runners, without Kubernetes or
[actions-runner-controller](https://github.com/actions/actions-runner-controller).

It implements the same API contract as GitHub's Go client,
[`github.com/actions/scaleset`](https://github.com/actions/scaleset), with an
API designed for Rust: async on Tokio, `reqwest` for HTTP, typed IDs, enums
for errors and events, and `tracing` for logs. See
[`docs/DESIGN.md`](docs/DESIGN.md) for how and why it differs from the Go
client.

```toml
[dependencies]
scaleset = { git = "https://github.com/imumesh18/scaleset" }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
```

## What is a scale set?

A runner scale set is a group of self-hosted runners that autoscales with
workflow demand.

1. **Register** a scale set with a name. The name is the label workflows
   target with `runs-on`. Scale sets can live at repository, organization or
   enterprise level.
2. **Poll** its message queue, reporting how many runners you can run.
3. GitHub **matches jobs** to the scale set and tells you how many runners it
   needs: `statistics.total_assigned_jobs`.
4. **Start runners** with just-in-time (JIT) configs. Each runner takes one
   job and exits.

## Example

```rust,no_run
use scaleset::listener::{Event, Listener, Scaler};
use scaleset::{
    BoxError, Client, Credentials, JitRunnerSetting, MessageSession, RunnerGroupId,
    RunnerScaleSetSpec, ScaleSetId,
};

struct Autoscaler {
    client: Client,
    session: MessageSession,
    scale_set_id: ScaleSetId,
    running: u32,
}

impl Scaler for Autoscaler {
    type Error = BoxError;

    async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
        // Acquire the jobs you want, or they stay unassigned.
        if let Event::Message(msg) = event {
            let jobs: Vec<_> = msg.jobs_available().map(|j| j.job.runner_request_id).collect();
            if !jobs.is_empty() {
                self.session.acquire_jobs(&jobs).await?;
            }
        }

        // Scale on statistics, never on message counts.
        let Some(stats) = event.statistics() else { return Ok(()) };
        while self.running < stats.total_assigned_jobs {
            let jit = self
                .client
                .generate_jit_config(
                    self.scale_set_id,
                    &JitRunnerSetting::new(format!("runner-{}", self.running)),
                )
                .await?;
            // Start a runner with ACTIONS_RUNNER_INPUT_JITCONFIG=jit.encoded_jit_config
            self.running += 1;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let pem = std::fs::read_to_string("app.pem")?;
    let client = Client::builder(
        "https://github.com/my-org",
        Credentials::github_app("Iv1.abc123", 12345, &pem)?,
    )
    .build()?;

    let scale_set = client
        .create_runner_scale_set(
            &RunnerScaleSetSpec::new("my-scale-set").runner_group_id(RunnerGroupId::DEFAULT),
        )
        .await?;
    let session = client.create_message_session(scale_set.id, "my-host").await?;

    let mut scaler = Autoscaler {
        client: client.clone(),
        session: session.clone(),
        scale_set_id: scale_set.id,
        running: 0,
    };
    let result = Listener::new(session.clone(), 10)
        .run(&mut scaler, async { let _ = tokio::signal::ctrl_c().await; })
        .await;

    session.close().await?;
    client.delete_runner_scale_set(scale_set.id).await?;
    result?;
    Ok(())
}
```

[`examples/autoscaler.rs`](examples/autoscaler.rs) is a complete autoscaler
that runs each runner through a shell command (for example `docker run`) and
supports a minimum number of warm runners.

## How the message API works

- **Long polling.** `MessageSession::get_message` returns as soon as there is
  news, and otherwise returns `None` after about 50 seconds. Poll again right
  away.
- **Acknowledgment.** Call `delete_message` after handling a message.
  Unacknowledged messages are redelivered, so a crash does not lose them.
- **Message IDs.** Pass the last handled message ID to the next poll.
- **Scale on statistics.** A batch holds at most 50 messages, and a job can be
  assigned, canceled and requeued up to three times, each time with new
  messages. `total_assigned_jobs` is always current.
- **Job messages are still useful** for other things: marking runners busy on
  `JobStarted`, metrics, logs.

The `listener` module runs this loop for you: it hands your `Scaler` the
initial statistics, then each message (or `Event::Idle`), and acknowledges a
message only after `scale` succeeds.

## Testing your autoscaler

The `testing` feature adds `scaleset::testing::Emulator`, an in-process,
stateful emulator of GitHub and the Actions service. It is not a mock that
replays canned responses. It keeps scale sets, sessions, runners and jobs,
computes statistics from them, redelivers messages until they are
acknowledged, and lets a test drive the job lifecycle or inject failures.

```toml
[dev-dependencies]
scaleset = { version = "0.1", features = ["testing"] }
```

```rust,no_run
use std::time::Duration;
use scaleset::RunnerScaleSetSpec;
use scaleset::testing::{Emulator, Endpoint, Fault, JobState};

#[tokio::test]
async fn my_autoscaler_runs_jobs() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(100))      // don't wait 50 s on idle polls
        .auto_run_jobs(Duration::from_millis(50))   // play the runners
        .start()
        .await;
    let client = emu.client();

    let spec = RunnerScaleSetSpec::new("linux");
    let id = client.create_runner_scale_set(&spec).await.unwrap().id;
    // ... start your autoscaler with `client` ...

    emu.inject(Fault::status(Endpoint::GetMessage, 503).times(2)); // a flaky service
    emu.queue_job(id, ["linux"]);

    emu.wait_until(Duration::from_secs(10), |s| {
        s.jobs.iter().all(|j| j.state == JobState::Completed) && s.runners.is_empty()
    })
    .await;
}
```

Tests can also expire the queue or admin token, check statistics, and
inspect every request served. The `scaleset-emulator` binary serves the same
emulator with a JSON control API, so clients in other languages, including
GitHub's Go client, can run against it:

```sh
cargo run --features testing --bin scaleset-emulator -- --port 4100 --long-poll-ms 2000
```

## Authentication

1. **GitHub App** (recommended): `Credentials::github_app(client_id,
   installation_id, pem)`.
2. **GitHub App with an external signer** (KMS/HSM): implement `Rs256Signer`
   and use `SignerJwtProvider`, or implement `JwtProvider` directly.
3. **Personal access token**: `Credentials::personal_access_token(token)`.

The client exchanges the credentials for a runner registration token and then
an Actions service admin token, and refreshes it before it expires. GitHub
Enterprise Server works the same way; use your GHES URL.
See [GitHub's docs](https://docs.github.com/en/actions/tutorials/use-actions-runner-controller/authenticate-to-the-api)
for the permissions needed.

On GHES, multiple labels per scale set need GHES 3.18 or later with the
`DistributedTask.AllowRunnerScaleSetCustomLabels` feature flag (on by default
from 3.21).

## HTTP and retries

Transport belongs to you; retries belong to the SDK.

```rust,no_run
use scaleset::{Client, Credentials, RetryPolicy, reqwest};

fn build() -> Result<Client, Box<dyn std::error::Error>> {
    let http = scaleset::http_client_builder() // SDK defaults, as a reqwest::ClientBuilder
        .tls_certs_merge([reqwest::Certificate::from_pem(&std::fs::read("ca.pem")?)?])
        .build()?;

    let credentials = Credentials::personal_access_token("ghp_...")?;
    Ok(Client::builder("https://ghes.example.com/org", credentials)
        .http_client(http)
        .retry_policy(RetryPolicy::default().max_retries(8))
        .build()?)
}
```

By default, transport errors, 5xx except 501, and 429 are retried four times
with jittered exponential backoff (1 s to 30 s); `Retry-After` is honored in
full. Message polls get a 2-minute window per attempt, whatever the client's
own timeout.

Rate-limited responses (any 429, or a 403 with `Retry-After`,
`X-RateLimit-Remaining: 0`, or a secondary-rate-limit message) come back as a
`RateLimitError`:

```rust,no_run
use std::time::Duration;
use scaleset::{Client, JitRunnerSetting, RateLimitPolicy, RateLimitRule, RetryPolicy, ScaleSetId};

async fn start_runner(client: &Client, id: ScaleSetId) {
    // For these calls only: give up at once on a spent quota, absorb short
    // secondary limits, and never spend more than 20 s per request.
    let quick = client.with_retry_policy(
        RetryPolicy::default()
            .max_elapsed(Duration::from_secs(20))
            .rate_limit(RateLimitPolicy::rules(
                RateLimitRule::give_up(),
                RateLimitRule::retries(2).max_wait(Duration::from_secs(5)),
            )),
    );

    match quick.generate_jit_config(id, &JitRunnerSetting::new("r1")).await {
        Ok(jit) => { /* start the runner */ }
        Err(e) => match e.rate_limit() {
            Some(rl) => {
                let requeue_after = rl.retry_after.unwrap_or(Duration::from_secs(60));
                // requeue the work
            }
            None => { /* other error */ }
        },
    }
}
```

## Security

- Prefer GitHub App credentials; rotate PATs if you must use them.
- Use `SignerJwtProvider` to keep private keys in a KMS or HSM.
- Treat JIT configs as secrets until a runner has used them. Tokens and JIT
  configs are redacted from `Debug` output.

## Requirements

Rust 1.85 or later.

## License

MIT. The protocol handling follows GitHub's MIT-licensed
[`actions/scaleset`](https://github.com/actions/scaleset).
