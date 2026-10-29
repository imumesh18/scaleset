# scaleset

[![CI](https://shieldcn.dev/github/ci/imumesh18/scaleset.svg)](https://github.com/imumesh18/scaleset/actions/workflows/ci.yml)
[![crates.io](https://shieldcn.dev/crates/scaleset.svg?logo=rust)](https://crates.io/crates/scaleset)
[![downloads](https://shieldcn.dev/crates/d/scaleset.svg)](https://crates.io/crates/scaleset)
[![docs.rs](https://shieldcn.dev/badge/docs-docs.rs-blue.svg?logo=docsdotrs)](https://docs.rs/scaleset)
[![MSRV](https://shieldcn.dev/badge/MSRV-1.88-orange.svg?logo=rust)](#minimum-supported-rust-version)
[![license](https://shieldcn.dev/github/license/imumesh18/scaleset.svg)](LICENSE)
[![stars](https://shieldcn.dev/github/stars/imumesh18/scaleset.svg)](https://github.com/imumesh18/scaleset/stargazers)

Autoscale GitHub Actions self-hosted runners on any infrastructure. A Rust
client for the GitHub Actions **Runner Scale Set** APIs, the APIs
[actions-runner-controller](https://github.com/actions/actions-runner-controller)
is built on, without Kubernetes.

- **Complete**: scale sets, runner groups, just-in-time (JIT) runner
  configs and message sessions, on github.com, GHE.com and GitHub
  Enterprise Server.
- **Resilient**: a listener that acquires jobs, acknowledges messages,
  retries with backoff, waits out session conflicts after a restart, and
  replaces lost sessions. Token refresh and rate limits are handled for you.
- **Testable**: a stateful emulator of the whole service ships behind a
  feature flag, so an autoscaler can be tested end to end without a network.
- **Rust-first**: async on Tokio, `reqwest`, typed IDs, `tracing`, secrets in
  `SecretString`. It follows the protocol of GitHub's Go client,
  [`actions/scaleset`](https://github.com/actions/scaleset), not its API
  ([why](docs/DESIGN.md)).

```toml
[dependencies]
scaleset = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
```

Features:

- `rustls` (default): TLS through rustls.
- `native-tls`: TLS through the platform's library instead. Enable one of
  the two; without TLS the client cannot connect.
- `testing`: the emulator, for tests and the `scaleset-emulator` binary.

Status: 0.x, so the API may change between minor versions. GitHub does not
document the scale set APIs; this crate follows the behavior of
`actions/scaleset`.

## Quick start

```rust,no_run
use std::collections::HashSet;

use scaleset::listener::{Event, Listener, Scaler};
use scaleset::{
    BoxError, Client, Credentials, ExposeSecret, JitRunnerSetting, RunnerScaleSetSpec,
    ScaleSetId,
};

/// Keeps one runner for every job assigned to the scale set.
struct Autoscaler {
    client: Client,
    scale_set_id: ScaleSetId,
    /// Runners started and not finished yet.
    runners: HashSet<String>,
}

impl Scaler for Autoscaler {
    type Error = BoxError;

    async fn scale(&mut self, event: Event<'_>) -> Result<(), BoxError> {
        // Runners are ephemeral: each one leaves when its job completes.
        if let Some(message) = event.message() {
            for job in message.jobs_completed() {
                self.runners.remove(&job.runner_name);
            }
        }
        let wanted = event.statistics().total_assigned_jobs as usize;
        while self.runners.len() < wanted {
            let setting = JitRunnerSetting::unique("runner");
            let jit = self.client.generate_jit_config(self.scale_set_id, &setting).await?;
            // Start a runner (process, container, VM) with
            // ACTIONS_RUNNER_INPUT_JITCONFIG set to this value.
            let _config = jit.encoded_jit_config.expose_secret();
            self.runners.insert(setting.name);
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

    // The name is the label workflows target: `runs-on: my-scale-set`.
    let scale_set = client
        .create_runner_scale_set(&RunnerScaleSetSpec::new("my-scale-set"))
        .await?;

    let mut scaler = Autoscaler {
        client: client.clone(),
        scale_set_id: scale_set.id,
        runners: HashSet::new(),
    };
    Listener::new(client.clone(), scale_set.id, 10)
        .run(&mut scaler, async { let _ = tokio::signal::ctrl_c().await; })
        .await?;

    client.delete_runner_scale_set(scale_set.id).await?;
    Ok(())
}
```

[`examples/autoscaler.rs`](examples/autoscaler.rs) is a complete autoscaler
that starts runners through any shell command (for example `docker run`),
keeps a minimum of warm runners, and cleans up on Ctrl-C.

## How it works

A scale set is a group of ephemeral self-hosted runners. You register it
once; GitHub then routes matching jobs to it and tells you, through a
message session, how many runners it needs.

1. **Register** a scale set with a name (and optionally more labels) at a
   repository, organization or enterprise.
2. **Listen**: the listener long-polls the scale set's message queue,
   reporting your capacity (`max_runners`). Each poll returns a batch of job
   messages or, after about 50 seconds, nothing.
3. **Scale**: every event carries the current statistics. Keep
   `total_assigned_jobs` runners available, each started with a JIT config.
4. **Run**: GitHub hands each job to an idle runner. The runner runs one job
   and exits.

**Scale on statistics, never on message counts.** A batch holds at most 50
messages, and a job that no runner picks up in time is canceled and
reassigned up to three times, each time with new messages.
`total_assigned_jobs` is always current.

### The listener

`Listener::run` does the bookkeeping:

- opens the message session, and closes it on shutdown;
- acquires every available job, so none is left unassigned;
- acknowledges a message only after your scaler returns `Ok`; after an
  error the message is delivered again, so make scaling idempotent;
- backs off and retries on failures, and gives up after more than
  `max_failures` (default 10) failures in a row;
- waits on standby while another session is open, such as one left by a
  previous run that crashed;
- opens a new session if the old one disappears.

`scale` takes `&mut self` and is never called concurrently, so a scaler can
keep state without locks.

### Slow provisioning

The listener waits for `scale` before polling again. If a runner takes long
to start (a VM booting), reconcile elsewhere: a
`tokio::sync::watch::Sender<ScaleSetStatistics>` is itself a scaler that
publishes the latest statistics.

```rust,no_run
use scaleset::listener::Listener;
use scaleset::{Client, ScaleSetId, ScaleSetStatistics};

async fn run(client: Client, scale_set_id: ScaleSetId) -> Result<(), scaleset::listener::Error> {
    let (mut statistics, mut desired) = tokio::sync::watch::channel(ScaleSetStatistics::default());
    tokio::spawn(async move {
        while desired.changed().await.is_ok() {
            let wanted = desired.borrow_and_update().total_assigned_jobs;
            // reconcile toward `wanted`, however long it takes
        }
    });
    Listener::new(client, scale_set_id, 10)
        .run(&mut statistics, async { let _ = tokio::signal::ctrl_c().await; })
        .await
}
```

### Busy runners

Statistics say how many runners are needed, not which ones are working.
Before stopping a runner, ask a `JobTracker` fed with each message:

```rust,no_run
use scaleset::listener::{Event, JobTracker};

fn on_event(tracker: &mut JobTracker, event: Event<'_>) {
    if let Some(message) = event.message() {
        tracker.observe(message);
    }
    if !tracker.is_runner_busy("runner-7") {
        // safe to stop runner-7
    }
}
```

### Doing it by hand

`client.create_message_session(scale_set_id, owner)` returns a
`MessageSession`. Its `subscribe(max_runners)` returns a `Subscription`, the
pull API the listener is built on: `next()` yields events and `ack()`
acknowledges.
`MessageSession` exposes the raw calls: `get_message`, `delete_message` and
`acquire_jobs`.

## Authentication

| Credentials | When |
| --- | --- |
| `Credentials::github_app(client_id, installation_id, pem)` | GitHub App: recommended |
| `Credentials::github_app_with_provider(installation_id, SignerJwtProvider::new(client_id, signer))` | GitHub App whose key stays in a KMS or HSM: implement `Rs256Signer` |
| `Credentials::github_app_with_provider(installation_id, provider)` | any `JwtProvider`, such as a signing service |
| `Credentials::personal_access_token(token)` | PAT: simpler, but broadly scoped |

The client exchanges credentials for a registration token and then an
Actions service admin token, refreshes it before it expires, and replaces it
if the service rejects it early. See
[GitHub's docs](https://docs.github.com/en/actions/tutorials/use-actions-runner-controller/authenticate-to-the-api)
for the permissions needed.

For GitHub Enterprise Server, use your GHES URL. Multiple labels per scale
set need GHES 3.18 or later with the
`DistributedTask.AllowRunnerScaleSetCustomLabels` feature flag (on by
default from 3.21); without it, extra labels are dropped.

## HTTP, retries and rate limits

Transport belongs to you; retries belong to the SDK.

```rust,no_run
use scaleset::{Client, Credentials, RetryPolicy, reqwest};

fn build() -> Result<Client, Box<dyn std::error::Error>> {
    let ca = reqwest::Certificate::from_pem(&std::fs::read("corp-ca.pem")?)?;
    let http = scaleset::http_client_builder() // the SDK's defaults
        .tls_certs_merge([ca])
        .build()?;

    let credentials = Credentials::personal_access_token("ghp_...")?;
    Ok(Client::builder("https://ghes.example.com/my-org", credentials)
        .http_client(http)
        .retry_policy(RetryPolicy::default().max_retries(8))
        .build()?)
}
```

- Transport errors, 5xx other than 501, and 429 are retried four times with
  jittered exponential backoff (1 s up to 30 s). `Retry-After` on a 429 or
  503 is honored in full. TLS certificate errors are not retried.
- Each long-poll attempt gets two minutes, whatever the client's timeout.
- Rate-limited responses (429, or 403 with `Retry-After`,
  `X-RateLimit-Remaining: 0` or a secondary-limit message) come back as a
  `RateLimitError` with its kind and `retry_after`.
- `max_elapsed` caps the time spent on a request. Rather than sleeping into
  that deadline, the SDK returns the last response.
- Credentials only travel over HTTPS. For tests and isolated GHES labs,
  `ClientBuilder::allow_http(true)` permits plain HTTP; github.com and
  GHE.com always need HTTPS.

Change the policy for some calls only, for example to requeue on a rate
limit instead of blocking:

```rust,no_run
use std::time::Duration;
use scaleset::{Client, JitRunnerSetting, RateLimitPolicy, RateLimitRule, RetryPolicy, ScaleSetId};

/// Returns how long to wait before requeueing, if rate limited.
async fn start(client: &Client, id: ScaleSetId) -> Option<Duration> {
    let quick = client.with_retry_policy(
        RetryPolicy::default()
            .max_elapsed(Duration::from_secs(20))
            .rate_limit(RateLimitPolicy::rules(
                RateLimitRule::give_up(),                                   // spent quota
                RateLimitRule::retries(2).max_wait(Duration::from_secs(5)), // secondary limits
            )),
    );
    match quick.generate_jit_config(id, &JitRunnerSetting::new("r1")).await {
        Ok(_jit) => None,
        Err(e) => e.rate_limit().map(|rl| rl.retry_after.unwrap_or(Duration::from_secs(60))),
    }
}
```

## Testing

The `testing` feature adds `scaleset::testing::Emulator`: GitHub and the
Actions service, in your test process. It is stateful, not a mock: scale
sets, sessions, runners and jobs live in it, statistics are computed from
them, messages are redelivered until acknowledged, and tokens expire.

```toml
[dev-dependencies]
scaleset = { version = "0.1", features = ["testing"] }
```

```rust,no_run
use std::time::Duration;
use scaleset::RunnerScaleSetSpec;
use scaleset::testing::{Emulator, Endpoint, Fault, JobState};

#[tokio::test]
async fn autoscaler_runs_jobs() {
    let emu = Emulator::builder()
        .long_poll(Duration::from_millis(100))    // idle polls return quickly
        .auto_run_jobs(Duration::from_millis(50)) // play the runners
        .start()
        .await;
    let client = emu.client();
    let spec = RunnerScaleSetSpec::new("linux");
    let id = client.create_runner_scale_set(&spec).await.unwrap().id;

    // ... spawn your autoscaler with `client` ...

    emu.inject(Fault::status(Endpoint::GetMessage, 503).times(2)); // a flaky service
    emu.queue_job(id, ["linux"]);

    emu.wait_until(Duration::from_secs(10), |s| {
        s.jobs.iter().all(|j| j.state == JobState::Completed) && s.runners.is_empty()
    })
    .await;
}
```

Tests can also start or complete jobs by hand, expire tokens and sessions,
read statistics, and inspect every request. The `scaleset-emulator` binary
serves the same emulator, with a JSON control API, for clients in any
language:

```sh
cargo install scaleset --features testing --bin scaleset-emulator
scaleset-emulator --port 4100 --long-poll-ms 2000
```

## Security

- Prefer GitHub App credentials, and keep app keys in a KMS or HSM with
  `SignerJwtProvider` where you can.
- JIT configs and tokens are `SecretString`s: redacted from `Debug` output
  and wiped from memory on drop. Treat a JIT config as a secret until its
  runner has used it.
- Report vulnerabilities as described in [SECURITY.md](SECURITY.md).

## Minimum supported Rust version

Rust 1.88. Raising it is a minor-version change.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). [docs/DESIGN.md](docs/DESIGN.md)
explains how the crate is put together and why.

## License

[MIT](LICENSE). The protocol handling follows GitHub's MIT-licensed
[`actions/scaleset`](https://github.com/actions/scaleset).
