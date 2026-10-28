# Design: `scaleset` for Rust

This crate is a Rust client for the GitHub Actions **Runner Scale Set** APIs.
It follows the wire contract and behavior of GitHub's Go client,
[`github.com/actions/scaleset`](https://github.com/actions/scaleset), but the
public API is designed for Rust: async/await on Tokio, `reqwest` for HTTP,
typed IDs, enums instead of sentinel values, `tracing` for logs, and traits
built for the borrow checker rather than for Go interfaces.

This document records what the Go client does, what we keep, and what we
deliberately do differently.

---

## 1. What the library has to do

A scale set client does four jobs.

1. **Authenticate.** Turn a PAT or GitHub App credential into an *Actions
   service admin token*. This is a three-step exchange:
   1. (GitHub App only) sign an RS256 JWT and `POST
      /app/installations/{id}/access_tokens` → installation token (201).
   2. `POST {api}/{orgs|repos|enterprises}/…/actions/runners/registration-token`
      with `Bearer <PAT or installation token>` → registration token (201).
   3. `POST {api}/actions/runner-registration` with `RemoteAuth <registration
      token>` and body `{"url": <config url>, "runner_event": "register"}` →
      `{ "url": <actions service url>, "token": <admin JWT> }`. A fresh
      registration token is sometimes not usable yet, so this request also
      retries 401 and 403.

   The admin JWT's `exp` claim (read without verification) decides when to
   refresh. It is refreshed 60 seconds before expiry, by at most one task at
   a time.

2. **Manage resources** on the Actions service (`{admin url}/_apis/…`,
   always with `api-version=6.0-preview`):

   | Operation | Request | Success |
   | --- | --- | --- |
   | Get scale set by name | `GET runtime/runnerscalesets?runnerGroupId=&name=` | 200, list (0 → none, >1 → error) |
   | List scale sets | `GET runtime/runnerscalesets?runnerGroupId=` | 200 |
   | Get scale set by id | `GET runtime/runnerscalesets/{id}` | 200 |
   | Create scale set | `POST runtime/runnerscalesets` | 200 |
   | Update scale set | `PATCH runtime/runnerscalesets/{id}` | 200 |
   | Delete scale set | `DELETE runtime/runnerscalesets/{id}` | 204 |
   | Runner group by name | `GET runtime/runnergroups/?groupName=` | 200, list |
   | JIT config | `POST runtime/runnerscalesets/{id}/generatejitconfig` | 200 |
   | Get runner | `GET distributedtask/pools/0/agents/{id}` | 200 |
   | Runner by name | `GET distributedtask/pools/0/agents?agentName=` | 200, list |
   | Remove runner | `DELETE distributedtask/pools/0/agents/{id}` | 204 |

   A scale set with no labels gets its name as its only label, and every
   label without a type gets type `System`.

3. **Run a message session.** A session is created per listener
   (`POST …/{id}/sessions`), refreshed (`PATCH …/sessions/{sid}`) when the
   message-queue token expires, and deleted (`DELETE …/sessions/{sid}`) on
   shutdown. Using the session:

   * `GET {messageQueueUrl}?lastMessageId=N` with
     `Authorization: Bearer <queue token>` and `X-ScaleSetMaxCapacity: N` is
     a long poll. The service holds it ~50 s; 202 means "nothing happened",
     200 carries a `RunnerScaleSetJobMessages` envelope whose `body` is a
     JSON *string* containing an array of job messages.
   * `DELETE {messageQueueUrl}/{messageId}` acknowledges a message (204).
   * `POST …/{id}/acquirejobs` with a JSON array of request IDs acquires
     jobs; it uses the admin URL but the *queue* token.
   * A 401 on any of these means the queue token expired. The client
     refreshes the session once (single-flight, skipped if another task
     already refreshed) and retries the call once.

4. **Retry well.** Transport errors, 5xx except 501, and 429 are retried with
   jittered exponential backoff. `Retry-After` on 429/503 is honored in full.
   Rate-limited responses (any 429, or a 403 with `Retry-After`,
   `X-RateLimit-Remaining: 0`, or a "secondary rate" message) are classified
   as *primary* or *secondary* and go through a separate, pluggable decision.
   An optional total budget per request (`max_elapsed`) stops retrying early
   and returns the last response rather than a bare timeout. Each long-poll
   attempt gets its own 2-minute window.

On top of that sits the **listener**: deliver the session's initial
statistics, then loop *poll → scale → acknowledge*. The scaling signal is
`statistics.totalAssignedJobs`, never a count of job messages.

---

## 2. Rust API shape

### Crate layout

```
scaleset
├── Client, ClientBuilder           client.rs
├── Credentials, JwtProvider, …     auth.rs
├── GitHubConfigUrl, Scope          config.rs
├── Error, ApiError, RateLimitError error.rs
├── RetryPolicy, RateLimitPolicy …  retry.rs
├── MessageSession                  session.rs
├── types (ids, scale sets, msgs)   types.rs
├── listener::{Listener, Scaler, Event, Subscription, JobTracker}
├── testing::Emulator               (feature `testing`)
└── (internal) transport.rs         the retry engine
```

One crate, not a workspace. The listener is a module, not a separate crate
as in Go, because it adds no dependencies; the emulator is a module behind a
feature so it can never drift from the client it imitates.

### Construction: a builder, not functional options

```rust
let client = Client::builder(
        "https://github.com/my-org",
        Credentials::github_app(client_id, installation_id, private_key_pem)?,
    )
    .system_info(SystemInfo::new("my-autoscaler").version("1.2.3"))
    .http_client(my_reqwest_client)        // optional
    .retry_policy(RetryPolicy::default())  // optional
    .build()?;
```

Go has three constructors × config structs × `HTTPOption`s. Rust has one
builder; the credential kind is a value (`Credentials`) rather than a choice
of constructor. Credentials are validated (and a PEM key parsed) when they are
created, so misconfiguration fails before any request is sent.

### `Client` is a cheap, cloneable handle

`Client` is `Arc`-backed and `Clone + Send + Sync`, like `reqwest::Client`.
All clones share the admin token cache, the HTTP connection pool and the
system info.

### Per-call retry policy without `context.Context`

Go threads per-request retry overrides through `context.Context`
(`WithRequestRetry`). Rust has no ambient context, so the equivalent is a
*view*:

```rust
let jit = client
    .with_retry_policy(RetryPolicy::default().rate_limit(RateLimitPolicy::rules(
        RateLimitRule::give_up(),                              // primary
        RateLimitRule::retries(2).max_wait(Duration::from_secs(5)), // secondary
    )))
    .generate_jit_config(scale_set_id, &JitRunnerSetting::new("runner-1"))
    .await?;
```

`with_retry_policy` returns a new `Client` sharing all state with the
original but using a different policy. A message session created from that
view keeps that policy, which replaces Go's "override options when creating
a session".

### Cancellation and deadlines

Rust futures are cancelled by dropping them, and a deadline is
`tokio::time::timeout`. There is no context to read a deadline from, so:

* `RetryPolicy::max_elapsed` is the deadline the retry engine knows about.
  When the next backoff would reach it, the engine stops and returns the last
  response (e.g. a `RateLimitError` with its `retry_after`) instead of
  sleeping into a timeout. If the budget runs out mid-attempt the call fails
  with `Error::MaxElapsedExceeded`.
* Wrapping a call in `tokio::time::timeout` works as usual; dropping the
  future cancels the in-flight request.

### Errors: one enum, with predicates

```rust
#[non_exhaustive]
pub enum Error {
    InvalidConfigUrl { .. }, InvalidCredentials(..), InvalidInput(..),
    Api(Box<ApiError>),              // non-success HTTP response
    RateLimited(Box<RateLimitError>),
    Transport { .. },                // reqwest error after retries
    MaxElapsedExceeded { .. },
    Decode { .. }, UnexpectedResponse(..), Jwt(..),
}
```

Go uses sentinel errors (`RunnerNotFoundError`, `NotFoundError`, …) matched
with `errors.Is`. Here, `ApiError` carries the status, the Actions exception
(`typeName`, `message`), `ActivityId` and `X-GitHub-Request-Id`, plus an
`ApiErrorKind` (`RunnerExists`, `RunnerNotFound`, `JobStillRunning`,
`MessageQueueTokenExpired`, `Other`). Predicates on `Error`
(`is_not_found()`, `is_runner_exists()`, `rate_limit()`, `status()`, …)
cover the common checks without nested matching.

`RateLimitError` keeps Go's fields with Rust types:
`kind: RateLimitKind`, `status`, `retry_after: Option<Duration>` (Go uses
`0` for "none"), `reset: Option<SystemTime>`, and `retries`.

### Retry policy

```rust
RetryPolicy::default()            // 4 retries, 1s..30s, no budget
    .max_retries(8)
    .max_elapsed(Duration::from_secs(20))
    .should_retry(|outcome| ...)  // replaces DefaultShouldRetry
    .backoff(|ctx| ...)           // replaces DefaultBackoff
    .rate_limit(RateLimitPolicy::...);
RetryPolicy::none()               // no retries
```

Rate limits are an enum instead of a nilable function field:

* `RateLimitPolicy::Default` — retry 429 (via backoff, honoring
  `Retry-After`), return a rate-limited 403 immediately.
* `RateLimitPolicy::rules(primary, secondary)` — Go's `RateLimitRetries`.
* `RateLimitPolicy::custom(|rl| RateLimitDecision::…)` — full control.

`RateLimitDecision` is `GiveUp | Retry | RetryAfter(Duration)` instead of
Go's `(time.Duration, bool)` pair.

### Transport belongs to the caller

The caller can pass any `reqwest::Client`. `scaleset::http_client_builder()`
returns a `reqwest::ClientBuilder` with the same defaults the Go SDK uses
(5-minute timeout, 30 s connect timeout, 90 s idle pool timeout, TLS ≥ 1.2),
so adding a root CA, a client identity (mTLS) or a proxy is one call on a
builder users already know.

Go needs a workaround to stop a short `http.Client.Timeout` from cutting off
the long poll (it copies the client and raises the timeout). In `reqwest`, a
per-request timeout overrides the client timeout, so each poll attempt simply
sets a 2-minute timeout on its own request.

### JWT providers

```rust
pub trait JwtProvider: Send + Sync + 'static {
    fn token(&self) -> impl Future<Output = Result<String, BoxError>> + Send;
}
pub trait Rs256Signer: Send + Sync + 'static {
    fn sign(&self, message: &[u8]) -> impl Future<Output = Result<Vec<u8>, BoxError>> + Send;
}
```

* `Credentials::github_app(client_id, installation_id, pem)` signs locally
  (PKCS#1 or PKCS#8 PEM) with `aws-lc-rs`, the crypto library `rustls`
  already uses.
* `SignerJwtProvider::new(client_id, signer)` assembles the JWT and asks an
  `Rs256Signer` for the signature, for KMS/HSM keys (Go's
  `SignerJWTProvider` with a `crypto.Signer`).
* Any `JwtProvider` implementation, or a closure through `jwt_provider_fn`.

The traits use native `async fn` in traits; the client stores them behind a
small internal object-safe wrapper, so users never need `async_trait` or
`Box::pin`.

### Data types

* **Typed IDs**: `ScaleSetId`, `RunnerGroupId`, `RunnerId`, `MessageId`,
  `JobRequestId` are `#[serde(transparent)]` newtypes over `i64`, so a runner
  ID can't be passed where a scale set ID is expected.
* **Request vs. response types**: Go uses `RunnerScaleSet` for both. Here
  `RunnerScaleSetSpec` (builder) is what you send and `RunnerScaleSet` is what
  you get back.
* **Messages keep their order**: Go splits a batch into four slices by type.
  Here `ScaleSetMessage::jobs` is a `Vec<JobMessage>` (an internally tagged
  enum on `messageType`) in delivery order, with `jobs_available()`,
  `jobs_started()`… iterators for the per-type view. Unknown message types
  become `JobMessage::Unknown` instead of being dropped silently.
* **Times** are `Option<DateTime<Utc>>`; Go's zero time
  (`0001-01-01T00:00:00Z`) maps to `None`.
* **Secrets** (`JitRunnerConfig::encoded_jit_config`, the session queue
  token, the PAT) are `secrecy::SecretString`: redacted in `Debug`, wiped on
  drop, never serialized, and read only through `expose_secret()`.
* **Counts** in statistics are `u32`; a negative or oversized value from the
  service is clamped rather than failing the whole message.

### Listener

Go's listener takes an open session, polls, calls `Scale`, and acknowledges;
any error stops it. Production autoscalers need more, so the listener owns
the whole loop:

```rust
pub trait Scaler: Send {
    type Error: Into<BoxError>;
    fn scale(&mut self, event: Event<'_>) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub enum Event<'a> {
    Initial { statistics: &'a ScaleSetStatistics },  // a session was opened
    Message { message: &'a ScaleSetMessage, statistics: &'a ScaleSetStatistics },
    Idle { statistics: &'a ScaleSetStatistics },     // the long poll returned nothing
}

Listener::new(client, scale_set_id)
    .max_runners(10)
    .run(&mut scaler, shutdown)
    .await?;
```

What it does, and how that differs from Go:

* **It opens and closes the session.** A 409 on open, the previous run's
  session still alive after a crash, is retried until that session
  expires. A session that disappears (404) is replaced, and the scaler sees
  `Event::Initial` again.
* **It acquires every available job before calling the scaler.** In Go each
  scaler must remember to; forgetting leaves jobs unassigned. Callers who
  want to choose use `Subscription`.
* **It recovers.** A failed step (poll, acquire, scale, acknowledge) backs off
  exponentially and is retried; the message is delivered again because it was
  not acknowledged. It gives up after `max_failures` failures in a row
  (default 10; 0 restores Go's stop-on-first-error).
* **Every event carries statistics.** Idle polls repeat the last known ones,
  so a scaler can keep converging without caching them itself. This replaces
  Go's `nil` message and its synthetic initial message with ID `-1`.
* **`scale` takes `&mut self`.** Go promises it is never called
  concurrently; the borrow checker enforces it, and scalers need no mutex.
* **Shutdown is a future** (`ctrl_c()`, `token.cancelled()`, …). It
  interrupts a poll, a backoff or `scale`, but not an acknowledgment, as Go
  does with `context.WithoutCancel`.
* **Slow provisioning is decoupled.** `watch::Sender<ScaleSetStatistics>`
  implements `Scaler`, so a separate task can reconcile at its own pace while
  the listener keeps polling and acknowledging (level-triggered, like a
  Kubernetes controller).
* **`JobTracker`** follows jobs by request ID through assignment, start and
  completion, to tell busy runners from idle ones (what ARC uses `JobStarted`
  for) without double counting reassigned jobs.
* **`Subscription`** is the pull API underneath: `next()` yields events,
  `ack()` acknowledges, and calling `next()` without `ack()` gives the
  message back. It borrows mutably, so a message can't be held across polls.
* Capacity can change while running through a shared `MaxRunners` handle.

### Logging

`tracing` replaces the injected `slog.Logger`: every HTTP request runs in a
`scaleset.request` span (method and path, never the query), retries log at
`debug`, token refreshes and session changes at `info`, listener failures at
`warn`. Applications choose a subscriber; the library never installs one.

### User agent

Same JSON user agent as Go (`system`, `version`, `commit_sha`,
`scale_set_id`, `subsystem`, `build_version`, `build_commit_sha`,
`kind: "scaleset"`), so the service sees the same shape from either SDK.
`build_version` is the crate version.

---

## 3. Deliberately left out

* `DebugInfo()` — it inspects Go's `*http.Transport`; a `reqwest::Client` is
  opaque, so there is nothing reliable to report.
* Go's deprecated transport options (`WithRetryMax`, `WithProxy`, …) and
  their migration path.
* Honoring a caller deadline when deciding whether to sleep before a retry:
  Rust has no ambient deadline. `max_elapsed` covers this case.
* Orphaned runner cleanup. The API has no documented way to list a scale
  set's runners, so cleanup needs your own inventory; `get_runner_by_name`
  and `remove_runner` do the rest.
* Typed TLS certificate errors. `reqwest` does not expose them, so the retry
  policy recognizes certificate failures by their message to avoid retrying
  them.

## 4. Dependencies

| Crate | Why |
| --- | --- |
| `reqwest` | HTTP; `rustls` (default) or `native-tls` feature |
| `tokio` | timers, sync primitives, `select!` |
| `serde`, `serde_json` | wire format |
| `aws-lc-rs` | RS256 signing for GitHub App keys |
| `base64` | JWT encoding, PEM decoding |
| `chrono` | timestamps |
| `uuid` | session IDs |
| `url` | URL parsing and joining |
| `thiserror` | error types |
| `tracing` | logs |
| `secrecy` | secrets that are redacted and zeroized |
| `httpdate`, `fastrand` | `Retry-After` dates, backoff jitter |
| `axum` (only with `testing`) | the emulator's HTTP server |

## 5. Testing

Two kinds of test, each with the tool that suits it:

* **Wire tests** (`tests/client.rs`, `tests/session.rs`) use `wiremock`.
  They pin exact bytes: headers, query strings, bodies, status sequences
  (401 then 200), `Retry-After`, delays and a byte order mark. These guard
  the protocol contract.
* **Behavior tests** (`tests/emulator.rs`, `tests/listener.rs`) use
  `scaleset::testing::Emulator`, a stateful emulator shipped behind the
  `testing` feature. They run whole scenarios: an autoscaler draining a
  queue, redelivery after a scaler error, token and session expiry, a stale
  session after a restart, busy runners, injected faults.

Unit tests cover URL parsing, rate-limit classification, `Retry-After`,
backoff, JWT signing and message decoding.

### Why our own emulator

[`vercel-labs/emulate`](https://github.com/vercel-labs/emulate) has the
right idea (stateful emulation, not mocks) but emulates GitHub's public
REST API only. It has none of the runner registration endpoints and none of
the Actions service APIs, which are most of this crate's surface, and would
add Node to `cargo test`. A Rust emulator in this crate:

* shares the crate's types, so it cannot drift from the client;
* ships with the crate, so users can test their own autoscalers;
* compiles only with the `testing` feature, so production builds carry no
  server dependency (`axum`);
* runs standalone (`scaleset-emulator`, with a JSON control API under
  `/_emulator`), so GitHub's Go client can run the same scenarios, which
  makes it a reference to compare both clients against.

The emulator models the protocol as this crate and the Go client understand
it. Where the real service is undocumented (exception names for conflicts,
when acquired jobs are assigned), it makes a documented choice; see the
`testing` module docs.

### What the emulator found

The first scenario run exposed a gap shared with the Go client: an admin
token rejected before its expiry (revoked, or clock skew) failed every call
until it expired. The client now drops the token and retries once with a
fresh one.
