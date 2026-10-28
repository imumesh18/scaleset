//! An autoscaler that starts each runner as a local process.
//!
//! It registers a scale set, listens for jobs, and keeps
//! `min(MAX_RUNNERS, MIN_RUNNERS + assigned jobs)` runners alive by running
//! `RUNNER_COMMAND` through `sh -c`, with the runner's JIT config in
//! `ACTIONS_RUNNER_INPUT_JITCONFIG`. Runners are ephemeral: each runs one
//! job and exits. On Ctrl-C it stops its runners, closes the session and
//! deletes the scale set.
//!
//! ```sh
//! export SCALESET_URL=https://github.com/my-org/my-repo
//! export SCALESET_NAME=my-scale-set
//! export GITHUB_TOKEN=ghp_...   # or GITHUB_APP_CLIENT_ID, _INSTALLATION_ID, _PRIVATE_KEY_PATH
//! export RUNNER_COMMAND='docker run --rm -e ACTIONS_RUNNER_INPUT_JITCONFIG ghcr.io/actions/actions-runner:latest /home/runner/run.sh'
//! cargo run --example autoscaler
//! ```
//!
//! Optional: `MIN_RUNNERS` (default 0), `MAX_RUNNERS` (default 5),
//! `RUNNER_GROUP` (default `default`).
//!
//! Try it without GitHub against the emulator:
//!
//! ```sh
//! cargo run --features testing --bin scaleset-emulator -- --port 4100 --long-poll-ms 2000 --auto-run-ms 3000 &
//! SCALESET_URL=http://127.0.0.1:4100/org SCALESET_NAME=demo GITHUB_TOKEN=ghp_emulator \
//!   RUNNER_COMMAND='sleep 5' cargo run --example autoscaler
//! curl -X POST localhost:4100/_emulator/jobs -d '{"scaleSetId": 1001}'
//! ```

use std::collections::HashMap;
use std::env;

use anyhow::{Context, Result, bail};
use scaleset::listener::{Event, Listener, Scaler};
use scaleset::{
    Client, Credentials, DEFAULT_RUNNER_GROUP, ExposeSecret, JitRunnerSetting, RunnerGroupId,
    RunnerScaleSetSpec, ScaleSetId, SystemInfo,
};
use tokio::process::{Child, Command};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Config::from_env()?;
    let client = Client::builder(&config.url, config.credentials()?)
        .system_info(SystemInfo::new("scaleset-example").subsystem("autoscaler"))
        .build()?;

    let runner_group_id = match config.runner_group.as_str() {
        DEFAULT_RUNNER_GROUP => RunnerGroupId::DEFAULT,
        name => {
            client
                .get_runner_group_by_name(name)
                .await?
                .with_context(|| format!("runner group {name:?} not found"))?
                .id
        }
    };
    let scale_set = client
        .create_runner_scale_set(
            &RunnerScaleSetSpec::new(&config.name).runner_group_id(runner_group_id),
        )
        .await?;
    tracing::info!(id = %scale_set.id, name = %scale_set.name, "scale set created");

    let mut scaler = ProcessScaler {
        client: client.clone(),
        scale_set_id: scale_set.id,
        min_runners: config.min_runners,
        max_runners: config.max_runners,
        command: config.runner_command.clone(),
        runners: HashMap::new(),
    };
    let result = Listener::new(client.clone(), scale_set.id)
        .max_runners(config.max_runners)
        .run(&mut scaler, async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await;

    // Dropping the scaler kills the runner processes.
    drop(scaler);
    if let Err(e) = client.delete_runner_scale_set(scale_set.id).await {
        tracing::warn!(error = %e, "failed to delete the scale set");
    }
    Ok(result?)
}

/// Keeps the desired number of runner processes alive.
struct ProcessScaler {
    client: Client,
    scale_set_id: ScaleSetId,
    min_runners: u32,
    max_runners: u32,
    command: String,
    runners: HashMap<String, Child>,
}

impl Scaler for ProcessScaler {
    type Error = anyhow::Error;

    async fn scale(&mut self, event: Event<'_>) -> Result<()> {
        self.reap();
        let wanted = self
            .max_runners
            .min(self.min_runners + event.statistics().total_assigned_jobs);
        let wanted = usize::try_from(wanted)?;
        if self.runners.len() < wanted {
            tracing::info!(current = self.runners.len(), wanted, "scaling up");
        }
        while self.runners.len() < wanted {
            self.start_runner().await?;
        }
        // Runners exit after their job, so there is nothing to scale down.
        Ok(())
    }
}

impl ProcessScaler {
    /// Forgets runners whose process has exited.
    fn reap(&mut self) {
        self.runners.retain(|name, child| match child.try_wait() {
            Ok(None) => true,
            Ok(Some(status)) => {
                tracing::info!(runner = %name, %status, "runner exited");
                false
            }
            Err(e) => {
                tracing::warn!(runner = %name, error = %e, "failed to check the runner");
                false
            }
        });
    }

    async fn start_runner(&mut self) -> Result<()> {
        let name = format!("runner-{:08x}", fastrand::u32(..));
        let jit = self
            .client
            .generate_jit_config(self.scale_set_id, &JitRunnerSetting::new(&name))
            .await?;
        let child = Command::new("sh")
            .arg("-c")
            .arg(&self.command)
            .env(
                "ACTIONS_RUNNER_INPUT_JITCONFIG",
                jit.encoded_jit_config.expose_secret(),
            )
            .kill_on_drop(true)
            .spawn()
            .context("failed to start the runner")?;
        tracing::info!(runner = %name, "runner started");
        self.runners.insert(name, child);
        Ok(())
    }
}

struct Config {
    url: String,
    name: String,
    runner_group: String,
    min_runners: u32,
    max_runners: u32,
    runner_command: String,
}

impl Config {
    fn from_env() -> Result<Self> {
        let config = Self {
            url: required("SCALESET_URL")?,
            name: required("SCALESET_NAME")?,
            runner_group: env::var("RUNNER_GROUP").unwrap_or_else(|_| DEFAULT_RUNNER_GROUP.into()),
            min_runners: number("MIN_RUNNERS", 0)?,
            max_runners: number("MAX_RUNNERS", 5)?,
            runner_command: required("RUNNER_COMMAND")?,
        };
        if config.min_runners > config.max_runners {
            bail!("MIN_RUNNERS must not exceed MAX_RUNNERS");
        }
        Ok(config)
    }

    fn credentials(&self) -> Result<Credentials> {
        if let Ok(client_id) = env::var("GITHUB_APP_CLIENT_ID") {
            let installation_id = required("GITHUB_APP_INSTALLATION_ID")?
                .parse()
                .context("GITHUB_APP_INSTALLATION_ID must be a number")?;
            let path = required("GITHUB_APP_PRIVATE_KEY_PATH")?;
            let pem = std::fs::read_to_string(&path).with_context(|| format!("reading {path}"))?;
            return Ok(Credentials::github_app(client_id, installation_id, &pem)?);
        }
        let token = required("GITHUB_TOKEN").context("or set the GITHUB_APP_* variables")?;
        Ok(Credentials::personal_access_token(token)?)
    }
}

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

fn number(name: &str, default: u32) -> Result<u32> {
    env::var(name).map_or(Ok(default), |v| {
        v.parse()
            .with_context(|| format!("{name} must be a number"))
    })
}
