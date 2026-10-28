//! A small autoscaler that starts each runner as a local process.
//!
//! It registers a scale set, listens for jobs, and keeps
//! `min(MAX_RUNNERS, MIN_RUNNERS + assigned jobs)` runners alive by running
//! `RUNNER_COMMAND` through `sh -c` with the runner's JIT config in
//! `ACTIONS_RUNNER_INPUT_JITCONFIG`. Runners are ephemeral: each runs one job
//! and exits. On Ctrl-C it stops the runners, closes the session and deletes
//! the scale set.
//!
//! ```sh
//! export SCALESET_URL=https://github.com/my-org/my-repo
//! export SCALESET_NAME=my-scale-set
//! export GITHUB_TOKEN=ghp_...            # or the three GITHUB_APP_* variables
//! export RUNNER_COMMAND='docker run --rm -e ACTIONS_RUNNER_INPUT_JITCONFIG ghcr.io/actions/actions-runner:latest /home/runner/run.sh'
//! cargo run --example autoscaler
//! ```
//!
//! Optional: `MIN_RUNNERS` (default 0), `MAX_RUNNERS` (default 5),
//! `RUNNER_GROUP` (default "default"), `GITHUB_APP_CLIENT_ID`,
//! `GITHUB_APP_INSTALLATION_ID`, `GITHUB_APP_PRIVATE_KEY_PATH`.

use std::collections::HashMap;
use std::env;

use anyhow::{Context, Result, bail};
use scaleset::listener::{Event, Listener, Scaler};
use scaleset::{
    Client, Credentials, DEFAULT_RUNNER_GROUP, JitRunnerSetting, MessageSession, RunnerGroupId,
    RunnerScaleSetSpec, ScaleSetId, SystemInfo,
};
use tokio::process::{Child, Command};

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
        let var = |k: &str| env::var(k).with_context(|| format!("{k} is required"));
        let num = |k: &str, default: u32| -> Result<u32> {
            env::var(k).ok().map_or(Ok(default), |v| {
                v.parse().with_context(|| format!("{k} must be a number"))
            })
        };
        let config = Self {
            url: var("SCALESET_URL")?,
            name: var("SCALESET_NAME")?,
            runner_group: env::var("RUNNER_GROUP").unwrap_or_else(|_| DEFAULT_RUNNER_GROUP.into()),
            min_runners: num("MIN_RUNNERS", 0)?,
            max_runners: num("MAX_RUNNERS", 5)?,
            runner_command: var("RUNNER_COMMAND")?,
        };
        if config.min_runners > config.max_runners {
            bail!("MIN_RUNNERS must not exceed MAX_RUNNERS");
        }
        Ok(config)
    }

    fn credentials() -> Result<Credentials> {
        if let Ok(client_id) = env::var("GITHUB_APP_CLIENT_ID") {
            let installation_id = env::var("GITHUB_APP_INSTALLATION_ID")
                .context("GITHUB_APP_INSTALLATION_ID is required")?
                .parse()
                .context("GITHUB_APP_INSTALLATION_ID must be a number")?;
            let key_path = env::var("GITHUB_APP_PRIVATE_KEY_PATH")
                .context("GITHUB_APP_PRIVATE_KEY_PATH is required")?;
            let pem = std::fs::read_to_string(&key_path)
                .with_context(|| format!("reading {key_path}"))?;
            return Ok(Credentials::github_app(client_id, installation_id, &pem)?);
        }
        let token =
            env::var("GITHUB_TOKEN").context("set GITHUB_TOKEN or the GITHUB_APP_* variables")?;
        Ok(Credentials::personal_access_token(token)?)
    }
}

/// Keeps the desired number of runner processes alive.
struct ProcessScaler {
    client: Client,
    session: MessageSession,
    scale_set_id: ScaleSetId,
    min_runners: u32,
    max_runners: u32,
    command: String,
    runners: HashMap<String, Child>,
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
                tracing::warn!(runner = %name, error = %e, "failed to check runner");
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
            .env("ACTIONS_RUNNER_INPUT_JITCONFIG", &jit.encoded_jit_config)
            .kill_on_drop(true)
            .spawn()
            .context("failed to start runner")?;
        tracing::info!(runner = %name, "runner started");
        self.runners.insert(name, child);
        Ok(())
    }
}

impl Scaler for ProcessScaler {
    type Error = anyhow::Error;

    async fn scale(&mut self, event: Event<'_>) -> Result<()> {
        // Acquire first, so jobs are assigned as early as possible.
        // Acquiring twice is harmless if this message is redelivered.
        if let Event::Message(msg) = event {
            let jobs: Vec<_> = msg
                .jobs_available()
                .map(|j| j.job.runner_request_id)
                .collect();
            if !jobs.is_empty() {
                self.session.acquire_jobs(&jobs).await?;
            }
            for job in msg.jobs_completed() {
                tracing::info!(runner = %job.runner_name, result = %job.result, "job completed");
            }
        }

        self.reap();
        let Some(stats) = event.statistics() else {
            return Ok(());
        };
        let target = self
            .max_runners
            .min(self.min_runners + stats.total_assigned_jobs) as usize;
        if self.runners.len() < target {
            tracing::info!(current = self.runners.len(), target, "scaling up");
        }
        while self.runners.len() < target {
            self.start_runner().await?;
        }
        // Runners are ephemeral and exit after their job, so there is no
        // scale-down step.
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,scaleset=info".into()),
        )
        .init();

    let config = Config::from_env()?;
    let client = Client::builder(&config.url, Config::credentials()?)
        .system_info(
            SystemInfo::new("scaleset-rs-example")
                .version(env!("CARGO_PKG_VERSION"))
                .subsystem("autoscaler"),
        )
        .build()?;

    let runner_group_id = if config.runner_group == DEFAULT_RUNNER_GROUP {
        RunnerGroupId::DEFAULT
    } else {
        client
            .get_runner_group_by_name(&config.runner_group)
            .await?
            .with_context(|| format!("runner group {:?} not found", config.runner_group))?
            .id
    };

    let scale_set = client
        .create_runner_scale_set(
            &RunnerScaleSetSpec::new(&config.name).runner_group_id(runner_group_id),
        )
        .await?;
    tracing::info!(id = %scale_set.id, name = %scale_set.name, "scale set created");

    let mut info = client.system_info();
    info.scale_set_id = scale_set.id;
    client.set_system_info(info);

    let result = run(&client, &config, scale_set.id).await;

    if let Err(e) = client.delete_runner_scale_set(scale_set.id).await {
        tracing::warn!(error = %e, "failed to delete scale set");
    }
    result
}

async fn run(client: &Client, config: &Config, scale_set_id: ScaleSetId) -> Result<()> {
    let hostname = env::var("HOSTNAME").unwrap_or_else(|_| "scaleset-rs".into());
    let session = client
        .create_message_session(scale_set_id, hostname)
        .await?;

    let mut scaler = ProcessScaler {
        client: client.clone(),
        session: session.clone(),
        scale_set_id,
        min_runners: config.min_runners,
        max_runners: config.max_runners,
        command: config.runner_command.clone(),
        runners: HashMap::new(),
    };
    let listener = Listener::new(session.clone(), config.max_runners);
    let result = listener
        .run(&mut scaler, async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await;

    // Dropping the scaler kills the runner processes.
    drop(scaler);
    if let Err(e) = session.close().await {
        tracing::warn!(error = %e, "failed to close session");
    }
    result.map_err(Into::into)
}
