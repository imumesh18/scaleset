//! Serves the scale set emulator for clients in any language.
//!
//! ```sh
//! cargo run --features testing --bin scaleset-emulator -- --port 4100 --long-poll-ms 2000
//! ```
//!
//! Point a client at the printed `config_url`, authenticating with the token
//! given to `--pat` (default `ghp_emulator`). Drive the emulator through
//! `/_emulator`:
//!
//! | Request | Effect |
//! | --- | --- |
//! | `GET /_emulator/state` | current state |
//! | `POST /_emulator/jobs` `{"scaleSetId": 1001, "labels": ["linux"]}` | queue a job |
//! | `POST /_emulator/jobs/{id}/start` `{"runnerName": "r1"}` | start it on a runner |
//! | `POST /_emulator/jobs/{id}/complete` `{"result": "failed"}` | complete it |
//! | `POST /_emulator/faults` `{"endpoint": "getMessage", "response": {"kind": "status", "status": 503}, "times": 2}` | inject a fault |
//! | `DELETE /_emulator/faults` | clear faults |
//! | `POST /_emulator/expire/admin-tokens` | expire admin tokens |
//! | `POST /_emulator/expire/queue-token/{scaleSetId}` | expire a session's queue token |
//! | `POST /_emulator/expire/session/{scaleSetId}` | drop a session, as if it timed out |
//! | `POST /_emulator/reset` | clear everything |

use std::net::{IpAddr, SocketAddr};
use std::process::ExitCode;
use std::time::Duration;

use scaleset::testing::Emulator;

const USAGE: &str = "\
usage: scaleset-emulator [options]

  --host <addr>                 address to bind (default 127.0.0.1)
  --port <port>                 port to bind (default 4100; 0 picks one)
  --pat <token>                 personal access token to accept (default ghp_emulator)
  --app-installation-id <id>    accept GitHub App JWTs for this installation
  --long-poll-ms <ms>           how long idle polls are held (default 50000)
  --auto-run-ms <ms>            start assigned jobs on idle runners and finish them after <ms>
  --admin-token-ttl-secs <s>    admin token lifetime (default 3600)
  --runner-group <name>         add a runner group (repeatable)
";

fn main() -> ExitCode {
    match parse(std::env::args().skip(1)) {
        Ok(Some((builder, addr))) => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            runtime.block_on(async move {
                let emu = match builder.bind(addr).try_start().await {
                    Ok(emu) => emu,
                    Err(e) => {
                        eprintln!("error: cannot listen on {addr}: {e}");
                        return ExitCode::FAILURE;
                    }
                };
                println!(
                    "{}",
                    serde_json::json!({
                        "url": emu.uri(),
                        "config_url": emu.config_url(),
                        "control_url": format!("{}/_emulator", emu.uri()),
                    })
                );
                let _ = tokio::signal::ctrl_c().await;
                ExitCode::SUCCESS
            })
        }
        Ok(None) => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}\n\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

fn parse(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<(scaleset::testing::EmulatorBuilder, SocketAddr)>, String> {
    let mut builder = Emulator::builder();
    let mut host: IpAddr = [127, 0, 0, 1].into();
    let mut port = 4100u16;
    while let Some(flag) = args.next() {
        if flag == "-h" || flag == "--help" {
            return Ok(None);
        }
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        let number = |v: &str| {
            v.parse::<u64>()
                .map_err(|_| format!("{flag}: not a number: {v}"))
        };
        match flag.as_str() {
            "--host" => {
                host = value
                    .parse()
                    .map_err(|_| format!("invalid host: {value}"))?
            }
            "--port" => {
                port = value
                    .parse()
                    .map_err(|_| format!("invalid port: {value}"))?
            }
            "--pat" => builder = builder.pat(value),
            "--app-installation-id" => {
                builder = builder.github_app(
                    value
                        .parse()
                        .map_err(|_| format!("invalid installation ID: {value}"))?,
                );
            }
            "--long-poll-ms" => builder = builder.long_poll(Duration::from_millis(number(&value)?)),
            "--auto-run-ms" => {
                builder = builder.auto_run_jobs(Duration::from_millis(number(&value)?))
            }
            "--admin-token-ttl-secs" => {
                builder = builder.admin_token_ttl(Duration::from_secs(number(&value)?));
            }
            "--runner-group" => builder = builder.runner_group(value),
            other => return Err(format!("unknown option {other}")),
        }
    }
    Ok(Some((builder, SocketAddr::new(host, port))))
}
