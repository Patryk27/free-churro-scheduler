#![feature(duration_constructors)]
#![feature(noop_waker)]

#[cfg(test)]
#[macro_use]
mod test_utils;

mod cmds;
mod database;
mod ext;
mod supervisor;
mod task;
mod worker;

use self::cmds::*;
use anyhow::Result;
use clap::Parser;
use indoc::indoc;
use std::env;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

/// How often to send a heartbeat message from each worker to the supervisor.
///
/// Affects how quickly supervisor is able to discover dead nodes within the
/// cluster.
const HEARTBEAT_DURATION: Duration = Duration::from_secs(1);

/// If a heartbeat doesn't arrive in this duration, supervisor will consider
/// the worker dead.
///
/// Must be larger than `HEARTBEAT_DURATION`.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);

const LOGO: &str = indoc! {r#"
        ____
       / __/_________
      / /_/ ___/ ___/
     / __/ /__(__  )
    /_/  \___/____/

"#};

#[derive(Debug, Parser)]
enum Cmd {
    Init(InitCmd),
    Supervise(SuperviseCmd),
    Work(WorkCmd),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = Cmd::parse();

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "fcs=info");
    }

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    for line in LOGO.lines() {
        info!("{}", line);
    }

    match cmd {
        Cmd::Init(cmd) => cmd.run().await,
        Cmd::Supervise(cmd) => cmd.run().await,
        Cmd::Work(cmd) => cmd.run().await,
    }
}
