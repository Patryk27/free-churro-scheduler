mod http;

use crate::worker::{Worker, WorkerId};
use anyhow::Result;
use clap::Parser;
use std::future::IntoFuture;
use std::net::SocketAddr;
use tokio::select;
use uuid::Uuid;

#[derive(Debug, Parser)]
pub struct WorkCmd {
    #[clap(short, long)]
    database: String,

    // TODO could be `Option<SocketAddr>` so that it's possible to spawn
    //      "headless" workers
    #[clap(short, long)]
    listen: SocketAddr,

    #[clap(short, long)]
    id: Uuid,
}

impl WorkCmd {
    pub async fn run(self) -> Result<()> {
        let worker =
            Worker::new(&self.database, WorkerId::new(self.id)).await?;

        let server =
            http::serve(self.listen, worker.database().clone()).await?;

        select! {
            result = worker.start() => result,
            result = server.into_future() => Ok(result?),
        }
    }
}
