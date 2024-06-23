use crate::supervisor::Supervisor;
use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
pub struct SuperviseCmd {
    #[clap(short, long)]
    database: String,
}

impl SuperviseCmd {
    pub async fn run(self) -> Result<()> {
        Supervisor::new(&self.database).await?.start().await?;

        Ok(())
    }
}
