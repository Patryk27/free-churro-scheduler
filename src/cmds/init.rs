use crate::database::Database;
use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;

#[derive(Debug, Parser)]
pub struct InitCmd {
    #[clap(short, long)]
    database: String,
}

impl InitCmd {
    pub async fn run(self) -> Result<()> {
        let db = Database::connect(&self.database).await?;

        info!("setting-up the database");

        // TODO if any tables already exist, require `--force`

        sqlx::migrate!("db/migrations")
            .run(&db)
            .await
            .context("couldn't set-up the database")?;

        db.close().await;

        info!("done, uwu");

        Ok(())
    }
}
