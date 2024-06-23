use super::SupervisorNotification;
use crate::ext::PgListenerExt;
use anyhow::{Context, Result};
use sqlx::postgres::PgListener;

#[derive(Debug)]
pub struct SupervisorListener {
    inner: PgListener,
}

impl SupervisorListener {
    pub async fn connect(database: &str) -> Result<Self> {
        let mut inner = PgListener::connect(database)
            .await
            .context("couldn't connect to the database")?;

        inner.listen("supervisor").await?;

        Ok(Self { inner })
    }

    pub async fn next(&mut self) -> Result<SupervisorNotification> {
        self.inner.try_recv_json().await
    }
}
