use super::{WorkerId, WorkerNotification};
use crate::ext::PgListenerExt;
use anyhow::{Context, Result};
use sqlx::postgres::PgListener;

#[derive(Debug)]
pub struct WorkerListener {
    inner: PgListener,
}

impl WorkerListener {
    pub async fn connect(database: &str, id: WorkerId) -> Result<Self> {
        let mut inner = PgListener::connect(database)
            .await
            .context("couldn't connect to the database")?;

        inner.listen(&format!("worker:{}", id.get())).await?;

        Ok(Self { inner })
    }

    pub async fn next(&mut self) -> Result<WorkerNotification> {
        self.inner.try_recv_json().await
    }
}
