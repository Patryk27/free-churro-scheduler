use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use sqlx::postgres::PgListener;
use std::future::Future;
use tracing::debug;

pub trait PgListenerExt {
    fn try_recv_json<T>(&mut self) -> impl Future<Output = Result<T>>
    where
        T: DeserializeOwned;
}

impl PgListenerExt for PgListener {
    async fn try_recv_json<T>(&mut self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let notif = self
            .try_recv()
            .await
            .context("couldn't get next notification")?
            .context("lost connection to the database")?;

        let notif = notif.payload();

        debug!("got notification: {}", notif);

        serde_json::from_str(notif).with_context(|| {
            format!("couldn't deserialize notification: {}", notif)
        })
    }
}
