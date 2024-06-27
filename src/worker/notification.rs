use super::WorkerId;
use crate::task::TaskId;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{Executor, Postgres};
use tracing::{instrument, trace};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "ty")]
pub enum WorkerNotification {
    TaskDispatched { id: TaskId },
}

impl WorkerNotification {
    #[instrument(skip(db))]
    pub async fn send(
        self,
        db: impl Executor<'_, Database = Postgres>,
        id: WorkerId,
    ) -> Result<()> {
        trace!("sending notification");

        sqlx::query("select pg_notify($1, $2::text)")
            .bind(format!("worker:{}", id.get()))
            .bind(Json(self))
            .execute(db)
            .await?;

        Ok(())
    }
}
