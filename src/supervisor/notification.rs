use crate::task::TaskId;
use crate::worker::{WorkerId, WorkerStatus};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{Executor, Postgres};
use tracing::{instrument, trace};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "ty")]
pub enum SupervisorNotification {
    WorkerHeartbeat {
        id: WorkerId,

        // Because supervisor performs node discovery through the heartbeat
        // messages, we have to pass the worker's status in here as well -
        // otherwise a freshly-started supervisor wouldn't know whether the node
        // is busy or idling
        status: WorkerStatus,
    },

    WorkerIdle {
        id: WorkerId,
    },

    TaskCreated {
        id: TaskId,
        scheduled_at: Option<DateTime<Utc>>,
    },
}

impl SupervisorNotification {
    #[instrument(skip(db))]
    pub async fn send(
        self,
        db: impl Executor<'_, Database = Postgres>,
    ) -> Result<()> {
        trace!("sending notification");

        sqlx::query("select pg_notify('supervisor', $1::text)")
            .bind(Json(self))
            .execute(db)
            .await?;

        Ok(())
    }
}
