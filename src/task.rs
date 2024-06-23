use crate::worker::WorkerId;
use anyhow::Result;
use chrono::{DateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sqlx::prelude::Type;
use std::time::Duration;
use tokio::time;
use tracing::info;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize)]
pub struct Task {
    pub id: TaskId,
    pub def: TaskDef,
    pub worker_id: Option<WorkerId>,
    pub status: TaskStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub scheduled_at: Option<DateTime<Utc>>,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct TaskId(Uuid);

impl TaskId {
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    pub fn get(self) -> Uuid {
        self.0
    }
}

#[cfg(test)]
impl From<u128> for TaskId {
    fn from(id: u128) -> Self {
        Self::new(Uuid::from_u128(id))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "kebab-case")]
#[sqlx(type_name = "task_status", rename_all = "kebab-case")]
pub enum TaskStatus {
    /// Task is waiting for the supervisor to dispatch it.
    ///
    /// Next state: `Dispatched`
    Pending,

    /// Task has been assigned to a worker and it's waiting to get started.
    ///
    /// Next state: `Running` or `Pending`.
    Dispatched,

    /// Task has been started and it's currently running.
    ///
    /// Next state: `Succeeded` or `Failed` or `Interrupted`.
    Running,

    /// Task has been completed successfully.
    Succeeded,

    /// Task has been completed unsuccessfully.
    Failed,

    /// Worker has crashed/disappeared etc. during the task's execution.
    Interrupted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "ty")]
pub enum TaskDef {
    Foo,
    Bar,
    Baz,
}

impl TaskDef {
    pub async fn run(&self, ctxt: &TaskContext) -> Result<()> {
        match self {
            TaskDef::Foo => {
                time::sleep(Duration::from_secs(3)).await;

                info!("Foo {}", ctxt.id.get());
            }

            TaskDef::Bar => {
                let status =
                    reqwest::get("https://www.whattimeisitrightnow.com")
                        .await?
                        .status();

                info!("{}", status);
            }

            TaskDef::Baz => {
                let n = rand::thread_rng().gen_range(0..=343);

                info!("Baz {}", n);
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct TaskContext {
    pub id: TaskId,
}
