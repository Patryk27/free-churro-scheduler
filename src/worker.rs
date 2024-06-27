mod id;
mod listener;
mod notification;
mod status;
mod watchdog;

pub use self::id::*;
use self::listener::*;
pub use self::notification::*;
pub use self::status::*;
use self::watchdog::*;
use crate::database::Database;
use crate::supervisor::SupervisorNotification;
use crate::task::{TaskContext, TaskId};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::select;
use tokio::sync::oneshot;
use tracing::info;

#[derive(Debug)]
pub struct Worker {
    id: WorkerId,
    database: PgPool,
    listener: WorkerListener,
    status: Arc<AtomicWorkerStatus>,
    watchdog: oneshot::Receiver<WorkerWatchdogDied>,
}

impl Worker {
    pub async fn new(database: &str, id: WorkerId) -> Result<Self> {
        let listener = WorkerListener::connect(database, id).await?;
        let database = Database::connect(database).await?;
        let status = Arc::new(AtomicWorkerStatus::default());

        let watchdog = WorkerWatchdog::new(id, status.clone())
            .spawn(database.clone())
            .await?;

        Ok(Self {
            id,
            database,
            listener,
            status,
            watchdog,
        })
    }

    pub fn database(&self) -> &PgPool {
        &self.database
    }

    pub async fn start(mut self) -> Result<()> {
        loop {
            let reason = select! {
                notif = self.listener.next() => {
                    WakeupReason::GotNotification(notif)
                },
                _ = &mut self.watchdog => {
                    WakeupReason::WatchdogDied
                }
            };

            match reason {
                WakeupReason::GotNotification(notif) => match notif? {
                    WorkerNotification::TaskDispatched { id } => {
                        self.process_task(id).await.with_context(|| {
                            format!("couldn't process task {}", id.get())
                        })?;
                    }
                },

                WakeupReason::WatchdogDied => {
                    return Err(anyhow!("watchdog died, shutting down"));
                }
            }
        }
    }

    async fn process_task(&mut self, id: TaskId) -> Result<()> {
        self.status.store(WorkerStatus::Busy);

        info!(?id, "starting task");

        let task = Database::begin_task(&self.database, id, Utc::now()).await?;

        match task.run(&TaskContext { id }).await {
            Ok(_) => {
                info!(?id, "task succeeded");

                Database::complete_task(&self.database, id, true, Utc::now())
                    .await?;
            }

            Err(err) => {
                info!(?id, "task failed: {:?}", err);

                Database::complete_task(&self.database, id, false, Utc::now())
                    .await?;
            }
        }

        self.status.store(WorkerStatus::Idle);

        SupervisorNotification::WorkerIdle { id: self.id }
            .send(&self.database)
            .await?;

        Ok(())
    }
}

enum WakeupReason<A> {
    GotNotification(A),
    WatchdogDied,
}
