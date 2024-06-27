use super::AtomicWorkerStatus;
use crate::database::Database;
use crate::supervisor::SupervisorNotification;
use crate::worker::WorkerId;
use crate::HEARTBEAT_DURATION;
use anyhow::Result;
use chrono::Utc;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::{task, time};
use tracing::{error, info};

#[derive(Debug)]
pub struct WorkerWatchdog {
    id: WorkerId,
    status: Arc<AtomicWorkerStatus>,
}

impl WorkerWatchdog {
    pub fn new(id: WorkerId, status: Arc<AtomicWorkerStatus>) -> Self {
        Self { id, status }
    }

    pub async fn spawn(
        self,
        db: PgPool,
    ) -> Result<oneshot::Receiver<WorkerWatchdogDied>> {
        let (tx, rx) = oneshot::channel();

        info!(id = ?self.id, "initializing watchdog");

        // TODO if the `workers` table already contains an entry for our `id`
        //      and its `last_heard_at` is within a couple of seconds of now,
        //      bail out
        //
        //      (most likely someone's launched a worker with the same id twice,
        //      which is ayy ayy dios mio)

        Database::create_worker(&db, self.id, Utc::now()).await?;

        let mut interval = {
            let mut interval = time::interval(HEARTBEAT_DURATION);

            interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            interval
        };

        task::spawn(async move {
            loop {
                if let Err(err) = self.heartbeat(&db).await {
                    error!("> {:?}", err);

                    _ = tx.send(WorkerWatchdogDied);
                    break;
                }

                interval.tick().await;
            }
        });

        Ok(rx)
    }

    async fn heartbeat(&self, db: &PgPool) -> Result<()> {
        Database::update_worker(db, self.id, Utc::now()).await?;

        SupervisorNotification::WorkerHeartbeat {
            id: self.id,
            status: self.status.load(),
        }
        .send(db)
        .await?;

        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct WorkerWatchdogDied;
