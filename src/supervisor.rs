mod listener;
mod notification;
mod tasks;
mod workers;

use self::listener::*;
pub use self::notification::*;
use self::tasks::*;
use self::workers::*;
use crate::database::Database;
use crate::worker::WorkerNotification;
use anyhow::{Context, Result};
use chrono::Utc;
use sqlx::PgPool;
use std::time::Duration;
use tokio::time::Interval;
use tokio::{select, time};
use tracing::{debug, info};

#[derive(Debug)]
pub struct Supervisor {
    database: PgPool,
    listener: SupervisorListener,
    workers: SupervisedWorkers,
    tasks: PendingTasks,
    maintenance: Interval,
}

impl Supervisor {
    pub async fn new(database: &str) -> Result<Self> {
        let listener = SupervisorListener::connect(database).await?;

        // Note that connection acquisition order is important here -- not to
        // miss any tasks, we must first acquire connection to the listener and
        // only then connect to the database
        let database = Database::connect(database).await?;

        // We start assuming that our cluster is empty and then perform node
        // discovery *solely* through the heartbeat messages.
        //
        // Another approach could be e.g. based on us reading the `workers`
        // table and materializing cluster's status from there, but it feels a
        // bit more fragile, so let's not do that.
        let workers = SupervisedWorkers::default();

        Ok(Self {
            database,
            listener,
            workers,
            tasks: Default::default(),
            maintenance: time::interval(Duration::from_secs(1)),
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        self.process_backlog()
            .await
            .context("couldn't process backlog")?;

        info!("ready");

        loop {
            let reason = select! {
                notif = self.listener.next() => {
                    WakeupReason::GotNotification(notif)
                },
                id = &mut self.tasks => {
                    WakeupReason::GotTask(id)
                },
                _ = self.maintenance.tick() => {
                    WakeupReason::MaintenanceTime
                }
            };

            match reason {
                WakeupReason::GotNotification(notif) => match notif? {
                    SupervisorNotification::WorkerHeartbeat { id, status } => {
                        self.workers.add(id, status, Utc::now());
                        self.tasks.resume();
                    }

                    SupervisorNotification::WorkerIdle { id } => {
                        self.workers.mark_as_idle(id);
                        self.tasks.resume();
                    }

                    SupervisorNotification::TaskCreated {
                        id,
                        scheduled_at,
                    } => {
                        self.tasks.push(id, scheduled_at, Utc::now());
                    }
                },

                WakeupReason::GotTask(task_id) => {
                    let Some(worker_id) = self.workers.choose_idling() else {
                        self.tasks.push(task_id, None, Utc::now());
                        self.tasks.pause();
                        continue;
                    };

                    info!(?task_id, ?worker_id, "dispatching task");

                    let mut tx = self.database.begin().await?;

                    let task_got_dispatched = Database::dispatch_task(
                        &mut *tx,
                        task_id,
                        worker_id,
                        Utc::now(),
                    )
                    .await?;

                    if task_got_dispatched {
                        // TODO if the worker never acknowledges the task, it
                        //      will forever remain on the "dispatched" status -
                        //      we should detect this case and reassign the task

                        WorkerNotification::TaskDispatched { id: task_id }
                            .send(&mut *tx, worker_id)
                            .await?;

                        tx.commit().await?;
                    } else {
                        // If we hit this branch, then the task either:
                        //
                        // - must've gotten removed in the meantime,
                        // - must've already gotten assigned to someone.
                        //
                        // The first case is rather straightforward (since we
                        // even provide HTTP API for removing tasks) and the
                        // second case is the direct result of us having a (n
                        // unsolvable) race condition when the supervisor's
                        // starting.
                        //
                        // See, when we're starting, we first connect to the
                        // notification listener, then process all tasks from
                        // the "backlog" (i.e. we look for tasks that got
                        // created when the supervisor was offline) and then we
                        // start to process the notifications.
                        //
                        // With this design, it's possible for us to observe a
                        // single task twice - an example scenario could be:
                        //
                        // - T+0: we start the listener
                        // - T+1: someone creates a new task
                        // - T+2: we process backlog, find this task and process
                        //        it
                        // - T+3: we process notifications, find this task again
                        //        and try assigning it *again*
                        //
                        // In any case, this is not an error, so let's just roll
                        // back the transaction and try picking up a new task
                        // next iteration.

                        debug!(id = ?task_id, "suspicious: couldn't dispatch the task");

                        tx.rollback().await?;
                    }
                }

                WakeupReason::MaintenanceTime => {
                    self.workers.gc(Utc::now());
                }
            }
        }
    }

    /// Some tasks could got published when the supervisor was offline - this
    /// function looks for such tasks and prepares them for scheduling.
    async fn process_backlog(&mut self) -> Result<()> {
        let tasks = Database::get_backlog(&self.database).await?;

        for (id, scheduled_at) in tasks {
            info!(?id, "task was created while supervisor was shut down");

            self.tasks.push(id, scheduled_at, Utc::now());
        }

        Ok(())
    }
}

enum WakeupReason<A, B> {
    GotNotification(A),
    GotTask(B),
    MaintenanceTime,
}
