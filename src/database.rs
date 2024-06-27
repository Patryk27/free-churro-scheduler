use crate::task::{Task, TaskDef, TaskId, TaskStatus};
use crate::worker::WorkerId;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::types::Json;
use sqlx::{Executor, PgPool, Postgres, Row};
use std::time::Duration;
use tracing::{info, instrument, trace};
use uuid::Uuid;

pub struct Database;

impl Database {
    pub async fn connect(url: &str) -> Result<PgPool> {
        // TODO check if we're compatible with the database we're connecting to
        //      (e.g. compare migration version)

        info!("connecting to database");

        PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .context("couldn't connect to the database")
    }

    #[instrument(skip(db))]
    pub async fn create_worker(
        db: impl Executor<'_, Database = Postgres>,
        id: WorkerId,
        last_heard_at: DateTime<Utc>,
    ) -> Result<()> {
        trace!("running query");

        sqlx::query(
            "
            insert into workers (
                id,
                last_heard_at
            ) values (
                $1,
                $2
            ) on conflict (id) do update set
                last_heard_at = $2
            ",
        )
        .bind(id.get())
        .bind(last_heard_at)
        .execute(db)
        .await?;

        Ok(())
    }

    #[instrument(skip(db))]
    pub async fn update_worker(
        db: impl Executor<'_, Database = Postgres>,
        id: WorkerId,
        last_heard_at: DateTime<Utc>,
    ) -> Result<()> {
        trace!("running query");

        sqlx::query(
            "
            update workers
               set last_heard_at = $1
             where id = $2
            ",
        )
        .bind(last_heard_at)
        .bind(id.get())
        .execute(db)
        .await?;

        Ok(())
    }

    #[instrument(skip(db))]
    pub async fn create_task(
        db: impl Executor<'_, Database = Postgres>,
        def: TaskDef,
        created_at: DateTime<Utc>,
        scheduled_at: Option<DateTime<Utc>>,
    ) -> Result<TaskId> {
        trace!("running query");

        // TODO in theory it's possible we generate an id that's been already
        //      taken, so proooooobably it'd be nice to handle it somehow below
        //
        //      (it'll cause the query to throw "duplicate key violates ...")
        let id = TaskId::new(Uuid::new_v4());

        sqlx::query(
            "
            insert into tasks (
                id,
                def,
                worker_id,
                status,
                created_at,
                updated_at,
                scheduled_at
            ) values (
                $1,
                $2,
                null,
                'pending',
                $3,
                $3,
                $4
            )
            ",
        )
        .bind(id.get())
        .bind(Json(&def))
        .bind(created_at)
        .bind(scheduled_at)
        .execute(db)
        .await?;

        Ok(id)
    }

    #[instrument(skip(db))]
    pub async fn dispatch_task(
        db: impl Executor<'_, Database = Postgres>,
        task_id: TaskId,
        worker_id: WorkerId,
        updated_at: DateTime<Utc>,
    ) -> Result<bool> {
        trace!("running query");

        let row: Option<_> = sqlx::query(
            "
                update tasks
                   set worker_id = $1,
                       status = 'dispatched',
                       updated_at = $2
                 where id = $3
                   and status = 'pending'
             returning id
            ",
        )
        .bind(worker_id.get())
        .bind(updated_at)
        .bind(task_id.get())
        .fetch_optional(db)
        .await?;

        Ok(row.is_some())
    }

    #[instrument(skip(db))]
    pub async fn begin_task(
        db: impl Executor<'_, Database = Postgres>,
        id: TaskId,
        updated_at: DateTime<Utc>,
    ) -> Result<TaskDef> {
        trace!("running query");

        let row = sqlx::query(
            "
               update tasks
                  set status = 'running',
                      updated_at = $1
                where id = $2
                  and status = 'dispatched'
            returning def
            ",
        )
        .bind(updated_at)
        .bind(id.get())
        .fetch_one(db)
        .await?;

        let def: Json<_> = row.get(0);

        Ok(def.0)
    }

    #[instrument(skip(db))]
    pub async fn complete_task(
        db: impl Executor<'_, Database = Postgres>,
        id: TaskId,
        succeeded: bool,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        trace!("running query");

        let status = if succeeded { "succeeded" } else { "failed" };

        sqlx::query(
            "
            update tasks
               set status = $1::task_status,
                   updated_at = $2
             where id = $3
               and status in ('running', 'interrupted')
            ",
        )
        .bind(status)
        .bind(updated_at)
        .bind(id.get())
        .execute(db)
        .await?;

        Ok(())
    }

    #[instrument(skip(db))]
    pub async fn delete_task(
        db: impl Executor<'_, Database = Postgres>,
        id: TaskId,
    ) -> Result<()> {
        trace!("running query");

        // TODO consider soft-deletions
        sqlx::query(
            "
            delete from tasks
                  where id = $1
            ",
        )
        .bind(id.get())
        .execute(db)
        .await?;

        Ok(())
    }

    #[instrument(skip(db))]
    pub async fn find_task(
        db: impl Executor<'_, Database = Postgres>,
        id: TaskId,
    ) -> Result<Task> {
        Self::find_tasks(db, Some(id), None)
            .await?
            .into_iter()
            .next()
            .with_context(|| format!("couldn't find task {}", id.get()))
    }

    #[instrument(skip(db))]
    pub async fn find_tasks(
        db: impl Executor<'_, Database = Postgres>,
        id: Option<TaskId>,
        status: Option<TaskStatus>,
    ) -> Result<Vec<Task>> {
        trace!("running query");

        let tasks = sqlx::query(
            "
            select id,
                   def,
                   worker_id,
                   status,
                   created_at,
                   updated_at,
                   scheduled_at
              from tasks
             where ($1 is null or id = $1)
               and ($2 is null or status = $2)
            ",
        )
        .bind(id.map(|id| id.get()))
        .bind(status)
        .map(|row| Task {
            id: TaskId::new(row.get(0)),
            def: row.get::<Json<_>, _>(1).0,
            worker_id: row.get::<Option<_>, _>(2).map(WorkerId::new),
            status: row.get(3),
            created_at: row.get(4),
            updated_at: row.get(5),
            scheduled_at: row.get(6),
        })
        .fetch_all(db)
        .await?;

        Ok(tasks)
    }

    #[instrument(skip(db))]
    pub async fn get_backlog(
        db: impl Executor<'_, Database = Postgres>,
    ) -> Result<Vec<(TaskId, Option<DateTime<Utc>>)>> {
        trace!("running query");

        let rows = sqlx::query(
            "
            select id, scheduled_at
              from tasks
             where status = 'pending'
            ",
        )
        .map(|row: PgRow| (TaskId::new(row.get(0)), row.get(1)))
        .fetch_all(db)
        .await?;

        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::TaskStatus;
    use crate::test_utils::dt;
    use futures::future::BoxFuture;
    use sqlx::Transaction;
    use test_case::test_case;

    impl Database {
        pub async fn test() -> PgPool {
            Self::connect("postgres://127.0.0.1:5432/db")
                .await
                .expect("couldn't connect to the local database")
        }

        pub async fn with_test(
            f: impl for<'a> FnOnce(Transaction<'a, Postgres>) -> BoxFuture<'a, ()>,
        ) {
            let db = Self::test().await;
            let tx = db.begin().await.unwrap();

            // tx gets automatically rolled-back on drop within `f()`
            f(tx).await;
        }

        async fn get_worker_last_heard_at(
            db: impl Executor<'_, Database = Postgres>,
            id: WorkerId,
        ) -> DateTime<Utc> {
            sqlx::query("select last_heard_at from workers where id = $1")
                .bind(id.get())
                .fetch_one(db)
                .await
                .unwrap()
                .get(0)
        }

        async fn get_task_status(
            db: impl Executor<'_, Database = Postgres>,
            id: TaskId,
        ) -> TaskStatus {
            sqlx::query("select status from tasks where id = $1")
                .bind(id.get())
                .fetch_one(db)
                .await
                .unwrap()
                .get(0)
        }
    }

    #[tokio::test]
    async fn create_and_update_worker() {
        Database::with_test(|mut tx| {
            Box::pin(async move {
                let id = WorkerId::from(1234);
                let expected = dt("2018-01-01 12:00:00");

                // ---

                Database::create_worker(&mut *tx, id, expected)
                    .await
                    .unwrap();

                let actual =
                    Database::get_worker_last_heard_at(&mut *tx, id).await;

                assert_eq!(actual, expected);

                // ---

                let expected = dt("2018-01-01 12:30:00");

                Database::create_worker(&mut *tx, id, expected)
                    .await
                    .unwrap();

                let actual =
                    Database::get_worker_last_heard_at(&mut *tx, id).await;

                assert_eq!(actual, expected);

                // ---

                let expected = dt("2018-01-01 13:00:00");

                Database::update_worker(&mut *tx, id, expected)
                    .await
                    .unwrap();

                let actual =
                    Database::get_worker_last_heard_at(&mut *tx, id).await;

                assert_eq!(actual, expected);
            })
        })
        .await;
    }

    #[tokio::test]
    async fn create_task() {
        Database::with_test(|mut tx| {
            Box::pin(async move {
                let def = TaskDef::Bar;
                let created_at = dt("2018-01-01 12:00:00");
                let scheduled_at = Some(dt("2018-01-02 10:00:00"));

                let id = Database::create_task(
                    &mut *tx,
                    def,
                    created_at,
                    scheduled_at,
                )
                .await
                .unwrap();

                let actual = Database::find_task(&mut *tx, id).await.unwrap();

                assert_eq!(def, actual.def);
                assert_eq!(None, actual.worker_id);
                assert_eq!(TaskStatus::Pending, actual.status);
                assert_eq!(created_at, actual.created_at);
                assert_eq!(created_at, actual.updated_at);
                assert_eq!(scheduled_at, actual.scheduled_at);
            })
        })
        .await;
    }

    #[test_case(true)]
    #[test_case(false)]
    #[tokio::test]
    async fn task_flow_simple(succeeded: bool) {
        Database::with_test(|mut tx| {
            Box::pin(async move {
                let now = dt("2018-01-01 12:00:00");
                let worker_id = WorkerId::from(1234);

                Database::create_worker(&mut *tx, worker_id, now)
                    .await
                    .unwrap();

                // ---

                let task_id =
                    Database::create_task(&mut *tx, TaskDef::Bar, now, None)
                        .await
                        .unwrap();

                // ---

                let got_dispatched =
                    Database::dispatch_task(&mut *tx, task_id, worker_id, now)
                        .await
                        .unwrap();

                let actual_status =
                    Database::get_task_status(&mut *tx, task_id).await;

                assert!(got_dispatched);
                assert_eq!(TaskStatus::Dispatched, actual_status);

                // ---

                let actual_def =
                    Database::begin_task(&mut *tx, task_id, now).await.unwrap();

                let actual_status =
                    Database::get_task_status(&mut *tx, task_id).await;

                assert_eq!(TaskDef::Bar, actual_def);
                assert_eq!(TaskStatus::Running, actual_status);

                // ---

                Database::complete_task(&mut *tx, task_id, succeeded, now)
                    .await
                    .unwrap();

                // ---

                let expected = if succeeded {
                    TaskStatus::Succeeded
                } else {
                    TaskStatus::Failed
                };

                let actual = Database::get_task_status(&mut *tx, task_id).await;

                assert_eq!(expected, actual);
            })
        })
        .await;
    }

    #[tokio::test]
    async fn test_double_dispatch() {
        Database::with_test(|mut tx| {
            Box::pin(async move {
                let now = dt("2018-01-01 12:00:00");
                let worker_id = WorkerId::from(1234);

                Database::create_worker(&mut *tx, worker_id, now)
                    .await
                    .unwrap();

                // ---

                let task_id =
                    Database::create_task(&mut *tx, TaskDef::Bar, now, None)
                        .await
                        .unwrap();

                // ---

                let got_dispatched =
                    Database::dispatch_task(&mut *tx, task_id, worker_id, now)
                        .await
                        .unwrap();

                assert!(got_dispatched);

                // ---

                let got_dispatched =
                    Database::dispatch_task(&mut *tx, task_id, worker_id, now)
                        .await
                        .unwrap();

                assert!(!got_dispatched);
            })
        })
        .await;
    }
}
