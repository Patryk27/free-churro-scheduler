use crate::database::Database;
use crate::supervisor::SupervisorNotification;
use crate::task::{TaskDef, TaskId};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

pub async fn endpoint(
    State(db): State<PgPool>,
    Json(req): Json<Request>,
) -> impl IntoResponse {
    // TODO handle .unwrap()
    let tx = db.begin().await.unwrap();

    let id = Database::create_task(&db, req.def, Utc::now(), req.scheduled_at)
        .await
        .unwrap();

    SupervisorNotification::TaskCreated {
        id,
        scheduled_at: req.scheduled_at,
    }
    .send(&db)
    .await
    .unwrap();

    tx.commit().await.unwrap();

    Json(Response { id })
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    def: TaskDef,
    scheduled_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Response {
    id: TaskId,
}
