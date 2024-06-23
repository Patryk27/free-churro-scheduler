use crate::database::Database;
use crate::task::TaskStatus;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use sqlx::PgPool;

pub async fn endpoint(
    State(db): State<PgPool>,
    Query(params): Query<Params>,
) -> impl IntoResponse {
    // TODO .unwrap()
    let tasks = Database::find_tasks(&db, None, params.status)
        .await
        .unwrap();

    Json(tasks)
}

#[derive(Clone, Debug, Deserialize)]
pub struct Params {
    status: Option<TaskStatus>,
}
