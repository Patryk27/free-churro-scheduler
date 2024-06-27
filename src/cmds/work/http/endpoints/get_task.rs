use crate::database::Database;
use crate::task::TaskId;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Json;
use sqlx::PgPool;
use uuid::Uuid;

pub async fn endpoint(
    State(db): State<PgPool>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    // TODO .unwrap()
    let task = Database::find_task(&db, TaskId::new(id)).await.unwrap();

    Json(task)
}
