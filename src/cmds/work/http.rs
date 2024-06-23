mod endpoints;

use anyhow::Result;
use axum::routing::{get, post};
use axum::serve::Serve;
use axum::Router;
use sqlx::PgPool;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

pub async fn serve(
    addr: SocketAddr,
    database: PgPool,
) -> Result<Serve<Router, Router>> {
    use self::endpoints::*;

    info!("initializing http server");

    let listener = TcpListener::bind(addr).await?;

    let router = Router::new()
        .route(
            "/tasks",
            post(create_task::endpoint).get(get_tasks::endpoint),
        )
        .route(
            "/tasks/:id",
            get(get_task::endpoint).delete(delete_task::endpoint),
        )
        .with_state(database);

    info!(?addr, "ready");

    Ok(axum::serve(listener, router))
}
