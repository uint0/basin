mod config;
mod constants;
mod controller;
mod fluid;

use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use std::net::SocketAddr;

use controller::{base::BaseController, database::DatabaseController};
use fluid::descriptor::database::DatabaseDescriptor;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let conf = config::init(constants::DEFAULT_CONF);

    let app = Router::new()
        .route("/healthcheck", get(|| async { "1" }))
        .route("/test/database/validate", post(test_db_controller))
        .route("/test/database/reconcile", post(test_db_reconcile));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn test_db_controller(Json(payload): Json<DatabaseDescriptor>) -> impl IntoResponse {
    let ctl = DatabaseController::new().await.expect("wtf");
    match ctl.validate(&payload).await {
        Err(t) => (StatusCode::BAD_REQUEST, format!("error {}", t.to_string())),
        Ok(_) => (StatusCode::OK, String::from("")),
    }
}

async fn test_db_reconcile(Json(payload): Json<DatabaseDescriptor>) -> impl IntoResponse {
    let ctl = DatabaseController::new().await.expect("wtf");
    ctl.reconcile(&payload).await;
    "lol k"
}
