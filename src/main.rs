#![feature(result_option_inspect)]

mod config;
mod constants;
mod controller;
mod fluid;
mod provisioner;
mod store;

use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use std::net::SocketAddr;

use controller::{base::BaseController, database::DatabaseController, table::TableController};
use fluid::descriptor::{database::DatabaseDescriptor, table::TableDescriptor};

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder().finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // TODO: something with this
    let _conf = config::init(constants::DEFAULT_CONF);

    let app = Router::new()
        .route("/healthcheck", get(|| async { "1" }))
        .route("/test/table/validate", post(test_table_controller))
        .route("/test/table/reconcile", post(test_table_reconcile))
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
        Err(t) => (StatusCode::BAD_REQUEST, format!("error: {}", t.to_string())),
        Ok(_) => (StatusCode::OK, String::from("")),
    }
}

async fn test_db_reconcile(Json(payload): Json<DatabaseDescriptor>) -> impl IntoResponse {
    let ctl = DatabaseController::new().await.expect("wtf");
    match ctl.reconcile(&payload).await {
        Err(t) => (StatusCode::INTERNAL_SERVER_ERROR, format!("error {:?}", t)),
        Ok(_) => (StatusCode::OK, String::from("yay!")),
    }
}

async fn test_table_controller(Json(payload): Json<TableDescriptor>) -> impl IntoResponse {
    let ctl = TableController::new().await.expect("wtf");
    match ctl.validate(&payload).await {
        Err(t) => (StatusCode::BAD_REQUEST, format!("error: {}", t.to_string())),
        Ok(_) => (StatusCode::OK, String::from("")),
    }
}

async fn test_table_reconcile(Json(payload): Json<TableDescriptor>) -> impl IntoResponse {
    let ctl = TableController::new().await.expect("wtf");
    match ctl.reconcile(&payload).await {
        Err(t) => (StatusCode::INTERNAL_SERVER_ERROR, format!("error {:?}", t)),
        Ok(_) => (StatusCode::OK, String::from("yay!")),
    }
}
