#![feature(result_option_inspect)]
#![feature(let_chains)]

mod config;
mod constants;
mod controller;
mod fluid;
mod provisioner;
mod store;

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use std::{net::SocketAddr, sync::Arc};

use controller::{
    base::BaseController, database::DatabaseController, flow::FlowController,
    table::TableController,
};
use fluid::descriptor::{
    database::DatabaseDescriptor, flow::FlowDescriptor, table::TableDescriptor,
};

struct AppContext {
    db_controller: DatabaseController,
    table_controller: TableController,
    flow_controller: FlowController,
}

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder().finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let conf = config::init(constants::DEFAULT_CONF)
        .await
        .expect("failed to load configuration");

    let app_context = AppContext {
        db_controller: DatabaseController::new(&conf)
            .await
            .expect("could not construct database controller"),
        table_controller: TableController::new(&conf)
            .await
            .expect("could not construct table controller"),
        flow_controller: FlowController::new(&conf)
            .await
            .expect("cloud not construct flow controller"),
    };

    let app = Router::new()
        .route("/healthcheck", get(|| async { "1" }))
        .route("/api/v1/flow/validate", post(handle_flow_controller))
        .route("/api/v1/flow/reconcile", post(handle_flow_reconcile))
        .route("/api/v1/table/validate", post(handle_table_controller))
        .route("/api/v1/table/reconcile", post(handle_table_reconcile))
        .route("/api/v1/database/validate", post(handle_db_controller))
        .route("/api/v1/database/reconcile", post(handle_db_reconcile))
        .with_state(Arc::new(app_context));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn handle_db_controller(
    State(ctx): State<Arc<AppContext>>,
    Json(payload): Json<DatabaseDescriptor>,
) -> impl IntoResponse {
    let ctl = &ctx.db_controller;
    match ctl.validate(&payload).await {
        Err(t) => (StatusCode::BAD_REQUEST, format!("error: {}", t.to_string())),
        Ok(_) => (StatusCode::OK, String::from("")),
    }
}

async fn handle_db_reconcile(
    State(ctx): State<Arc<AppContext>>,
    Json(payload): Json<DatabaseDescriptor>,
) -> impl IntoResponse {
    let ctl = &ctx.db_controller;
    match ctl.reconcile(&payload).await {
        Err(t) => (StatusCode::INTERNAL_SERVER_ERROR, format!("error {:?}", t)),
        Ok(_) => (StatusCode::OK, String::from("yay!")),
    }
}

async fn handle_table_controller(
    State(ctx): State<Arc<AppContext>>,
    Json(payload): Json<TableDescriptor>,
) -> impl IntoResponse {
    let ctl = &ctx.table_controller;
    match ctl.validate(&payload).await {
        Err(t) => (StatusCode::BAD_REQUEST, format!("error: {}", t.to_string())),
        Ok(_) => (StatusCode::OK, String::from("")),
    }
}

async fn handle_table_reconcile(
    State(ctx): State<Arc<AppContext>>,
    Json(payload): Json<TableDescriptor>,
) -> impl IntoResponse {
    let ctl = &ctx.table_controller;
    match ctl.reconcile(&payload).await {
        Err(t) => (StatusCode::INTERNAL_SERVER_ERROR, format!("error {:?}", t)),
        Ok(_) => (StatusCode::OK, String::from("yay!")),
    }
}

async fn handle_flow_controller(
    State(ctx): State<Arc<AppContext>>,
    Json(payload): Json<FlowDescriptor>,
) -> impl IntoResponse {
    let ctl = &ctx.flow_controller;
    match ctl.validate(&payload).await {
        Err(t) => (StatusCode::BAD_REQUEST, format!("error: {}", t.to_string())),
        Ok(_) => (StatusCode::OK, String::from("")),
    }
}

async fn handle_flow_reconcile(
    State(ctx): State<Arc<AppContext>>,
    Json(payload): Json<FlowDescriptor>,
) -> impl IntoResponse {
    let ctl = &ctx.flow_controller;
    match ctl.reconcile(&payload).await {
        Err(t) => (StatusCode::INTERNAL_SERVER_ERROR, format!("error {:?}", t)),
        Ok(_) => (StatusCode::OK, String::from("yay!")),
    }
}
