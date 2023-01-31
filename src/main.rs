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
use store::{DescriptorStore, RedisDescriptorStore};

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
    descriptor_store: RedisDescriptorStore,
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
            .expect("could not construct flow controller"),

        descriptor_store: RedisDescriptorStore::new(conf.redis_url.clone())
            .await
            .expect("could not construct redis descriptor store"),
    };

    let app = Router::new()
        .route("/healthcheck", get(|| async { "1" }))
        .route("/api/v1/flow/reconcile", post(handle_flow_reconcile))
        .route("/api/v1/table/reconcile", post(handle_table_reconcile))
        .route("/api/v1/database/reconcile", post(handle_db_reconcile))
        .with_state(Arc::new(app_context));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn handle_db_reconcile(
    State(ctx): State<Arc<AppContext>>,
    Json(payload): Json<DatabaseDescriptor>,
) -> impl IntoResponse {
    let ctl = &ctx.db_controller;
    let descriptor_store = &ctx.descriptor_store;

    // FIXME: handle the anyhow errors using axum
    if let Err(e) = ctl.validate(&payload).await {
        return (StatusCode::BAD_REQUEST, format!("bad request: {:?}", e));
    }

    descriptor_store
        .store_descriptor::<DatabaseDescriptor>(&payload)
        .await;

    if let Err(e) = ctl.reconcile(&payload).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("error {:?}", e));
    }

    (StatusCode::ACCEPTED, "".to_string())
}

async fn handle_table_reconcile(
    State(ctx): State<Arc<AppContext>>,
    Json(payload): Json<TableDescriptor>,
) -> impl IntoResponse {
    let ctl = &ctx.table_controller;
    let descriptor_store = &ctx.descriptor_store;

    if let Err(e) = ctl.validate(&payload).await {
        return (StatusCode::BAD_REQUEST, format!("bad request: {:?}", e));
    }

    descriptor_store
        .store_descriptor::<TableDescriptor>(&payload)
        .await;

    if let Err(e) = ctl.reconcile(&payload).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("error {:?}", e));
    }

    (StatusCode::ACCEPTED, "".to_string())
}

async fn handle_flow_reconcile(
    State(ctx): State<Arc<AppContext>>,
    Json(payload): Json<FlowDescriptor>,
) -> impl IntoResponse {
    let ctl = &ctx.flow_controller;
    let descriptor_store = &ctx.descriptor_store;

    if let Err(e) = ctl.validate(&payload).await {
        return (StatusCode::BAD_REQUEST, format!("bad request: {:?}", e));
    }

    descriptor_store
        .store_descriptor::<FlowDescriptor>(&payload)
        .await;

    if let Err(e) = ctl.reconcile(&payload).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("error {:?}", e));
    }

    (StatusCode::ACCEPTED, "".to_string())
}
