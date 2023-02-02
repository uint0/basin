#![feature(async_closure)]
#![feature(let_chains)]
#![feature(never_type)]
#![feature(result_option_inspect)]

mod config;
mod constants;
mod controller;
pub mod deployment_state_store;
mod descriptor_event_watcher;
mod descriptor_store;
mod fluid;
mod provisioner;

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use deployment_state_store::{
    DeploymentInfo, DeploymentState, DeploymentStateStore, RedisDeploymentStateStore,
};
use descriptor_event_watcher::DescriptorEventWatcher;
use descriptor_store::{DescriptorStore, RedisDescriptorStore};
use serde::Serialize;
use std::{net::SocketAddr, sync::Arc};
use tokio::task;

use controller::{
    base::BaseController, database::DatabaseController, flow::FlowController,
    table::TableController,
};
use fluid::descriptor::{
    database::DatabaseDescriptor, flow::FlowDescriptor, table::TableDescriptor,
    IdentifiableDescriptor,
};

struct AppContext {
    descriptor_store: RedisDescriptorStore,
    deployment_state_store: RedisDeploymentStateStore,
}

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder().finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let conf = config::init(constants::DEFAULT_CONF)
        .await
        .expect("failed to load configuration");

    let app_context = AppContext {
        descriptor_store: RedisDescriptorStore::new(&conf.redis_url)
            .await
            .expect("could not construct redis descriptor store"),
        deployment_state_store: RedisDeploymentStateStore::new(&conf.redis_url)
            .await
            .expect("could not construct redis deployment state store"),
    };

    let db_ctl = DatabaseController::new(&conf)
        .await
        .expect("could not construct database controller");
    let tbl_ctl = TableController::new(&conf)
        .await
        .expect("could not construct table controller");
    let flow_ctl = FlowController::new(&conf)
        .await
        .expect("could not construct flow controller");

    task::spawn(async move {
        db_ctl.run().await;
    });
    task::spawn(async move {
        tbl_ctl.run().await;
    });
    task::spawn(async move {
        flow_ctl.run().await;
    });

    let event_watcher = DescriptorEventWatcher::new(&conf)
        .await
        .expect("could not construct event watcher");
    task::spawn(async move {
        event_watcher.ingest_loop().await;
    });

    let app = Router::new()
        .route("/healthcheck", get(|| async { "1" }))
        .route(
            "/api/v1/database/reconcile",
            post(handle_resource_submit::<DatabaseDescriptor>),
        )
        .route(
            "/api/v1/flow/reconcile",
            post(handle_resource_submit::<FlowDescriptor>),
        )
        .route(
            "/api/v1/table/reconcile",
            post(handle_resource_submit::<TableDescriptor>),
        )
        .with_state(Arc::new(app_context));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn handle_resource_submit<DescriptorKind: IdentifiableDescriptor + Serialize + Sync>(
    State(ctx): State<Arc<AppContext>>,
    Json(payload): Json<DescriptorKind>,
) -> impl IntoResponse {
    let depstate_store = &ctx.deployment_state_store;
    let descriptor_store = &ctx.descriptor_store;

    if let Err(e) = descriptor_store
        .store_descriptor::<DescriptorKind>(&payload)
        .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to store descriptor: {:?}", e),
        );
    }

    if let Err(e) = depstate_store
        .set_state(
            &payload.id(),
            &DeploymentInfo {
                state: DeploymentState::Pending,
                description: None,
            },
        )
        .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to set deployment state: {:?}", e),
        );
    }

    (StatusCode::ACCEPTED, "".to_string())
}
