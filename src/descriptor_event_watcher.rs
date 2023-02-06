use std::time::Duration;

use anyhow::Result;
use aws_sdk_sqs::model::DeleteMessageBatchRequestEntry;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, error, info, warn};

use crate::{
    config::BasinConfig,
    deployment_state_store::{
        DeploymentInfo, DeploymentState, DeploymentStateStore, RedisDeploymentStateStore,
    },
    descriptor_store::{DescriptorStore, RedisDescriptorStore},
    fluid::descriptor::{
        database::DatabaseDescriptor, flow::FlowDescriptor, table::TableDescriptor,
        IdentifiableDescriptor,
    },
};

pub struct DescriptorEventWatcher {
    sqs_client: aws_sdk_sqs::Client,
    sqs_queue_url: String,
    descriptor_store: RedisDescriptorStore,
    deployment_state_store: RedisDeploymentStateStore,
    http_client: reqwest::Client,
}

#[derive(Deserialize, Debug)]
struct DescriptorEvent {
    r#type: String,
    #[serde(rename = "descriptorURI")]
    descriptor_uri: String,
    kind: String, // TODO: enum
    revision: u32,
}

#[derive(Deserialize, Debug)]
struct EnvelopedEvent {
    event_id: String,
    r#type: String,
    payload: DescriptorEvent,
    resource: Option<String>,
    time: Option<String>,
}

// TODO: s/Watcher/Reflector/g
impl DescriptorEventWatcher {
    pub async fn new(conf: &BasinConfig) -> Result<DescriptorEventWatcher> {
        Ok(DescriptorEventWatcher {
            sqs_client: aws_sdk_sqs::Client::new(&conf.aws_creds),
            sqs_queue_url: conf.event_sqs_url.clone(),
            descriptor_store: RedisDescriptorStore::new(&conf.redis_url).await?,
            deployment_state_store: RedisDeploymentStateStore::new(&conf.redis_url).await?,
            http_client: reqwest::Client::new(),
        })
    }

    pub async fn ingest_loop(&self) -> ! {
        let mut ticker = interval(Duration::from_millis(30000));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            info!("Ingesting events");
            ticker.tick().await;

            // TODO: circuit break
            if let Err(e) = self.ingest_set().await {
                error!("error when ingesting set {:?}", e);
            }
        }
    }

    async fn ingest_set(&self) -> Result<()> {
        let receive_output = self
            .sqs_client
            .receive_message()
            .queue_url(&self.sqs_queue_url)
            .visibility_timeout(10)
            .send()
            .await?;

        // NOTE: its safe to aggregate these and batch delete them at the end
        //       since in the worst case it the node is lost before deletion they'll just
        //       get picked up by another node. As the operation is idempotent it doesn't matter
        let mut deletions: Vec<(&str, String)> = Vec::new();

        if let Some(msgs) = receive_output.messages() {
            // TODO: run these concurrently
            for (i, msg) in msgs.iter().enumerate() {
                if let Some(receipt_handle) = msg.receipt_handle() {
                    info!(receipt_handle, "Read message sqs");

                    let msg_id = if let Some(x) = msg.message_id() {
                        x.to_string()
                    } else {
                        i.to_string()
                    };
                    deletions.push((receipt_handle, msg_id));
                }

                if let Some(event_str) = msg.body() {
                    let event: EnvelopedEvent = serde_json::from_str(event_str)?; // FIXME: handle all errors at the end
                    info!(
                        event_id = event.event_id,
                        "Received event from event source"
                    );

                    match event.payload.kind.as_str() {
                        "database" => {
                            self.load_upstream_descriptor::<DatabaseDescriptor>(
                                &event.payload.descriptor_uri,
                            )
                            .await?
                        }
                        "flow" => {
                            self.load_upstream_descriptor::<FlowDescriptor>(
                                &event.payload.descriptor_uri,
                            )
                            .await?
                        }
                        "table" => {
                            self.load_upstream_descriptor::<TableDescriptor>(
                                &event.payload.descriptor_uri,
                            )
                            .await?
                        }
                        k => {
                            warn!("Unsupported payload kind {}", k);
                            continue;
                        }
                    }
                }
            }
        }

        if !deletions.is_empty() {
            let mut delete_request = self
                .sqs_client
                .delete_message_batch()
                .queue_url(&self.sqs_queue_url);
            for (receipt_handle, msg_id) in deletions {
                delete_request = delete_request.entries(
                    DeleteMessageBatchRequestEntry::builder()
                        .id(msg_id)
                        .receipt_handle(receipt_handle)
                        .build(),
                )
            }
            delete_request.send().await?;
        }

        Ok(())
    }

    // TODO: probably include event_id in span if available
    async fn load_upstream_descriptor<
        DescriptorKind: IdentifiableDescriptor + Serialize + DeserializeOwned + Sync,
    >(
        &self,
        descriptor_uri: &str,
    ) -> Result<()> {
        // FIXME: handle ssrf
        debug!(descriptor_uri, "fetching descriptor from upstream");
        let resp = self.http_client.get(descriptor_uri).send().await?;

        // TODO: resp.error_for_status()?;
        let descriptor = match resp.json::<DescriptorKind>().await {
            Ok(t) => t,
            Err(e) => return Err(e.into()),
        };

        // TODO: check revision

        info!(
            descriptor_id = descriptor.id(),
            "received and storing descriptor"
        );
        self.descriptor_store
            .store_descriptor::<DescriptorKind>(&descriptor)
            .await?;

        self.deployment_state_store
            .set_state(
                &descriptor.id(),
                &DeploymentInfo {
                    state: DeploymentState::Pending,
                    description: None,
                },
            )
            .await?;

        info!(
            descriptor_id = descriptor.id(),
            "stored upstream descriptor into cache"
        );

        Ok(())
    }
}
