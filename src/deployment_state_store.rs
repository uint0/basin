use anyhow::Result;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum DeploymentState {
    // In descriptor store but not yet processing
    Pending,
    // Currently being processed
    // TODO: consider if we need this, if deployment is fast shouldn't be nessecary
    Deploying,
    // Deployment has succeeded
    Succeeded,
    // Deployment has failed
    Failed,
    // Unknown state
    Unknown,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeploymentInfo {
    pub state: DeploymentState,
    pub description: Option<String>,
}

#[async_trait::async_trait]
pub(crate) trait DeploymentStateStore {
    async fn set_state(&self, id: &str, info: &DeploymentInfo) -> Result<()>;
    async fn get_state(&self, id: &str) -> Result<Option<DeploymentInfo>>;
}

#[derive(Debug)]
pub struct RedisDeploymentStateStore {
    client: redis::Client,
}

#[async_trait::async_trait]
impl DeploymentStateStore for RedisDeploymentStateStore {
    async fn set_state(&self, id: &str, info: &DeploymentInfo) -> Result<()> {
        let mut conn = self.client.get_tokio_connection().await?;
        conn.set(
            format!("deployment-state/{}", id),
            serde_json::to_string(info)?,
        )
        .await?;
        Ok(())
    }

    async fn get_state(&self, id: &str) -> Result<Option<DeploymentInfo>> {
        let mut conn = self.client.get_tokio_connection().await?;
        let deployment_info: Option<String> = conn.get(format!("deployment-state/{}", id)).await?;
        Ok(if let Some(t) = deployment_info {
            Some(serde_json::from_str(&t)?)
        } else {
            None
        })
    }
}

impl RedisDeploymentStateStore {
    pub async fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;

        Ok(Self { client })
    }
}
