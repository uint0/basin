use anyhow::Result;
use redis::AsyncCommands;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::Sync;

use crate::fluid::descriptor::IdentifiableDescriptor;

#[async_trait::async_trait]
pub(crate) trait DescriptorStore {
    async fn get_descriptor<T: DeserializeOwned>(&self, id: &str, kind: &str) -> Result<Option<T>>;
    async fn store_descriptor<T: IdentifiableDescriptor + Serialize + Sync>(
        &self,
        descriptor: &T,
    ) -> Result<()>;
    async fn list_descriptors<T: DeserializeOwned + Send>(&self, kind: &str) -> Result<Vec<T>>;
}

#[derive(Debug)]
pub struct RedisDescriptorStore {
    client: redis::Client,
}

#[async_trait::async_trait]
impl DescriptorStore for RedisDescriptorStore {
    async fn get_descriptor<T: DeserializeOwned>(&self, id: &str, kind: &str) -> Result<Option<T>> {
        let mut conn = self.client.get_tokio_connection().await?;

        let descriptor_json: Option<String> =
            conn.get(format!("descriptor/{}/{}", kind, id)).await?;

        Ok(if let Some(t) = descriptor_json {
            Some(serde_json::from_str(&t)?)
        } else {
            None
        })
    }

    async fn store_descriptor<T: IdentifiableDescriptor + Serialize + Sync>(
        &self,
        descriptor: &T,
    ) -> Result<()> {
        let mut conn = self.client.get_tokio_connection().await?;

        let descriptor_json: String = serde_json::to_string(descriptor)?;
        conn.set(
            format!("descriptor/{}/{}", descriptor.kind(), descriptor.id()),
            descriptor_json,
        )
        .await?;

        Ok(())
    }

    async fn list_descriptors<T: DeserializeOwned + Send>(&self, kind: &str) -> Result<Vec<T>> {
        let mut conn = self.client.get_tokio_connection().await?;

        // FIXME: keys is evil and we should probably not be using redis for this...
        let descriptor_keys: Vec<String> = conn.keys(format!("descriptor/{}/*", kind)).await?;

        let mut descriptors = Vec::new();
        for d in descriptor_keys {
            // TODO: theres a toctou here
            let descriptor: String = conn.get(&d).await?;
            descriptors.push(serde_json::from_str(&descriptor)?);
        }

        Ok(descriptors)
    }
}

impl RedisDescriptorStore {
    pub async fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;

        Ok(Self { client })
    }
}
