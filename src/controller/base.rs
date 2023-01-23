use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub(crate) trait BaseController<DescriptorKind> {
    async fn new() -> Result<Self>
    where
        Self: Sized;
    async fn validate(&self, descriptor: &DescriptorKind) -> Result<()>;
    async fn reconcile(&self, descriptor: &DescriptorKind) -> Result<()>;
}
