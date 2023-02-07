use anyhow::Result;
use async_trait::async_trait;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::{error, info};

use crate::fluid::descriptor::IdentifiableDescriptor;

use super::error::ControllerReconciliationError;

#[async_trait]
pub(crate) trait BaseController<DescriptorKind: IdentifiableDescriptor + Sync + Send> {
    async fn validate(&self, descriptor: &DescriptorKind) -> Result<()>;
    async fn reconcile(&self, descriptor: &DescriptorKind) -> Result<()>;

    // TODO: probably just have a getter for the state store?
    async fn list_descriptors(&self) -> Result<Vec<DescriptorKind>>;

    async fn run(&self) {
        // TODO: ticker rate from config
        let mut ticker = interval(Duration::from_millis(5000));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            info!("running reconciliation");
            ticker.tick().await;

            // TODO: error handle and circuit break
            match self.reconcile_all().await {
                Ok(_) => info!("got ok from reconcile_all"),
                Err(e) => error!("got err from reconcile_all {:?}", e),
            }
        }
    }

    async fn reconcile_all(&self) -> Result<()> {
        let descriptors = self.list_descriptors().await?;

        for descriptor in descriptors {
            // TODO: update state
            // TODO: circuit break on descriptor id
            match self.reconcile(&descriptor).await {
                Ok(_) => (),
                Err(e) => {
                    match e.downcast_ref::<ControllerReconciliationError>() {
                        Some(ControllerReconciliationError::DependencyMissing(_)) => (),
                        Some(
                            ControllerReconciliationError::ProvisionerError(_)
                            | ControllerReconciliationError::ControllerError(_)
                        ) => (),
                        None => (),
                    }
                }
            }
        }

        Ok(())
    }
}
