use super::base::BaseController;
use super::error::ControllerReconciliationError;
use crate::config::BasinConfig;
use crate::descriptor_store::{DescriptorStore, RedisDescriptorStore};
use crate::provisioner::s3::S3Provisioner;
use crate::{fluid::descriptor::database::DatabaseDescriptor, provisioner::glue::GlueProvisioner};

use anyhow::{ensure, Result};
use regex::Regex;
use tokio::{
    time::{sleep, Duration},
    try_join,
};

use tracing::{debug, error, info};

const VALIDATION_REGEX_NAME: &str = r"^[a-z0-9_]+$";

#[derive(Debug)]
pub struct DatabaseController {
    descriptor_store: RedisDescriptorStore,
    glue_provisioner: GlueProvisioner,
    s3_provisioner: S3Provisioner,
}

#[async_trait::async_trait]
impl BaseController<DatabaseDescriptor> for DatabaseController {
    async fn validate(&self, descriptor: &DatabaseDescriptor) -> Result<()> {
        ensure!(
            Regex::new(VALIDATION_REGEX_NAME)
                .unwrap()
                .is_match(&descriptor.name),
            format!(
                "Invalid name '{}'. Must match '{}'",
                descriptor.name, VALIDATION_REGEX_NAME
            )
        );

        Ok(())
    }

    #[tracing::instrument(level = "info", name = "db_reconcile", skip(self, descriptor), fields(descriptor_id = %descriptor.id))]
    async fn reconcile(&self, descriptor: &DatabaseDescriptor) -> Result<()> {
        info!("Performing reconciliation for database");
        debug!("Full descriptor to be reconciled is {:?}", descriptor);

        info!("Delegating resource reconciliation to clients");
        try_join!(
            self.reconcile_s3(&descriptor),
            self.reconcile_glue(&descriptor),
            self.reconcile_iam(),
        )
        .inspect_err(|e| error!(?e, "Resource reconciliation failed"))
        .map_err(|e| ControllerReconciliationError::ProvisionerError(e.into()))?;

        info!("Finished resource reconciliation");
        Ok(())
    }

    async fn list_descriptors(&self) -> Result<Vec<DatabaseDescriptor>> {
        Ok(self
            .descriptor_store
            .list_descriptors::<DatabaseDescriptor>("database")
            .await?)
    }
}

impl DatabaseController {
    pub async fn new(conf: &BasinConfig) -> Result<Self> {
        Ok(DatabaseController {
            descriptor_store: RedisDescriptorStore::new(&conf.redis_url).await?,
            glue_provisioner: GlueProvisioner::new(&conf.aws_creds),
            s3_provisioner: S3Provisioner::new(&conf.aws_creds),
        })
    }

    async fn reconcile_s3(&self, descriptor: &DatabaseDescriptor) -> Result<()> {
        let s3_name = Self::s3_name_for(&descriptor);
        info!("Reconciling s3 resource");

        debug!(s3_name, "Fetching s3 bucket");
        let bucket_exists = self
            .s3_provisioner
            .bucket_exists(&s3_name)
            .await
            .inspect_err(|e| error!(?e, "got unexpected error when looking up s3 bucket"))?;

        if bucket_exists {
            info!("found bucket in s3");
            self.s3_provisioner
                .update_bucket(&s3_name)
                .await
                .inspect_err(|e| error!(?e, "got unexpected error when updating s3 bucket"))?;
            info!("finished updating s3 bucket");
        } else {
            info!("s3 bucket does not exist. provisioning a new one");

            self.s3_provisioner
                .create_bucket(&s3_name)
                .await
                .inspect_err(|e| error!(?e, "got unexpected error when creating s3 bucket"))?;
        }

        Ok(())
    }

    async fn reconcile_glue(&self, descriptor: &DatabaseDescriptor) -> Result<()> {
        let glue_name = Self::glue_name_for(&descriptor);
        info!("Reconciling glue resource");

        debug!(glue_name, "Fetching glue resource");
        let glue_resource = self.glue_provisioner.get_database(&glue_name).await?;

        info!("Evaluating remote resource state");
        match glue_resource {
            Some(t) => {
                info!("found database in glue");
                debug!(?t, "glue resource");

                self.glue_provisioner
                    .update_database(
                        &glue_name,
                        &descriptor.summary,
                        &format!("s3://{}", Self::s3_name_for(&descriptor)),
                    )
                    .await
                    .inspect_err(|e| {
                        error!(?e, "got unexpected error when updating glue database")
                    })?;
                info!("finished updating glue database");
            }
            None => {
                info!("glue database does not exist, provisioning a new one");

                self.glue_provisioner
                    .create_database(
                        &glue_name,
                        &descriptor.summary,
                        &format!("s3://{}", Self::s3_name_for(&descriptor)),
                    )
                    .await
                    .inspect_err(|e| {
                        error!(?e, "got unexpected error when creating glue database")
                    })?;
            }
        }
        Ok(())
    }

    async fn reconcile_iam(&self) -> Result<()> {
        Ok(())
    }

    // TODO: dedupe between this and table(table_input) controller
    fn glue_name_for(descriptor: &DatabaseDescriptor) -> String {
        format!("zone_{}", descriptor.name)
    }

    fn s3_name_for(descriptor: &DatabaseDescriptor) -> String {
        format!("cz-vaporeon-db-{}", descriptor.name.replace("_", "-"))
    }
}
