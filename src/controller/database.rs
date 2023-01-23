use super::base::BaseController;
use crate::fluid::descriptor::database::DatabaseDescriptor;
use crate::provisioner::s3::S3Provisioner;

use anyhow::{ensure, Result};
use aws_sdk_glue::{
    error::{GetDatabaseError, GetDatabaseErrorKind},
    model::DatabaseInput,
};
use regex::Regex;
use tokio::try_join;
use tracing::{debug, error, info};

const VALIDATION_REGEX_NAME: &str = r"^[a-z0-9_]+$";

#[derive(Debug)]
pub struct DatabaseController {
    glue_client: aws_sdk_glue::Client,
    s3_provisioner: S3Provisioner,
}

#[async_trait::async_trait]
impl BaseController<DatabaseDescriptor> for DatabaseController {
    async fn new() -> Result<Self> {
        // TODO: get this out of here
        let shared_config = aws_config::load_from_env().await;
        let glue_client = aws_sdk_glue::Client::new(&shared_config);

        Ok(DatabaseController {
            glue_client: glue_client,
            s3_provisioner: S3Provisioner::new(&shared_config),
        })
    }

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
        self.validate(&descriptor).await?;

        // TODO: error handle
        info!("Delegating resource reconciliation to clients");
        try_join!(
            self.reconcile_s3(&descriptor),
            self.reconcile_glue(&descriptor),
            self.reconcile_iam(),
        );
        info!("Finished resource reconciliation");

        Ok(())
    }
}

impl DatabaseController {
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
            // TODO: reconcile state :sigh:
            info!("found bucket");
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
        let glue_resource = self
            .glue_client
            .get_database()
            .name(&glue_name)
            .send()
            .await
            .map_err(|e| e.into_service_error());

        // FIXME: probably transact these - kinda pain tho :sigh:
        info!("Evaluating remote resource state");
        match glue_resource {
            Err(GetDatabaseError {
                kind: GetDatabaseErrorKind::EntityNotFoundException(_),
                ..
            }) => {
                info!("glue database does not exist, provisioning a new one");

                let db_input = DatabaseInput::builder()
                    .name(&glue_name)
                    .description(&descriptor.summary)
                    .location_uri(format!("s3://{}", Self::s3_name_for(&descriptor)))
                    .parameters("provisioner", "basin") // TODO: factor this out
                    .build();
                // TODO: log error probs
                self.glue_client
                    .create_database()
                    .database_input(db_input)
                    .send()
                    .await
                    .map_err(|e| e.into_service_error())?;
                Ok(())
            }
            Err(e) => {
                error!(?e, "got unexpected error when creating glue database");
                Err(e.into())
            }
            Ok(t) => {
                // TODO: reconcile state :sigh:
                info!("glue happy: {:?}", t);
                Ok(())
            }
        }
    }

    async fn reconcile_iam(&self) -> Result<()> {
        Ok(())
    }

    fn glue_name_for(descriptor: &DatabaseDescriptor) -> String {
        format!("zone_{}", descriptor.name)
    }

    fn s3_name_for(descriptor: &DatabaseDescriptor) -> String {
        format!("cz-vaporeon-db-{}", descriptor.name.replace("_", "-"))
    }
}
