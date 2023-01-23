use super::base::BaseController;
use crate::fluid::descriptor::{database::DatabaseDescriptor};

use anyhow::{ensure, Result};
use aws_sdk_glue::error::{GetDatabaseError, GetDatabaseErrorKind};
use aws_sdk_s3::error::{HeadBucketErrorKind, HeadBucketError};
use regex::Regex;
use tracing::{info, debug, span, info_span};

const VALIDATION_REGEX_NAME: &str = r"^[a-z0-9_]+$";

#[derive(Debug)]
pub struct DatabaseController {
    glue_client: aws_sdk_glue::Client,
    s3_client: aws_sdk_s3::Client,
}

#[async_trait::async_trait]
impl BaseController<DatabaseDescriptor> for DatabaseController {
    async fn new() -> Result<Self> {
        // TODO: get this out of here
        let shared_config = aws_config::load_from_env().await;
        let glue_client = aws_sdk_glue::Client::new(&shared_config);
        let s3_client = aws_sdk_s3::Client::new(&shared_config);

        Ok(DatabaseController {
            glue_client: glue_client,
            s3_client: s3_client,
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
        info!("Performing reconciliation for descriptor");
        debug!("Full descriptor to be reconciled is {:?}", descriptor);
        self.validate(&descriptor).await?;

        info!(descriptor_id = descriptor.id, "Fetching remote resource state");
        let glue_name = glue_name_for(&descriptor);
        let s3_name = s3_name_for(&descriptor);

        debug!(glue_name, "Fetching glue resource");
        let glue_resource = self
            .glue_client
            .get_database()
            .name(glue_name)
            .send()
            .await
            .map_err(|e| e.into_service_error());
        
        debug!(s3_name, "Fetching s3 bucket");
        let s3_resource = self
            .s3_client
            .head_bucket()
            .bucket(s3_name)
            .send()
            .await
            .map_err(|e| e.into_service_error());

        // FIXME: probably transact these - kinda pain tho :sigh:
        info!("Evaluating remote resource state");
        match glue_resource {
            Err(GetDatabaseError { kind, .. }) => {
                if let GetDatabaseErrorKind::EntityNotFoundException(e) = kind {
                    println!("no such glue db {:?}", e);
                } else {
                    println!("some other glue error: {:?}", kind);
                    // FIXME: error handling stuff
                }
            }
            Ok(t) => {
                println!("glue happy: {:?}", t);
            }
        }

        match s3_resource {
            Err(HeadBucketError { kind, .. }) => {
                if let HeadBucketErrorKind::NotFound(e) = kind {
                    println!("no such s3 bucket {:?}", e);
                } else {
                    println!("some other s3 error {:?}", kind);
                }
            }
            Ok(t) => {
                println!("s3 happy: {:?}", t);
            }
        }

        Ok(())
    }
}

fn glue_name_for(descriptor: &DatabaseDescriptor) -> String {
    format!("zone_{}", descriptor.name)
}

fn s3_name_for(descriptor: &DatabaseDescriptor) -> String {
    format!("cz_vaporeon_zone_{}", descriptor.name)
}
