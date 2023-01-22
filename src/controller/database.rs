use super::base::BaseController;
use crate::fluid::descriptor::{self, database::DatabaseDescriptor};

use anyhow::{ensure, Result};
use aws_sdk_glue::error::{GetDatabaseError, GetDatabaseErrorKind};
use aws_sdk_s3::error::{GetBucketLocationError, GetBucketLocationErrorKind, NoSuchBucket};
use regex::Regex;

const VALIDATION_REGEX_NAME: &str = r"^[a-z0-9_]+$";

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

    async fn reconcile(&self, descriptor: &DatabaseDescriptor) -> Result<()> {
        self.validate(&descriptor).await?;

        let glue_name = glue_name_for(&descriptor);
        let s3_name = s3_name_for(&descriptor);

        let glue_resource = self
            .glue_client
            .get_database()
            .name(glue_name)
            .send()
            .await
            .map_err(|e| e.into_service_error());
        let s3_resource = self
            .s3_client
            .get_bucket_location()
            .bucket(s3_name)
            .send()
            .await
            .map_err(|e| e.into_service_error());

        // FIXME: probably transact these - kinda pain tho :sigh:
        match glue_resource {
            Err(GetDatabaseError { kind, .. }) => {
                if let GetDatabaseErrorKind::EntityNotFoundException(e) = kind {
                    println!("no such glue db");
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
            Err(e) => println!("bad s3: {:?}", e),
            // Err(t) => println!("had error s3: {:?}", t),
            Ok(t) => println!("ok s3: {:?}", t),
        };

        Ok(())
    }
}

fn glue_name_for(descriptor: &DatabaseDescriptor) -> String {
    format!("zone_{}", descriptor.name)
}

fn s3_name_for(descriptor: &DatabaseDescriptor) -> String {
    format!("cz_vaporeon_zone_{}", descriptor.name)
}
