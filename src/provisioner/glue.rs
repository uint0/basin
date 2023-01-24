use anyhow::Result;
use std::option::Option;

use aws_config::SdkConfig;
use aws_sdk_glue::{
    error::{GetDatabaseError, GetDatabaseErrorKind},
    model::DatabaseInput,
    output::GetDatabaseOutput,
    Client,
};

#[derive(Debug)]
pub struct GlueProvisioner {
    glue_client: Client,
}

impl GlueProvisioner {
    pub fn new(aws_conf: &SdkConfig) -> Self {
        GlueProvisioner {
            glue_client: Client::new(aws_conf),
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn get_database(&self, database_name: &String) -> Result<Option<GetDatabaseOutput>> {
        let glue_resource = self
            .glue_client
            .get_database()
            .name(database_name)
            .send()
            .await
            .map_err(|e| e.into_service_error());

        match glue_resource {
            Err(GetDatabaseError {
                kind: GetDatabaseErrorKind::EntityNotFoundException(_),
                ..
            }) => Ok(None),
            Ok(t) => Ok(Some(t)),
            Err(e) => Err(e.into()),
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn create_database(
        &self,
        name: &String,
        description: &String,
        location: &String,
    ) -> Result<()> {
        let db_input = Self::build_db_input(name, description, location);

        self.glue_client
            .create_database()
            .database_input(db_input)
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        self.glue_client
            .tag_resource()
            .resource_arn(self.arn_for_database(&name))
            // TODO: read from config
            .tags_to_add("provisioner", "basin")
            .tags_to_add("subporovisioner", "glue")
            .tags_to_add("basin_version", "0.0.1")
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn update_database(
        &self,
        name: &String,
        description: &String,
        location: &String,
    ) -> Result<()> {
        let db_input = Self::build_db_input(name, description, location);

        self.glue_client
            .update_database()
            .name(name)
            .database_input(db_input)
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(())
    }

    fn build_db_input(name: &String, description: &String, location: &String) -> DatabaseInput {
        DatabaseInput::builder()
            .name(name)
            .description(description)
            .location_uri(location)
            .build()
    }

    fn arn_for_database(&self, database_name: &String) -> String {
        // FIXME: un-hardcode these
        format!(
            "arn:aws:glue:{}:{}:database/{}",
            "us-east-1", "549989278514", database_name
        )
    }
}
