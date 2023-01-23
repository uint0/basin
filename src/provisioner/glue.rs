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

    pub async fn create_database(
        &self,
        name: &String,
        description: &String,
        location: &String,
    ) -> Result<()> {
        let db_input = DatabaseInput::builder()
            .name(name)
            .description(description)
            .location_uri(location)
            .build();

        self.glue_client
            .create_database()
            .database_input(db_input)
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(())
        // TODO: tagging
    }
}
