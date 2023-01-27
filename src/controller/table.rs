use crate::{
    fluid::descriptor::{
        database::DatabaseDescriptor,
        table::{TableColumnType, TableDescriptor},
    },
    store::{DescriptorStore, RedisDescriptorStore},
};

use anyhow::{ensure, Result};
use aws_sdk_glue::{
    error::{GetTableError, GetTableErrorKind},
    model::{Column, StorageDescriptor, TableInput},
};
use regex::Regex;
use tracing::{debug, error, info};

use super::base::BaseController;

const VALIDATION_REGEX_TABLE_NAME: &str = r"^[a-z0-9_]";
const VALIDATION_REGEX_COLUMN_NAME: &str = r"^[a-z0-9_]";

static SUPPORTED_COL_TYPES: &'static [TableColumnType] = &[
    TableColumnType::Int,
    TableColumnType::Long,
    TableColumnType::Float,
    TableColumnType::Double,
    TableColumnType::Boolean,
    TableColumnType::String,
    TableColumnType::Date,
    TableColumnType::Timestamp,
];

pub struct TableController {
    descriptor_store: RedisDescriptorStore,
    glue_client: aws_sdk_glue::Client,
}

#[async_trait::async_trait]
impl BaseController<TableDescriptor> for TableController {
    async fn new() -> Result<Self> {
        let shared_config = aws_config::load_from_env().await;

        Ok(TableController {
            // TODO: url from config
            descriptor_store: RedisDescriptorStore::new("redis://127.0.0.1:6379".to_string())
                .await?,
            glue_client: aws_sdk_glue::Client::new(&shared_config),
        })
    }

    async fn validate(&self, descriptor: &TableDescriptor) -> Result<()> {
        ensure!(
            Regex::new(VALIDATION_REGEX_TABLE_NAME)
                .unwrap()
                .is_match(&descriptor.name),
            format!(
                "Invalid table name '{}'. Must match '{}'",
                descriptor.name, VALIDATION_REGEX_TABLE_NAME,
            )
        );

        for col_desc in descriptor.columns.iter() {
            ensure!(
                Regex::new(VALIDATION_REGEX_COLUMN_NAME)
                    .unwrap()
                    .is_match(&col_desc.name),
                format!(
                    "Invalid name '{}'. Must match '{}'",
                    descriptor.name, VALIDATION_REGEX_COLUMN_NAME,
                )
            );

            ensure!(
                SUPPORTED_COL_TYPES.contains(&col_desc.codec.kind),
                format!(
                    "Unsupport column type '{:?}'. Support types are '{:?}'",
                    col_desc.codec.kind, SUPPORTED_COL_TYPES,
                )
            );
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", name = "table_reconcile", skip(self, descriptor), fields(descriptor_id = %descriptor.id))]
    async fn reconcile(&self, descriptor: &TableDescriptor) -> Result<()> {
        info!("Performing reconciliation for table");
        debug!("Full descriptor to be reconciled is {:?}", descriptor);

        self.validate(&descriptor).await?;
        // FIXME: ugly af
        self.descriptor_store
            .store_descriptor::<TableDescriptor>(&descriptor)
            .await?;

        info!("Checking for dependency {}", descriptor.database);
        // Requeue for database dependency, fetch when present
        let depended_db: Option<DatabaseDescriptor> = self
            .descriptor_store
            .get_descriptor(&descriptor.database)
            .await?;
        let db_descriptor = match depended_db {
            Some(t) => {
                info!("Found depended database");
                t
            }
            None => {
                info!("Depended database could not be found");
                // TODO: requeue or wait
                return Ok(());
            }
        };

        info!("Dependency met");

        info!("Delegating resource reconcilation to clients");
        self.reconcile_glue_table(&descriptor, &db_descriptor)
            .await
            .inspect_err(|e| error!(?e, "Resource reconcicliation failed"))?;

        info!("Finished resource reconciliation");
        Ok(())
    }
}

impl TableController {
    async fn reconcile_glue_table(
        &self,
        table_descriptor: &TableDescriptor,
        db_descriptor: &DatabaseDescriptor,
    ) -> Result<()> {
        let db_name = Self::glue_name_for(&db_descriptor);

        let table = self
            .glue_client
            .get_table()
            .database_name(db_name)
            .name(&table_descriptor.name)
            .send()
            .await
            .map_err(|e| e.into_service_error());

        match table {
            Err(GetTableError {
                kind: GetTableErrorKind::EntityNotFoundException(_),
                ..
            }) => {
                self.create_table(table_descriptor, db_descriptor).await?;
            }
            Ok(_) => {
                self.update_table(table_descriptor, db_descriptor).await?;
            }
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }

    async fn create_table(
        &self,
        table_descriptor: &TableDescriptor,
        db_descriptor: &DatabaseDescriptor,
    ) -> Result<()> {
        let db_name = Self::glue_name_for(&db_descriptor);
        let table_input = Self::build_table_input(table_descriptor, db_descriptor);

        self.glue_client
            .create_table()
            .database_name(db_name)
            .table_input(table_input)
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(())
    }

    async fn update_table(
        &self,
        table_descriptor: &TableDescriptor,
        db_descriptor: &DatabaseDescriptor,
    ) -> Result<()> {
        let db_name = Self::glue_name_for(&db_descriptor);
        let table_input = Self::build_table_input(table_descriptor, db_descriptor);

        self.glue_client
            .update_table()
            .database_name(db_name)
            .table_input(table_input)
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(())
    }

    fn build_table_input(
        table_descriptor: &TableDescriptor,
        db_descriptor: &DatabaseDescriptor,
    ) -> TableInput {
        let mut storage_descriptor_builder = StorageDescriptor::builder();
        for col_desc in table_descriptor.columns.iter() {
            storage_descriptor_builder = storage_descriptor_builder.columns(
                Column::builder()
                    .name(&col_desc.name)
                    // TODO: don't abuse the name lol - write a function to convert
                    .r#type(format!("{:?}", col_desc.codec.kind).to_ascii_lowercase())
                    .comment(&col_desc.summary)
                    .build(),
            );
        }
        storage_descriptor_builder = storage_descriptor_builder.location(format!(
            "s3://{}/{}",
            Self::s3_name_for(&db_descriptor),
            table_descriptor.name
        ));

        let storage_descriptor = storage_descriptor_builder.build();

        TableInput::builder()
            .name(&table_descriptor.name)
            .description(&table_descriptor.summary)
            .storage_descriptor(storage_descriptor)
            .build()
    }

    // TODO: dedupe between this and db controller
    fn glue_name_for(descriptor: &DatabaseDescriptor) -> String {
        format!("zone_{}", descriptor.name)
    }

    fn s3_name_for(descriptor: &DatabaseDescriptor) -> String {
        format!("cz-vaporeon-db-{}", descriptor.name.replace("_", "-"))
    }
}
