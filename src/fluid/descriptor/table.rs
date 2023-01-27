use serde::{Deserialize, Serialize};

use super::IdentifiableDescriptor;

#[derive(Serialize, Deserialize, Debug)]
pub struct TableDescriptor {
    pub id: String,
    pub name: String,
    pub summary: String,
    pub columns: Vec<TableColumnAttribute>,
    pub database: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TableColumnAttribute {
    pub id: String,
    pub name: String,
    pub summary: String,
    pub codec: TableColumnCodec,
    pub nullable: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TableColumnCodec {
    #[serde(rename = "type")]
    pub kind: TableColumnType,
    // FIXME: we don't support any of the constraints
}

#[derive(PartialEq, Serialize, Deserialize, Debug)]
pub enum TableColumnType {
    Int,
    Long,
    Float,
    Double,
    Boolean,
    String,
    Date,
    Timestamp,
    Complex, // POC: pretty much exists to give us a type not supported by glue
}

impl IdentifiableDescriptor for TableDescriptor {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn kind(&self) -> String {
        String::from("table")
    }
}
