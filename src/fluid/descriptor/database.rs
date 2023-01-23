use serde::{Deserialize, Serialize};

// NOTE: probably more thought needs to be put into this esp re versioning
#[derive(Serialize, Deserialize, Debug)]
pub struct DatabaseDescriptor {
    pub id: String,
    pub name: String,
    pub summary: String,
}
