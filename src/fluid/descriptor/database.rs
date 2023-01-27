use serde::{Deserialize, Serialize};

use super::IdentifiableDescriptor;

// NOTE: probably more thought needs to be put into this esp re versioning
#[derive(Serialize, Deserialize, Debug)]
pub struct DatabaseDescriptor {
    pub id: String,
    pub name: String,
    pub summary: String,
}

impl IdentifiableDescriptor for DatabaseDescriptor {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn kind(&self) -> String {
        String::from("database")
    }
}
