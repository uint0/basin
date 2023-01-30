pub mod database;
pub mod flow;
pub mod table;

pub trait IdentifiableDescriptor {
    fn id(&self) -> String;
    fn kind(&self) -> String;
}
