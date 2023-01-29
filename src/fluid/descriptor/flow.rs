use serde::Deserialize;

use super::IdentifiableDescriptor;

#[derive(Serialize, Deserialize, Debug)]
pub struct FlowDescriptor {
    pub id: String,
    pub name: String,
    pub summary: String,
    pub condition: FlowCondition,
    pub steps: Vec<FlowStep>, 
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FlowCondition {
    Cron(FlowCronCondition),
    Upstream(FlowUpstreamCondition),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FlowCronCondition {
    pub schedule: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FlowUpstreamCondition {
    pub upstream: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FlowUpstreamFlowCondition {
    pub flow: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FlowStep {
    pub name: String,
    pub summary: String,
    pub parents: String,
    pub timeout: String,
    pub transformation: FlowStepTransformation,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FlowStepTransformation {
    Sql(FlowSqlTransformation),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FlowSqlTransformation {
    pub endpoint: String,
}

impl IdentifiableDescriptor for FlowDescriptor {
    fn id(&self) -> String {
        self.id.clone()
    }
    
    fn kind(&self) -> String {
        String::from("flow")
    }
}
