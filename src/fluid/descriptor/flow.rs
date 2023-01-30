use serde::{Deserialize, Serialize};

use super::IdentifiableDescriptor;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FlowDescriptor {
    pub id: String,
    pub name: String,
    pub summary: String,
    pub condition: FlowCondition,
    pub steps: Vec<FlowStep>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FlowCondition {
    Cron(FlowCronCondition),
    Upstream(FlowUpstreamCondition),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FlowCronCondition {
    pub schedule: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FlowUpstreamCondition {
    pub upstream: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FlowUpstreamFlowCondition {
    pub flow: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FlowStep {
    pub name: String,
    pub summary: String,
    pub parents: Vec<String>,
    pub timeout: String,
    pub transformation: FlowStepTransformation,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FlowStepTransformation {
    Sql(FlowSqlTransformation),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FlowSqlTransformation {
    pub sql: String,
}

impl IdentifiableDescriptor for FlowDescriptor {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn kind(&self) -> String {
        String::from("flow")
    }
}
