use std::borrow::Cow;

use super::base::BaseController;
use crate::{
    fluid::descriptor::flow::{FlowCondition, FlowDescriptor, FlowStepTransformation},
    provisioner::waterwheel::{
        WaterwheelDockerTask, WaterwheelJob, WaterwheelTask, WaterwheelTrigger,
    },
};

use anyhow::{bail, Result};
use tracing::error;

const DEFAULT_WW_PROJECT: &str = "basin_project";
const PRIMORDIAL_TIME: &str = "2000-01-01T00:00:00Z";

pub struct FlowController {
    pub waterwheel_url: String,
}

// TODO: support different deployment targets
#[async_trait::async_trait]
impl BaseController<FlowDescriptor> for FlowController {
    async fn validate(&self, _descriptor: &FlowDescriptor) -> Result<()> {
        // TODO: cycle detection
        Ok(())
    }

    async fn reconcile(&self, descriptor: &FlowDescriptor) -> Result<()> {
        // TODO: share a client for pooling
        let client = reqwest::Client::new();

        client
            .post(format!("{}/api/v1/job", self.waterwheel_url))
            .json(&Self::build_waterwheel_job_spec(descriptor)?)
            .send()
            .await?;

        Ok(())
    }
}

impl FlowController {
    pub async fn new(waterwheel_url: String) -> Result<Self> {
        Ok(FlowController { waterwheel_url })
    }

    fn build_waterwheel_job_spec(raw_descriptor: &FlowDescriptor) -> Result<WaterwheelJob> {
        let descriptor = raw_descriptor.clone();

        let mut triggers: Vec<WaterwheelTrigger> = vec![];
        match descriptor.condition {
            FlowCondition::Cron(cron_condition) => {
                triggers.push(WaterwheelTrigger {
                    name: "cron".to_string(),
                    start: PRIMORDIAL_TIME.to_string(),
                    period: cron_condition.schedule.clone(),
                });
            }
            t => {
                error!("Unsupported trigger condition {:?}", t);
                bail!("unsupported trigger condition");
            }
        }

        let mut tasks: Vec<WaterwheelTask> = vec![];
        for step in descriptor.steps.into_iter() {
            let task = match step.transformation {
                FlowStepTransformation::Sql(t) => {
                    let escaped_sql = shell_escape::escape(Cow::from(t.sql));
                    WaterwheelDockerTask {
                        image: "bash".to_string(),
                        args: vec!["-c".to_string(), format!("echo \"{}\"", escaped_sql)],
                    }
                }
                t => {
                    error!("Unsupported transformation type {:?}", t);
                    bail!("Unsupported transformation type")
                }
            };

            let depends: Vec<String> = step
                .parents
                .into_iter()
                .map(|x| format!("task/{}", x))
                .collect();

            tasks.push(WaterwheelTask {
                name: step.name.clone(),
                docker: task,
                depends,
            })
        }

        Ok(WaterwheelJob {
            // FIXME: use better id
            uuid: descriptor.id.clone(),
            project: DEFAULT_WW_PROJECT.to_string(),
            name: descriptor.name.clone(),
            description: descriptor.summary.clone(),
            paused: false,
            triggers,
            tasks,
        })
    }
}
