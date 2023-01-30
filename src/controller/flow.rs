use std::borrow::Cow;

use super::base::BaseController;
use crate::{
    fluid::descriptor::flow::{FlowCondition, FlowDescriptor, FlowStepTransformation},
    provisioner::waterwheel::{
        WaterwheelDockerTask, WaterwheelJob, WaterwheelTask, WaterwheelTrigger,
    },
};

use anyhow::{bail, Result};
use reqwest::StatusCode;
use tracing::{error, info};

const DEFAULT_WW_PROJECT: &str = "test_project";
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

    #[tracing::instrument(level = "info", name = "db_reconcile", skip(self, descriptor), fields(descriptor_id = %descriptor.id))]
    async fn reconcile(&self, descriptor: &FlowDescriptor) -> Result<()> {
        info!("Performing reconciliation for flow");

        // TODO: share a client for pooling
        let client = reqwest::Client::new();

        let job_spec = Self::build_waterwheel_job_spec(descriptor)?;
        info!(
            id = job_spec.uuid,
            "Sending job specification to waterwheel"
        );
        println!("job_spec: {:?}", job_spec);

        let resp = client
            .post(format!("{}/api/jobs", self.waterwheel_url))
            .json(&job_spec)
            .send()
            .await?;

        // TODO: status code check

        Ok(())
    }
}

impl FlowController {
    // TODO: maybe just take in config
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
                    cron: cron_condition.schedule.clone(),
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
