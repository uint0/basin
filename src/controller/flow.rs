use std::borrow::Cow;

use super::base::BaseController;
use crate::{
    config::BasinConfig,
    fluid::descriptor::flow::{FlowCondition, FlowDescriptor, FlowStepTransformation},
    provisioner::waterwheel::{
        WaterwheelDockerTask, WaterwheelJob, WaterwheelTask, WaterwheelTrigger,
    },
};

use anyhow::{bail, Result};
use tracing::{debug, error, info};

const PRIMORDIAL_TIME: &str = "2000-01-01T00:00:00Z";

pub struct FlowController {
    waterwheel_project: String,
    waterwheel_url: String,
    http_client: reqwest::Client,
}

// TODO: support different deployment targets (i.e. airflow)
#[async_trait::async_trait]
impl BaseController<FlowDescriptor> for FlowController {
    async fn validate(&self, _descriptor: &FlowDescriptor) -> Result<()> {
        // NOTE: actual validation is handled downstream
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "db_reconcile", skip(self, descriptor), fields(descriptor_id = %descriptor.id))]
    async fn reconcile(&self, descriptor: &FlowDescriptor) -> Result<()> {
        info!("Performing reconciliation for flow");

        let job_spec = self.build_waterwheel_job_spec(descriptor)?;
        info!(
            id = job_spec.uuid,
            "Sending job specification to waterwheel"
        );
        debug!("job_spec: {:?}", job_spec);

        let resp = self
            .http_client
            .post(format!("{}/api/jobs", self.waterwheel_url))
            .json(&job_spec)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let resp_msg = resp.text().await?;
            error!(
                status = status.as_u16(),
                resp_msg, "error when submitting job to waterwheel",
            );
            bail!("error when submitting job to waterwheel");
        }

        info!("Submitted job to waterwheel");
        Ok(())
    }
}

impl FlowController {
    pub async fn new(conf: &BasinConfig) -> Result<Self> {
        Ok(FlowController {
            waterwheel_project: conf.waterwheel_project.clone(),
            waterwheel_url: conf.waterwheel_url.clone(),
            http_client: reqwest::Client::new(),
        })
    }

    fn build_waterwheel_job_spec(&self, raw_descriptor: &FlowDescriptor) -> Result<WaterwheelJob> {
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
            };

            let depends: Vec<String> = step
                .parents
                .into_iter()
                .map(|x| format!("task/{}", x))
                .collect();

            tasks.push(WaterwheelTask {
                name: step.name.clone(),
                docker: task,
                depends: if depends.is_empty() {
                    vec!["trigger/cron".to_string()]
                } else {
                    depends
                },
            })
        }

        Ok(WaterwheelJob {
            uuid: descriptor.id.clone(),
            project: self.waterwheel_project.clone(),
            name: descriptor.name.clone(),
            description: descriptor.summary.clone(),
            paused: false,
            triggers,
            tasks,
        })
    }
}
