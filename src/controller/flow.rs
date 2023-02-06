use std::borrow::Cow;

use super::{base::BaseController, error::ControllerReconciliationError};
use crate::{
    config::BasinConfig,
    descriptor_store::{DescriptorStore, RedisDescriptorStore},
    fluid::descriptor::flow::{FlowCondition, FlowDescriptor, FlowStepTransformation},
    provisioner::waterwheel::{
        WaterwheelDockerTask, WaterwheelJob, WaterwheelTask, WaterwheelTrigger,
    },
};

use anyhow::{anyhow, bail, Result};
use tracing::{debug, error, info};

const PRIMORDIAL_TIME: &str = "2000-01-01T00:00:00Z";

pub struct FlowController {
    descriptor_store: RedisDescriptorStore,
    waterwheel_project: String,
    waterwheel_url: String,
    http_client: reqwest::Client,
}

// TODO: support different deployment targets (i.e. airflow)
#[async_trait::async_trait]
impl BaseController<FlowDescriptor> for FlowController {
    async fn validate(&self, descriptor: &FlowDescriptor) -> Result<()> {
        // NOTE: actual validation is handled downstream, this checks what we support generating specs for
        self.build_waterwheel_job_spec(descriptor)?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "db_reconcile", skip(self, descriptor), fields(descriptor_id = %descriptor.id))]
    async fn reconcile(&self, descriptor: &FlowDescriptor) -> Result<()> {
        info!("Performing reconciliation for flow");

        let job_spec = self
            .build_waterwheel_job_spec(descriptor)
            .map_err(|e| ControllerReconciliationError::ControllerError(e.into()))?;
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
            .await
            .map_err(|e| ControllerReconciliationError::ProvisionerError(e.into()))?;

        let status = resp.status();
        if !status.is_success() {
            let resp_msg = resp
                .text()
                .await
                .map_err(|e| ControllerReconciliationError::ProvisionerError(e.into()))?;
            error!(
                status = status.as_u16(),
                resp_msg, "error when submitting job to waterwheel",
            );
            return Err(ControllerReconciliationError::ProvisionerError(anyhow!(
                "error when submitting job to waterwheel"
            ))
            .into());
        }

        info!("Submitted job to waterwheel");
        Ok(())
    }

    async fn list_descriptors(&self) -> Result<Vec<FlowDescriptor>> {
        Ok(self
            .descriptor_store
            .list_descriptors::<FlowDescriptor>("flow")
            .await?)
    }
}

impl FlowController {
    pub async fn new(conf: &BasinConfig) -> Result<Self> {
        Ok(FlowController {
            descriptor_store: RedisDescriptorStore::new(&conf.redis_url).await?,
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
