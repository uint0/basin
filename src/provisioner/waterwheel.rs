use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct WaterwheelJob {
    pub uuid: String,
    pub project: String,
    pub name: String,
    pub description: String,
    pub paused: bool,
    pub triggers: Vec<WaterwheelTrigger>,
    pub tasks: Vec<WaterwheelTask>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WaterwheelTrigger {
    pub name: String,
    // FIXME: probably chrono
    pub start: String,
    pub cron: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WaterwheelTask {
    pub name: String,
    // FIXME: probably a enum
    pub docker: WaterwheelDockerTask,
    pub depends: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WaterwheelDockerTask {
    pub image: String,
    pub args: Vec<String>,
}
