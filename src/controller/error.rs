use thiserror::Error;

#[derive(Error, Debug)]
pub enum ControllerReconciliationError {
    #[error("error from provisioner")]
    ProvisionerError(#[source] anyhow::Error),
    #[error("error from controller")]
    ControllerError(#[source] anyhow::Error),
    #[error("missing dependency `{0}`")]
    DependencyMissing(String),
}

#[derive(Error, Debug)]
pub enum ControllerResourceError {
    #[error("circuit broken for {id:?} due to {source:?}")]
    CircuitBroken {
        #[source]
        source: anyhow::Error,
        id: String,
    },
}
