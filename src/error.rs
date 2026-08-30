use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("kube api error: {0}")]
    Kube(#[from] kube::Error),

    #[error("missing object metadata field: {0}")]
    MissingMetadata(&'static str),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("pod exec error: {0}")]
    Exec(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("redis protocol error: {0}")]
    Resp(String),

    #[error("timed out after {0:?}")]
    Timeout(std::time::Duration),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
