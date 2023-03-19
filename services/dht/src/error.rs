use marine_sqlite_connector::Error as SqliteError;
use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum ServiceError {
    #[error("Internal Sqlite error: {0}")]
    SqliteError(
        #[from]
        #[source]
        SqliteError,
    ),
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Invalid signature: {0}")]
    InvalidSignature(String),
    #[error["Invalid method: {0}"]]
    InvalidMethod(String),
    #[error["Invalid owner: {0}"]]
    InvalidOwner(String),
    #[error["Not supported encryption: {0}"]]
    NotSupportedEncryptionType(String),
    #[error["Does not specify encryption"]]
    NoEncryptionType(),
}
