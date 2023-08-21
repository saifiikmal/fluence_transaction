use marine_sqlite_connector::Error as SqliteError;
use serde_json::Error as SerdeJsonError;
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
    #[error("Record not found: {0}")]
    RecordNotFound(String),
    #[error("Record found: {0}")]
    RecordFound(String),
    #[error("Invalid signature: {0}")]
    InvalidSignature(String),
    #[error("Invalid encryption key: {0}")]
    InvalidEncryption(String),
    #[error["Invalid method: {0}"]]
    InvalidMethod(String),
    #[error["Invalid owner: {0}"]]
    InvalidOwner(String),
    #[error["Not supported encryption: {0}"]]
    NotSupportedEncryptionType(String),
    #[error["Does not specify encryption"]]
    NoEncryptionType(),
    #[error["Invalid data format: {0}"]]
    InvalidDataFormatForMethodType(String),
    #[error["No Meta Contract ID specify"]]
    NoProgramId(),
}

impl From<SerdeJsonError> for ServiceError {
  fn from(error: SerdeJsonError) -> Self {
      ServiceError::InternalError(error.to_string())
  }
}