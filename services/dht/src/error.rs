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
}
