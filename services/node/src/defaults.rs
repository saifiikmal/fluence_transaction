pub static TRANSACTIONS_TABLE_NAME: &str = "transactions";
pub static METADATAS_TABLE_NAME: &str = "metadatas";
pub static META_CONTRACT_TABLE_NAME: &str = "metacontracts";
pub static CRON_TABLE_NAME: &str = "cron";
pub static CRON_TX_TABLE_NAME: &str = "cron_tx";
pub static TRANSACTION_RECEIPT_TABLE_NAME: &str = "transaction_receipt";
pub static REGISTRY_TABLE_NAME: &str = "registry";
// General Status
pub static STATUS_PENDING: i64 = 0;
pub static STATUS_DONE: i64 = 1;
pub static STATUS_FAILED: i64 = 1;
// Transaction Receipt
pub static RECEIPT_STATUS_SUCCESS: i64 = 1;
pub static RECEIPT_STATUS_FAILED: i64 = 2;
// Cron
pub static CRON_STATUS_ENABLE: i64 = 1;
pub static CRON_STATUS_DISABLE: i64 = 0;
// Cron tx
pub static CRON_TX_STATUS_SUCCESS: i64 = 1;
pub static CRON_TX_STATUS_FAILED: i64 = 2;
// CRON ACTION
pub const CRON_ACTION_CREATE: &str = "create";
pub const CRON_ACTION_UPDATE: &str = "update";
pub const CRON_ACTION_UPDATE_STATUS: &str = "update_status";
// METHODS
pub static METHOD_CONTRACT: &str = "contract";
pub static METHOD_METADATA: &str = "metadata";
pub static METHOD_CLONE: &str = "clone";
pub static METHOD_CRON: &str = "cron";
// ENCRYPTION
pub static ENCRYPTION_TYPE_SECP256K1: &str = "secp256k1";
pub static ENCRYPTION_TYPE_ED25519: &str = "ed25519";
// RQLITE ENDPOINTS
pub static SQL_EXECUTE: &str = "http://172.17.0.1:4101/db/execute";
pub static SQL_QUERY: &str = "http://172.17.0.1:4101/db/query?associative";
