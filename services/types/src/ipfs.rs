use eyre::Result;
use marine_rs_sdk::marine;

#[marine]
#[derive(Debug)]
pub struct IpfsDagPutResult {
    pub success: bool,
    pub error: String,
    pub cid: String,
}

impl From<Result<String>> for IpfsDagPutResult {
    fn from(result: Result<String>) -> Self {
        match result {
            Ok(cid) => Self {
                success: true,
                error: "".to_string(),
                cid,
            },
            Err(err) => Self {
                success: false,
                error: err.to_string(),
                cid: "".to_string(),
            },
        }
    }
}

#[marine]
#[derive(Debug)]
pub struct IpfsDagGetResult {
    pub success: bool,
    pub error: String,
    pub content: String,
}

impl From<Result<String>> for IpfsDagGetResult {
    fn from(result: Result<String>) -> Self {
        match result {
            Ok(content) => Self {
                success: true,
                error: "".to_string(),
                content,
            },
            Err(err) => Self {
                success: false,
                error: err.to_string(),
                content: "".to_string(),
            },
        }
    }
}
