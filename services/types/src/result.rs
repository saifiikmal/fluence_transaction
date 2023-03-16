use marine_rs_sdk::marine;
use marine_sqlite_connector::Result;
//
#[marine]
pub struct FdbResult {
    pub success: bool,
    pub err_msg: String,
}

impl FdbResult {
    pub fn from_res(res: Result<()>) -> FdbResult {
        match res {
            Ok(_v) => FdbResult {
                success: true,
                err_msg: "".into(),
            },
            Err(e) => FdbResult {
                success: false,
                err_msg: e.to_string(),
            },
        }
    }

    pub fn from_err_str(e: &str) -> FdbResult {
        FdbResult {
            success: false,
            err_msg: e.to_string(),
        }
    }
}
