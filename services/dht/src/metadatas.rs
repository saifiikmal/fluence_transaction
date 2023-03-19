use marine_rs_sdk::marine;

#[marine]
#[derive(Debug, Default, Clone)]
pub struct Metadata {
    pub data_key: String,
    pub alias: String,
    pub cid: String,
    pub public_key: String,
    pub enc: String,
    pub service_id: String,
}

impl Metadata {
    pub fn new(
        data_key: String,
        alias: String,
        cid: String,
        public_key: String,
        enc: String,
        service_id: String,
    ) -> Self {
        Self {
            data_key,
            alias,
            cid,
            public_key,
            enc,
            service_id,
        }
    }
}
