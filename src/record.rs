use chrono::{DateTime, Duration, Utc};

pub struct Record {
    pub(crate) key: String,
    pub(crate) value: String,
    expiry: Option<DateTime<Utc>>,
}

impl Record {
    pub(crate) fn new(key: String, value: String, expire_in_ms: Option<i64>) -> Record {
        let expiry = expire_in_ms.map(|ms| Utc::now() + Duration::milliseconds(ms));

        Record {
            key,
            value,
            expiry,
        }
    }

    pub(crate) fn is_expired(&self) -> bool {
        self.expiry
            .map(|expiry_time| Utc::now() > expiry_time)
            .unwrap_or(false)
    }
}
