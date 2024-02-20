use std::collections::HashMap;

use crate::record::Record;

pub struct Database {
    records: HashMap<String, Record>,
}

impl Database {
    pub(crate) fn new() -> Database {
        Database { records: HashMap::new() }
    }

    pub(crate) fn insert(&mut self, record: Record) {
        self.records.insert(record.key.to_string(), record);
    }

    pub(crate) fn get(&self, key: &str) -> Option<&Record> {
        self.records.get(key).and_then(|record| {
            if record.is_expired() { None } else { Some(record) }
        })
    }
}
