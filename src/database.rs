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
        let record = self.records.get(key);
        if record.is_some() && record.unwrap().is_expired() {
            return None;
        }
        return record;
    }
}
