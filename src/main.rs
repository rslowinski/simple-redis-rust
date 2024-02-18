use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::sleep;

use chrono::{DateTime, Duration, Utc};

const NULL_BULK_STRING: &str = "$-1\r\n";

struct Record {
    key: String,
    value: String,
    expiry: Option<DateTime<Utc>>,
}

impl Record {
    fn new(key: String, value: String, expire_in_ms: Option<i64>) -> Record {
        let expiry = expire_in_ms.map(|ms| Utc::now() + Duration::milliseconds(ms));

        Record {
            key,
            value,
            expiry,
        }
    }

    fn is_expired(&self) -> bool {
        self.expiry
            .map(|expiry_time| Utc::now() > expiry_time)
            .unwrap_or(false)
    }
}

struct Database {
    records: HashMap<String, Record>,
}

impl Database {
    fn insert(&mut self, record: Record) {
        self.records.insert(record.key.to_string(), record);
    }

    fn get(&self, key: &str) -> Option<&Record> {
        let record = self.records.get(key);
        if record.is_some() && record.unwrap().is_expired() {
            return None;
        }
        return record;
    }
}


fn convert_to_bulk_string(input: &str) -> String {
    format!("${}\r\n{}\r\n", input.len(), input)
}


fn handle_req(incoming_str: &str, cache_mutex: Arc<Mutex<Database>>) -> String {
    let parts = incoming_str.split("\r\n").collect::<Vec<&str>>();
    let cmd = parts[2];

    println!("received request=\n{}", incoming_str);

    return match cmd.to_lowercase().as_str() {
        "ping" => {
            String::from("+PONG\r\n")
        }
        "echo" => {
            let re = parts[4];
            convert_to_bulk_string(re)
        }
        "get" => {
            let cache = cache_mutex.lock().unwrap();
            let key = parts[4];
            let record = cache.get(key);
            match record {
                None => { NULL_BULK_STRING.to_string() }
                Some(_record) => { convert_to_bulk_string(&_record.value) }
            }
        }
        "set" => {
            let key = parts[4];
            let value = parts[6];
            let expiry: Option<i64> = parts.get(10).and_then(|s| s.parse::<i64>().ok());
            println!("expiry: {}", parts.get(10).unwrap());
            let mut cache = cache_mutex.lock().unwrap();
            let record = Record::new(key.to_string(), value.to_string(), expiry);
            cache.insert(record);
            String::from("+OK\r\n")
        }
        _ => String::from("")
    };
}

fn handle_stream(mut tcp_stream: TcpStream, cache_mutex: Arc<Mutex<Database>>) {
    loop {
        let mut input_buf = [0; 512];
        let num_bytes = tcp_stream.read(&mut input_buf).unwrap();
        if num_bytes == 0 { return; }

        let incoming_str = std::str::from_utf8(&input_buf).unwrap();
        let resp = handle_req(incoming_str, cache_mutex.clone());
        println!("resp=\n{}", resp);

        tcp_stream.write_all(resp.as_ref()).unwrap()
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let cache = Database { records: HashMap::new() };
    let cache_mutex = Arc::new(Mutex::new(cache));


    for stream in listener.incoming() {
        let cache_mutex_clone = cache_mutex.clone();
        match stream {
            Ok(_stream) => {
                thread::spawn(move || {
                    handle_stream(_stream, cache_mutex_clone);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}


#[test]
pub fn test_get_and_set() {
    let cache = Database { records: HashMap::new() };
    let cache_mutex = Arc::new(Mutex::new(cache));


    let set_command = "*3\r\n$3\r\nset\r\n$3\r\nhey\r\n$5\r\nworld";
    handle_req(set_command, cache_mutex.clone());

    let get_command = "*2\r\n$3\r\nGET\r\n$3\r\nhey\r\n";
    let res = handle_req(get_command, cache_mutex.clone());
    assert_eq!(res, "$5\r\nworld\r\n");
}

#[test]
pub fn test_expiry() {
    let cache = Database { records: HashMap::new() };
    let cache_mutex = Arc::new(Mutex::new(cache));


    let set_command = "*4\r\n$3\r\nset\r\n$3\r\nhey\r\n$5\r\nworld\r\n$2\r\npx\r\n$3\r\n100";
    handle_req(set_command, cache_mutex.clone());

    let get_command = "*2\r\n$3\r\nGET\r\n$3\r\nhey\r\n";
    let res = handle_req(get_command, cache_mutex.clone());
    assert_eq!(res, "$5\r\nworld\r\n");

    sleep(std::time::Duration::from_millis(105));
    let res = handle_req(get_command, cache_mutex.clone());
    assert_eq!(res, NULL_BULK_STRING);
}
