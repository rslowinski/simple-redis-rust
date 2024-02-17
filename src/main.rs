use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

const NULL_BULK_STRING: &str = "$-1\r\n";

fn convert_to_bulk_string(input: &str) -> String {
    format!("${}\r\n{}\r\n", input.len(), input)
}

fn parse_request(incoming_str: &str, cache_mutex: Arc<Mutex<HashMap<String, String>>>) -> String {
    let parts = incoming_str.split("\r\n").collect::<Vec<&str>>();
    let cmd = parts[2];

    match cmd.to_lowercase().as_str() {
        "ping" => {
            return String::from("+PONG\r\n");
        }
        "echo" => {
            let re = parts[4];
            return convert_to_bulk_string(re);
        }
        "get" => {
            let cache = cache_mutex.lock().unwrap();
            let key = parts[4];
            let value = cache.get(key);
            return match value {
                None => { NULL_BULK_STRING.to_string() }
                Some(_value) => { convert_to_bulk_string(_value) }
            };
        }
        "set" => {
            let key = parts[4];
            let value = parts[6];
            let mut cache = cache_mutex.lock().unwrap();
            cache.insert(key.to_string(), value.to_string());
            return String::from("\r\nOK");
        }
        _ => String::from("")
    }
}

fn handle_stream(mut tcp_stream: TcpStream, cache_mutex: Arc<Mutex<HashMap<String, String>>>) {
    loop {
        let mut input_buf = [0; 512];
        let num_bytes = tcp_stream.read(&mut input_buf).unwrap();
        if num_bytes == 0 { return; }

        let incoming_str = std::str::from_utf8(&input_buf).unwrap();
        let resp = parse_request(incoming_str, cache_mutex.clone());

        tcp_stream.write_all(resp.as_ref()).unwrap()
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let cache = HashMap::new();
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
pub fn test_echo_parse_request() {
    let mut cache = HashMap::new();
    cache.insert("hey".to_string(), "works".to_string());
    let cache_mutex = Arc::new(Mutex::new(cache));


    let echo_str = "*2\r\n$4\r\nGET\r\n$3\r\nhey\r\n";
    let res = parse_request(echo_str, cache_mutex);
    assert_eq!(res, "$5\r\nworks\r\n");
}
