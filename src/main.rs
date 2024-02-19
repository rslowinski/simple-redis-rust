use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::sleep;

use database::Database;
use record::Record;

use crate::command::Command;

mod record;
mod database;
mod command;

const NULL_BULK_STRING: &str = "$-1\r\n";


fn convert_to_bulk_string(input: String) -> String {
    format!("${}\r\n{}\r\n", input.len(), input)
}

fn handle_req(incoming_str: &str, cache_mutex: Arc<Mutex<Database>>) -> String {
    println!("received request=\n{}", incoming_str);
    let command = Command::parse_redis_format(incoming_str);


    return match command {
        Ok(Command::Ping) => {
            String::from("+PONG\r\n")
        }
        Ok(Command::Echo(echo_val)) => {
            convert_to_bulk_string(echo_val)
        }
        Ok(Command::Get(key)) => {
            let database = cache_mutex.lock().unwrap();
            match database.get(&key) {
                None => NULL_BULK_STRING.to_string(),
                Some(record) => convert_to_bulk_string(record.value.clone())
            }
        }
        Ok(Command::Set(params)) => {
            let mut database = cache_mutex.lock().unwrap();
            let record = Record::new(params.key, params.value, params.expiry);
            database.insert(record);
            "+OK\r\n".to_string()
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

    let cache = Database::new();
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
    let cache = Database::new();
    let cache_mutex = Arc::new(Mutex::new(cache));


    let set_command = "*3\r\n$3\r\nset\r\n$3\r\nhey\r\n$5\r\nworld";
    handle_req(set_command, cache_mutex.clone());

    let get_command = "*2\r\n$3\r\nGET\r\n$3\r\nhey\r\n";
    let res = handle_req(get_command, cache_mutex.clone());
    assert_eq!(res, "$5\r\nworld\r\n");
}

#[test]
pub fn test_expiry() {
    let cache = Database::new();
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
