use std::{env, thread};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
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
        Ok(Command::Info(_)) => {
            let args = env::args().collect::<Vec<String>>();
            let master_addr = get_master_addr(args);

            let repl_id_resp = convert_to_bulk_string(String::from("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"));
            let offset_resp = convert_to_bulk_string(String::from("master_repl_offset:0"));

            if master_addr.is_some() {
                let role = "role:slave";
                format!("*3\r\n{}",
                        convert_to_bulk_string(String::from(role)),
                        )
            } else {
                let role = "role:master";
                format!("*3\r\n{}{}{}",
                        convert_to_bulk_string(String::from(role)),
                        repl_id_resp,
                        offset_resp)
            }
        }
        Err(_) => "Incorrect or unsupported req".to_string(),
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
    let args = env::args().collect::<Vec<String>>();

    let port: u16 = args.iter()
        .position(|arg| arg == "--port")
        .and_then(|index| args.get(index + 1))
        .and_then(|port| port.parse().ok())
        .unwrap_or(6379);

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).unwrap();

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

fn get_master_addr(args: Vec<String>) -> Option<String> {
    args.iter()
        .position(|arg| arg == "--replicaof")
        .and_then(|index| args
            .get(index + 1)
            .and_then(|ip| args
                .get(index + 2)
                .map(|port| format!("{}:{}", ip, port))))
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
