use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;


fn parse_request(incoming_str: &str) -> Vec<&str> {
    let parts = incoming_str.split("\r\n").collect::<Vec<&str>>();
    let cmd = parts[2];

    match cmd.to_lowercase().as_str() {
        "ping" => {
            return vec!["pong"];
        }
        "echo" => {
            return vec![parts[4]]
        }
        _ => vec![""]
    }
}

fn handle_stream(mut tcp_stream: TcpStream) {
    loop {
        let mut input_buf = [0; 512];
        let num_bytes = tcp_stream.read(&mut input_buf).unwrap();
        if num_bytes == 0 { return; }

        let incoming_str = std::str::from_utf8(&input_buf).unwrap();
        parse_request(incoming_str);
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                thread::spawn(move || {
                    handle_stream(_stream);
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
    let echo_str = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
    let res = parse_request(echo_str);
    assert_eq!(res, vec!["hey"]);
}
