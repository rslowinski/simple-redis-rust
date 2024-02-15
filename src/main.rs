use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn handle_stream(mut tcp_stream: TcpStream) {
    let mut input_buf = [0; 512];
    let output_buf = b"+PONG\r\n";
    loop {
        tcp_stream.read(&mut input_buf).expect("Failed to read from client");
        tcp_stream.write_all(output_buf).expect("Failed to wrtie to client");
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
