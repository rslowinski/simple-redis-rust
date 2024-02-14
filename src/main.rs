use std::io::{Read, Write};
// Uncomment this block to pass the first stage
use std::net::{TcpListener, TcpStream};


fn handle_stream(mut tcp_stream: TcpStream) {
    let mut input_buf = [0; 512];
    let mut output_buf = b"+PONG\r\n";
    loop {
        tcp_stream.read(&mut input_buf).expect("Failed to read from client");
        tcp_stream.write_all(&mut output_buf).expect("Failed to wrtie to client");
    }

}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    //
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                handle_stream(_stream)
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
