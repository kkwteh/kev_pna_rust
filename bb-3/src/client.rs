use std::io::{Read, Write};
use std::net::TcpStream;

fn main() {
    match TcpStream::connect("localhost:6379") {
        Ok(mut stream) => {
            println!("Successfully connected to server in port 6379");

            // let msg = b"*4\r\n$4\r\nPING\r\n$6\r\nhello!\r\n";
            let msg = b"*1\r\n$4\r\nPING\r\n";

            stream.write(msg).unwrap();

            let mut data: Vec<u8> = [0; 256].to_vec();
            dbg!(data.len());
            match stream.read(&mut data) {
                Ok(n) => {
                    println!("Reply has len {}", n);
                    println!("{:?}", std::str::from_utf8(&data[0..n]).unwrap());
                }
                Err(e) => {
                    println!("Failed to receive data: {}", e);
                }
            }
        }
        Err(e) => {
            println!("Failed to connect: {}", e);
        }
    }
    println!("Terminated.");
}
