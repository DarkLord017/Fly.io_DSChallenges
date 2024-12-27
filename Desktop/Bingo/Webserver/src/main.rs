use std::{
    io::{prelude::*, BufReader},
    net::{TcpListener, TcpStream},
    fs,
};


fn main() {
    //The function is called bind because, in networking, connecting to a port to listen to is known as “binding to a port.”
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    for stream in listener.incoming() {
        //unwrap function is used to trerminate the program if any error occurs
        let stream = stream.unwrap();

       handle_connection(stream)
    }

}

fn handle_connection(mut stream: TcpStream){
    let buf_reader = BufReader::new(&stream);
    let request_line = buf_reader.lines().next().unwrap().unwrap();

    let (status_line, filename) = if request_line == "GET / HTTP/1.1" {
        ("HTTP/1.1 200 OK", "hello.html")
    } else {
        ("HTTP/1.1 404 NOT FOUND", "404.html")
    };

    let contents = fs::read_to_string(filename).unwrap();
    let length = contents.len();

    let response =
        format!("{status_line}\r\nContent-Length: {length}\r\n\r\n{contents}");

    stream.write_all(response.as_bytes()).unwrap();


}