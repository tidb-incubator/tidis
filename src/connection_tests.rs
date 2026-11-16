use super::*;
use async_std::net::TcpListener;
use rustls::{ClientConfig, ClientSession, StreamOwned};
use std::io::{BufReader, Read, Write};
use std::sync::Arc;
use std::time::Duration;

trait Peer: Read + Write {}
impl<T: Read + Write> Peer for T {}

// Exercise the connection layer directly: no AUTH or backend is involved.
async fn exchange(tls: bool, payload: Vec<u8>, expected: &[u8]) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let identity = include_bytes!("testdata/tls.pem");
    let certs = rustls::internal::pemfile::certs(&mut BufReader::new(&identity[..])).unwrap();
    let mut keys =
        rustls::internal::pemfile::rsa_private_keys(&mut BufReader::new(&identity[..])).unwrap();
    let mut server_config = rustls::ServerConfig::new(rustls::NoClientAuth::new());
    server_config
        .set_single_cert(certs.clone(), keys.remove(0))
        .unwrap();
    let mut client_config = ClientConfig::new();
    client_config.root_store.add(&certs[0]).unwrap();
    let expected_response = expected.to_vec();
    let (partial_tx, partial_rx) = tokio::sync::oneshot::channel();
    let (continue_tx, continue_rx) = std::sync::mpsc::channel();
    let client = tokio::task::spawn_blocking(move || {
        let socket = std::net::TcpStream::connect(addr).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut peer: Box<dyn Peer> = if tls {
            Box::new(StreamOwned::new(
                ClientSession::new(
                    &Arc::new(client_config),
                    webpki::DNSNameRef::try_from_ascii_str("localhost").unwrap(),
                ),
                socket,
            ))
        } else {
            Box::new(socket)
        };
        // Send a fragmented command followed by two pipelined frames.
        peer.write_all(b"*1\r\n$").unwrap();
        peer.flush().unwrap();
        partial_tx.send(()).unwrap();
        continue_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        peer.write_all(b"4\r\nPING\r\n").unwrap();
        peer.flush().unwrap();
        let mut reply = [0; 7];
        peer.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"+PONG\r\n");
        peer.write_all(&payload).unwrap();
        peer.flush().unwrap();
        let mut response = Vec::new();
        if let Err(error) = peer.read_to_end(&mut response) {
            // TLS EOF may lack close_notify; timeouts must still fail this test.
            assert!(
                matches!(
                    error.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                ),
                "{}",
                error
            );
        }
        assert_eq!(response, expected_response);
    });
    let (socket, _) = listener.accept().await.unwrap();
    let mut connection = if tls {
        let stream = async_tls::TlsAcceptor::from(server_config)
            .accept(socket)
            .await
            .unwrap();
        Connection::new_tls(&addr.to_string(), &addr.to_string(), stream)
    } else {
        Connection::new(socket)
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        partial_rx.await.unwrap();
        let mut first_frame = Box::pin(connection.read_frame());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut first_frame)
                .await
                .is_err()
        );
        continue_tx.send(()).unwrap();
        assert!(matches!(first_frame.await.unwrap(), Some(Frame::Array(_))));
        connection
            .write_frame(&Frame::Simple("PONG".into()))
            .await
            .unwrap();
        if !expected.is_empty() {
            for n in [1, 2] {
                assert!(matches!(
                    connection.read_frame().await.unwrap(),
                    Some(Frame::Integer(value)) if value == n
                ));
                connection.write_frame(&Frame::Integer(n)).await.unwrap();
            }
        } else {
            assert!(connection.read_frame().await.is_err());
            assert!(connection.buffer.len() <= MAX_FRAME_BYTES);
        }
        drop(connection);
    })
    .await
    .unwrap();
    client.await.unwrap();
}

#[tokio::test]
async fn receive_buffer_stops_at_the_frame_byte_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    let mut connection = Connection::new(socket);
    // Leave only one byte in the receive budget; the socket offers two.
    connection.buffer.resize(MAX_FRAME_BYTES - 1, b'+');
    client.write_all(b"xx").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), connection.read_frame())
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(connection.buffer.len(), MAX_FRAME_BYTES);
}

#[tokio::test]
async fn tcp_and_tls_share_fragment_pipeline_and_rejection_behavior() {
    for tls in [false, true] {
        exchange(tls, b":1\r\n:2\r\n".to_vec(), b":1\r\n:2\r\n").await;
        for payload in [
            b"$1\r\naXX".to_vec(),
            b":12junk\r\n".to_vec(),
            format!("${}\r\n", frame::MAX_BULK_BYTES + 1).into_bytes(),
            format!("*{}\r\n", frame::MAX_ARRAY_ELEMENTS + 1).into_bytes(),
            [b"*1\r\n".repeat(frame::MAX_ARRAY_DEPTH), b"*".to_vec()].concat(),
        ] {
            exchange(tls, payload, b"").await;
        }
        // A malformed peer must not affect a fresh connection.
        exchange(tls, b":1\r\n:2\r\n".to_vec(), b":1\r\n:2\r\n").await;
    }
}
