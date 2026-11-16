use super::*;
use async_std::io::{ReadExt, WriteExt};
use rustls::{ClientConfig, ClientSession, Session, StreamOwned};
use std::io::{BufReader, Read, Write};
use std::net::SocketAddr;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

// Public test identity, never used outside these loopback-only tests.
const IDENTITY: &[u8] = include_bytes!("testdata/tls.pem");

type Clients = Arc<Mutex<HashMap<u64, Arc<Mutex<Client>>>>>;

struct TestServer {
    clients: Clients,
    limit_handshakes: Arc<Semaphore>,
    addr: SocketAddr,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

async fn start(limit: usize, timeout: Duration) -> TestServer {
    let certs = rustls::internal::pemfile::certs(&mut BufReader::new(IDENTITY)).unwrap();
    let mut keys =
        rustls::internal::pemfile::rsa_private_keys(&mut BufReader::new(IDENTITY)).unwrap();
    let mut config = rustls::ServerConfig::new(rustls::NoClientAuth::new());
    config.set_single_cert(certs, keys.remove(0)).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (notify, _) = broadcast::channel(1);
    let (complete_tx, complete_rx) = mpsc::channel(1);
    let server = TlsListener {
        db_holder: DbDropGuard::new(),
        topo_holder: Cluster::build_myself(&addr.to_string()),
        clients: Arc::new(Mutex::new(HashMap::new())),
        tls_listener: listener,
        tls_acceptor: TlsAcceptor::from(config),
        limit_handshakes: Arc::new(Semaphore::new(limit)),
        handshake_timeout: timeout,
        tls_notify_shutdown: notify,
        tls_shutdown_complete_tx: complete_tx,
        tls_shutdown_complete_rx: complete_rx,
    };
    let clients = server.clients.clone();
    let limit_handshakes = server.limit_handshakes.clone();
    let (stop_tx, stop_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut server = server;
        tokio::select! {
            result = server.run() => result.unwrap(),
            _ = stop_rx => {}
        }
        drop(server.tls_notify_shutdown);
        drop(server.tls_shutdown_complete_tx);
        time::timeout(
            Duration::from_secs(2),
            server.tls_shutdown_complete_rx.recv(),
        )
        .await
        .expect("handshake/handler tasks failed to drain");
    });
    TestServer {
        clients,
        limit_handshakes,
        addr,
        stop: stop_tx,
        task,
    }
}

fn client_session() -> ClientSession {
    let mut config = ClientConfig::new();
    let certs = rustls::internal::pemfile::certs(&mut BufReader::new(IDENTITY)).unwrap();
    config.root_store.add(&certs[0]).unwrap();
    ClientSession::new(
        &Arc::new(config),
        webpki::DNSNameRef::try_from_ascii_str("localhost").unwrap(),
    )
}

async fn ping(addr: SocketAddr) -> StreamOwned<ClientSession, std::net::TcpStream> {
    tokio::task::spawn_blocking(move || {
        let socket = std::net::TcpStream::connect(addr).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut client = StreamOwned::new(client_session(), socket);
        client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        client.flush().unwrap();
        let mut reply = [0; 7];
        client.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"+PONG\r\n");
        client
    })
    .await
    .unwrap()
}

async fn closed(socket: &mut TcpStream) {
    // A partial TLS exchange can leave ServerHello/alert bytes to drain first.
    let mut buf = Vec::new();
    let _ = time::timeout(Duration::from_secs(5), socket.read_to_end(&mut buf))
        .await
        .expect("socket was left open");
}

async fn registry_empty(clients: &Clients) {
    time::timeout(Duration::from_secs(2), async {
        while !clients.lock().await.is_empty() {
            time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("client registry leaked");
}

#[tokio::test]
async fn stalled_and_invalid_handshakes_do_not_block_or_register() {
    let TestServer {
        clients,
        addr,
        stop,
        task,
        ..
    } = start(2, Duration::from_secs(3)).await;
    let mut stalled = TcpStream::connect(addr).await.unwrap();
    let established = ping(addr).await;
    assert_eq!(clients.lock().await.len(), 1);
    // An established connection must release its slot while it stays open.
    drop(ping(addr).await);
    drop(established);
    registry_empty(&clients).await;
    closed(&mut stalled).await; // The configured deadline closes a silent peer.
    for _ in 0..32 {
        let mut invalid = TcpStream::connect(addr).await.unwrap();
        invalid.write_all(b"not a TLS record").await.unwrap();
        closed(&mut invalid).await;
        assert!(clients.lock().await.is_empty());
    }
    let _established = ping(addr).await; // Failure/timeout both release slots.
    stop.send(()).unwrap();
    task.await.unwrap();
    registry_empty(&clients).await;
}

#[tokio::test]
async fn handshake_limit_and_shutdown_cover_pending_and_queued_peers() {
    let TestServer {
        clients,
        limit_handshakes,
        addr,
        stop,
        task,
    } = start(1, Duration::from_secs(30)).await;
    let mut first = TcpStream::connect(addr).await.unwrap();
    let mut queued = TcpStream::connect(addr).await.unwrap();
    let mut hello = Vec::new();
    client_session().write_tls(&mut hello).unwrap();
    first.write_all(&hello).await.unwrap();
    let mut reply = [0; 1];
    time::timeout(Duration::from_secs(2), first.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    // The first peer has reached TLS processing but has not finished.
    queued.write_all(&hello).await.unwrap();
    assert!(
        time::timeout(Duration::from_millis(100), queued.read(&mut reply))
            .await
            .is_err()
    );
    assert_eq!(limit_handshakes.available_permits(), 0);
    assert!(clients.lock().await.is_empty());
    drop(first);
    time::timeout(Duration::from_secs(2), queued.read_exact(&mut reply))
        .await
        .expect("failed handshake did not release its slot")
        .unwrap();
    stop.send(()).unwrap();
    task.await.unwrap(); // Must finish far sooner than the 30-second deadline.
    closed(&mut queued).await;
    assert_eq!(limit_handshakes.available_permits(), 1);
    registry_empty(&clients).await;
}
