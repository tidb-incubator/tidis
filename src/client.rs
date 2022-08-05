use async_std::net::TcpStream;
use std::fmt;
use std::fmt::Formatter;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use tokio::sync::mpsc::Sender;

// reserve id 0
static COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct Client {
    id: u64,
    // name here does not constrain to uniqueness
    name: String,
    fd: RawFd,
    // last command played
    cmd: String,

    local_addr: String,
    peer_addr: String,

    create_time: SystemTime,
    last_interaction: SystemTime,

    kill_tx: Sender<()>,
}

impl Client {
    pub fn new(socket: TcpStream, kill_tx: Sender<()>) -> Client {
        let now = SystemTime::now();
        Client {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            name: "".to_owned(),
            fd: socket.as_raw_fd(),
            cmd: "".to_owned(),
            local_addr: (&socket).local_addr().unwrap().to_string(),
            peer_addr: (&socket).peer_addr().unwrap().to_string(),
            create_time: now,
            last_interaction: now,
            kill_tx,
        }
    }

    pub fn interact(&mut self, cmd_name: &str) {
        self.cmd = cmd_name.to_string();
        self.last_interaction = SystemTime::now();
    }

    pub async fn kill(&self) {
        let _ = self.kill_tx.send(()).await;
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn local_addr(&self) -> &str {
        &self.local_addr
    }
    pub fn peer_addr(&self) -> &str {
        &self.peer_addr
    }

    pub fn age(&self) -> u64 {
        self.create_time.elapsed().unwrap().as_secs()
    }

    pub fn idle(&self) -> u64 {
        self.last_interaction.elapsed().unwrap().as_secs()
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
}

impl fmt::Display for Client {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "id={} addr={} laddr={} fd={} name={} age={} idle={} flags=N \
            db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=10 obl=0 oll=0 omem=0 \
            tot-mem=0 events=r cmd={} user=default redir=-1",
            self.id,
            self.peer_addr,
            self.local_addr,
            self.fd,
            self.name,
            self.age(),
            self.idle(),
            self.cmd
        )
    }
}
