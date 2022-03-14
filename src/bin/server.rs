//! mini-redis server.
//!
//! This file is the entry point for the server implemented in the library. It
//! performs command line parsing and passes the arguments on to
//! `mini_redis::server`.
//!
//! The `clap` crate is used for parsing arguments.

use mini_redis::{server, DEFAULT_PORT, set_instance_id, do_async_raw_connect, PrometheusServer};

use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
pub async fn main() -> mini_redis::Result<()> {
    // enable logging
    // see https://docs.rs/tracing for more info
    tracing_subscriber::fmt::try_init()?;

    let cli = Cli::from_args();
    let port = cli.port.as_deref().unwrap_or(DEFAULT_PORT);
    let listen_addr = cli.listen_addr.as_deref().unwrap_or("0.0.0.0");
    let pd_addrs = cli.pd_addrs.as_deref().unwrap_or("127.0.0.1:2379");
    let instance_id_str = cli.instance_id.as_deref().unwrap_or("1");
    let prom_listen = cli.prom_listen_addr.as_deref().unwrap_or("0.0.0.0");
    let prom_port = cli.prom_port.as_deref().unwrap_or("8080");
    let mut instance_id: u64 = 0;
    match instance_id_str.parse::<u64>() {
        Ok(val) => {
            instance_id = val;
            set_instance_id(val);
        }
        Err(_) => set_instance_id(0),
    };
    let mut addrs: Vec<String> = Vec::new();
    pd_addrs.split(",").for_each(|s| {
        addrs.push(s.to_string());
    });
    do_async_raw_connect(addrs).await?;

    let server = PrometheusServer::new(format!("{}:{}", &prom_listen, prom_port), instance_id as i64);
    tokio::spawn(async move {
        server.run().await;
    });

    println!("Mini-Redis Server Listen on: {}:{}", &listen_addr, port);
    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("{}:{}", &listen_addr, port)).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

#[derive(StructOpt, Debug)]
#[structopt(name = "mini-redis-server", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A Redis server")]
struct Cli {
    #[structopt(name = "listen", long = "--listen")]
    listen_addr: Option<String>,

    #[structopt(name = "port", long = "--port")]
    port: Option<String>,

    #[structopt(name = "pdaddrs", long = "--pdaddrs")]
    pd_addrs: Option<String>,

    #[structopt(name = "instid", long = "--instid")]
    instance_id: Option<String>,

    #[structopt(name = "promlisten", long = "--promlisten")]
    prom_listen_addr: Option<String>,

    #[structopt(name = "promport", long = "--promport")]
    prom_port: Option<String>,
}
