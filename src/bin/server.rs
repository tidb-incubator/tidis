//! mini-redis server.
//!
//! This file is the entry point for the server implemented in the library. It
//! performs command line parsing and passes the arguments on to
//! `mini_redis::server`.
//!
//! The `clap` crate is used for parsing arguments.

use mini_redis::{server, DEFAULT_PORT, set_instance_id, do_async_raw_connect};

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
    let pd_addrs = cli.pd_addrs.as_deref().unwrap_or("127.0.0.1:2379");
    let instance_id_str = cli.instance_id.as_deref().unwrap_or("1");
    match instance_id_str.parse::<u64>() {
        Ok(val) => set_instance_id(val),
        Err(_) => set_instance_id(0),
    };
    let mut addrs: Vec<String> = Vec::new();
    pd_addrs.split(",").for_each(|s| {
        addrs.push(s.to_string());
    });
    do_async_raw_connect(addrs).await?;

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

#[derive(StructOpt, Debug)]
#[structopt(name = "mini-redis-server", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A Redis server")]
struct Cli {
    #[structopt(name = "port", long = "--port")]
    port: Option<String>,

    #[structopt(name = "pdaddrs", long = "--pdaddrs")]
    pd_addrs: Option<String>,

    #[structopt(name = "instid", long = "--instid")]
    instance_id: Option<String>,
}
