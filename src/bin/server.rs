use tikv_service::{server, DEFAULT_PORT, set_instance_id, do_async_raw_connect, do_async_connect, PrometheusServer};
use tikv_service::{Config, set_global_config};

use structopt::StructOpt;
use async_std::net::TcpListener;
use tokio::signal;
use std::fs;
use std::process::exit;

#[tokio::main]
pub async fn main() -> tikv_service::Result<()> {
    // enable logging
    // see https://docs.rs/tracing for more info
    tracing_subscriber::fmt::try_init()?;

    let cli = Cli::from_args();

    let mut config: Option<Config> = None;

    match cli.config {
        Some(config_file_name) => {
            let config_content = fs::read_to_string(config_file_name)
            .expect("Failed to read config file");

            println!("{:?}", config_content);

            // deserialize toml config
            config = match toml::from_str(&config_content) {
                Ok(d) => Some(d),
                Err(e) => {
                    println!("Unable to load config file {}", e.to_string());
                    exit(1);
                }
            };
        },
        None => (),
    };

    match &config {
        Some(c) => {
            println!("{:?}", c);
            set_global_config(c.clone())
        }
        None => (),
    }

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

    //do_async_raw_connect(addrs).await?;
    //do_async_txn_connect(addrs).await?;
    do_async_connect(addrs).await?;

    let server = PrometheusServer::new(format!("{}:{}", &prom_listen, prom_port), instance_id as i64);
    tokio::spawn(async move {
        server.run().await;
    });

    println!("TiKV Service Server Listen on: {}:{}", &listen_addr, port);
    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("{}:{}", &listen_addr, port)).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

#[derive(StructOpt, Debug)]
#[structopt(name = "tikv-service-server", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A service layer for TiKV")]
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

    #[structopt(name = "config", long = "--config")]
    config: Option<String>,
}

