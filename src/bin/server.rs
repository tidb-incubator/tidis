use tikv_service::{
    server,
    set_instance_id,
    do_async_connect,
    PrometheusServer,
    Config,
    set_global_config,
    config_listen_or_default,
    config_port_or_default,
    config_prometheus_listen_or_default,
    config_prometheus_port_or_default,
    config_pd_addrs_or_default,
    config_instance_id_or_default,
};

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

    let c_port = config_port_or_default();
    let port = cli.port.as_deref().unwrap_or(&c_port);
    let c_listen = config_listen_or_default();
    let listen_addr = cli.listen_addr.as_deref().unwrap_or(&c_listen);
    let c_pd_addrs = config_pd_addrs_or_default();
    let pd_addrs = cli.pd_addrs.as_deref().unwrap_or(&c_pd_addrs);
    let c_instance_id = config_instance_id_or_default();
    let instance_id_str = cli.instance_id.as_deref().unwrap_or(&c_instance_id);
    let c_prom_listen = config_prometheus_listen_or_default();
    let prom_listen = cli.prom_listen_addr.as_deref().unwrap_or(&c_prom_listen);
    let c_prom_port = config_prometheus_port_or_default();
    let prom_port = cli.prom_port.as_deref().unwrap_or(&c_prom_port);
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

