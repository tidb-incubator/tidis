use tikv_service::{
    config_instance_id_or_default, config_listen_or_default, config_pd_addrs_or_default,
    config_port_or_default, config_prometheus_listen_or_default, config_prometheus_port_or_default,
    config_tls_auth_client_or_default, config_tls_ca_cert_file_or_default,
    config_tls_cert_file_or_default, config_tls_key_file_or_default, config_tls_listen_or_default,
    config_tls_port_or_default, do_async_connect, server, set_global_config, set_instance_id,
    utils, Config, PrometheusServer,
};

use slog::info;

use async_std::net::TcpListener;
use std::fs;
use std::process::exit;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::signal;

use async_tls::TlsAcceptor;

#[tokio::main]
pub async fn main() -> tikv_service::Result<()> {
    let cli = Cli::from_args();

    let mut config: Option<Config> = None;

    match cli.config {
        Some(config_file_name) => {
            let config_content =
                fs::read_to_string(config_file_name).expect("Failed to read config file");

            // deserialize toml config
            config = match toml::from_str(&config_content) {
                Ok(d) => Some(d),
                Err(e) => {
                    println!("Unable to load config file {}", e.to_string());
                    exit(1);
                }
            };
        }
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
    let c_tls_port = config_tls_port_or_default();
    let tls_port = cli.tls_port.as_deref().unwrap_or(&c_tls_port);
    let c_tls_listen = config_tls_listen_or_default();
    let tls_listen_addr = cli.tls_listen_addr.as_deref().unwrap_or(&c_tls_listen);
    let c_tls_cert_file = config_tls_cert_file_or_default();
    let tls_cert_file = cli.tls_cert_file.as_deref().unwrap_or(&c_tls_cert_file);
    let c_tls_key_file = config_tls_key_file_or_default();
    let tls_key_file = cli.tls_key_file.as_deref().unwrap_or(&c_tls_key_file);
    let c_tls_auth_client = config_tls_auth_client_or_default();
    let tls_auth_client = cli.tls_auth_client.unwrap_or(c_tls_auth_client);
    let c_tls_ca_cert_file = config_tls_ca_cert_file_or_default();
    let tls_ca_cert_file = cli
        .tls_ca_cert_file
        .as_deref()
        .unwrap_or(&c_tls_ca_cert_file);
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

    let server = PrometheusServer::new(
        format!("{}:{}", &prom_listen, prom_port),
        instance_id as i64,
    );
    tokio::spawn(async move {
        server.run().await;
    });

    let mut listener = None;
    let mut tls_listener = None;
    let mut tls_acceptor = None;
    if port != "0" {
        info!(
            tikv_service::config::LOGGER,
            "TiKV Service Server Listen on: {}:{}", &listen_addr, port
        );
        // Bind a TCP listener
        listener = Some(TcpListener::bind(&format!("{}:{}", &listen_addr, port)).await?);
    }

    if tls_port != "0" && tls_cert_file != "" && tls_cert_file != "" {
        info!(
            tikv_service::config::LOGGER,
            "TiKV Service Server SSL Listen on: {}:{}", &tls_listen_addr, tls_port
        );
        tls_listener =
            Some(TcpListener::bind(&format!("{}:{}", &tls_listen_addr, tls_port)).await?);
        let tls_config = utils::load_config(
            &tls_cert_file,
            &tls_key_file,
            tls_auth_client,
            &tls_ca_cert_file,
        )?;
        tls_acceptor = Some(TlsAcceptor::from(Arc::new(tls_config)));
    }

    server::run(listener, tls_listener, tls_acceptor, signal::ctrl_c()).await;

    Ok(())
}

#[derive(StructOpt, Debug)]
#[structopt(name = "tikv-service-server", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A service layer for TiKV")]
struct Cli {
    #[structopt(name = "listen", long = "--listen")]
    listen_addr: Option<String>,

    #[structopt(name = "port", long = "--port")]
    port: Option<String>,

    #[structopt(name = "tls_listen", long = "--tls_listen")]
    tls_listen_addr: Option<String>,

    #[structopt(name = "tls_key_file", long = "--tls_key_file")]
    tls_key_file: Option<String>,

    #[structopt(name = "tls_cert_file", long = "--tls_cert_file")]
    tls_cert_file: Option<String>,

    #[structopt(name = "tls_port", long = "--tls_port")]
    tls_port: Option<String>,

    #[structopt(name = "tls_auth_client", long = "--tls_auth_client")]
    tls_auth_client: Option<bool>,

    #[structopt(name = "tls_ca_cert_file", long = "--tls_ca_cert_file")]
    tls_ca_cert_file: Option<String>,

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
