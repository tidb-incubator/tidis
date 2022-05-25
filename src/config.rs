use serde::Deserialize;

use crate::{DEFAULT_PORT, DEFAULT_TLS_PORT};

use slog::{self, Drain};
use slog_term;
use std::fs::OpenOptions;

lazy_static! {
    pub static ref LOGGER: slog::Logger = slog::Logger::root(
        slog_term::FullFormat::new(
            slog_term::PlainSyncDecorator::new(OpenOptions::new()
                                                .create(true)
                                                .write(true)
                                                .truncate(true)
                                                .open(log_file())
                                                .unwrap()
            )
        )
        .build()
        .filter_level(slog::Level::from_usize(log_level()).unwrap())
        .fuse(),
        slog::o!());
        
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    server: Server,
    backend: Backend,
}
#[derive(Debug, Deserialize, Clone)]
struct Server {
    listen: Option<String>,
    port: Option<u16>,
    tls_listen: Option<String>,
    tls_port: Option<u16>,
    tls_key_file: Option<String>,
    tls_cert_file: Option<String>,
    pd_addrs: Option<String>,
    instance_id: Option<String>,
    prometheus_listen: Option<String>,
    prometheus_port: Option<u16>,
    // username: Option<String>,
    password: Option<String>,
    log_level: Option<String>,
    log_file: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct Backend {
    use_txn_api: Option<bool>,
    use_async_commit: Option<bool>,
    try_one_pc_commit: Option<bool>,
    use_pessimistic_txn: Option<bool>,
    local_pool_number: Option<usize>,
    txn_retry_count: Option<u32>,
    txn_region_backoff_delay_ms: Option<u64>,
    txn_region_backoff_delay_attemps: Option<u32>,
    txn_lock_backoff_delay_ms: Option<u64>,
    txn_lock_backoff_delay_attemps: Option<u32>,
}

// Config
pub static mut SERVER_CONFIG: Option<Config> = None;

pub fn is_auth_enabled() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(_) = c.server.password.clone() {
                return true;
            }
        }
    }
    false
}

// return false only if auth is enabled and password mismatch
pub fn is_auth_matched(password: &str) -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.password.clone() {
                return s == password;
            }
        }
    }
    true
}

pub fn txn_retry_count() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.backend.txn_retry_count {
                return s;
            }
        }
    }
    // default to 3
    3
}

pub fn config_listen_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.listen.clone() {
                return s;
            }
        }
    }

    "0.0.0.0".to_owned()
}

pub fn config_port_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.port {
                return s.to_string();
            }
        }
    }

    DEFAULT_PORT.to_owned()
}

pub fn config_tls_listen_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.tls_listen.clone() {
                return s;
            }
        }
    }

    "0.0.0.0".to_owned()
}

pub fn config_tls_port_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.tls_port {
                return s.to_string();
            }
        }
    }

    DEFAULT_TLS_PORT.to_owned()
}

pub fn config_tls_cert_file_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.tls_cert_file.clone() {
                return s.to_string();
            }
        }
    }

    "".to_owned()
}

pub fn config_tls_key_file_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.tls_key_file.clone() {
                return s.to_string();
            }
        }
    }

    "".to_owned()
}

pub fn config_pd_addrs_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.pd_addrs.clone() {
                return s;
            }
        }
    }
    "127.0.0.1:2379".to_owned()
}

pub fn config_instance_id_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.instance_id.clone() {
                return s;
            }
        }
    }
    "1".to_owned()
}


pub fn config_prometheus_listen_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.prometheus_listen.clone() {
                return s;
            }
        }
    }
    "0.0.0.0".to_owned()
}

pub fn config_prometheus_port_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.prometheus_port.clone() {
                return s.to_string();
            }
        }
    }
    "8080".to_owned()
}

pub fn config_local_pool_number() -> usize {
    unsafe{
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.backend.local_pool_number {
                return s;
            }
        }
    }
    // default use 8 localset pool to handle connections
    8
}

fn log_level_str() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(l) = c.server.log_level.clone() {
                return l;
            }
        }
    }
    "info".to_owned()
}

pub fn log_level() -> usize {
    let level_str = log_level_str();
    match level_str.as_str() {
        "off" => 0,
        "critical" => 1,
        "error" => 2,
        "warning" => 3,
        "info" => 4,
        "debug" => 5,
        "trace" => 6,
        _ => 0,
    }
}

pub fn log_file() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(l) = c.server.log_file.clone() {
                return l;
            }
        }
    }
    "tikv-service.log".to_owned()
}

pub fn set_global_config(config: Config) {
    unsafe {
        SERVER_CONFIG.replace(config);
    }
}

pub fn get_global_config() -> &'static Config {
    unsafe { SERVER_CONFIG.as_ref().unwrap() }
}

pub fn is_use_txn_api() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.use_txn_api {
                return b;
            }
        }
        false
    }
}

pub fn is_use_async_commit() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.use_async_commit {
                return b;
            }
        }
        false
    }
}

pub fn is_try_one_pc_commit() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.try_one_pc_commit {
                return b;
            }
        }
        false
    }
}

pub fn is_use_pessimistic_txn() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.use_pessimistic_txn {
                return b;
            }
        }
        // default use pessimistic txn mode
        true
    }
}

pub fn txn_region_backoff_delay_ms() -> u64 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.txn_region_backoff_delay_ms {
                return b;
            }
        }
    }
    2
}

pub fn txn_region_backoff_delay_attemps() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.txn_region_backoff_delay_attemps {
                return b;
            }
        }
    }
    2
}

pub fn txn_lock_backoff_delay_ms() -> u64 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.txn_lock_backoff_delay_ms {
                return b;
            }
        }
    }
    2
}

pub fn txn_lock_backoff_delay_attemps() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.txn_lock_backoff_delay_attemps {
                return b;
            }
        }
    }
    2
}