use serde::Deserialize;

use crate::{DEFAULT_PORT, DEFAULT_TLS_PORT};

use slog::{self, Drain};
use slog_term;
use std::fs::OpenOptions;

lazy_static! {
    pub static ref LOGGER: slog::Logger = slog::Logger::root(
        slog_term::FullFormat::new(slog_term::PlainSyncDecorator::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(log_file())
                .unwrap()
        ))
        .build()
        .filter_level(slog::Level::from_usize(log_level()).unwrap())
        .fuse(),
        slog::o!()
    );
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
    tls_auth_client: Option<bool>,
    tls_ca_cert_file: Option<String>,
    pd_addrs: Option<String>,
    instance_id: Option<String>,
    prometheus_listen: Option<String>,
    prometheus_port: Option<u16>,
    // username: Option<String>,
    password: Option<String>,
    log_level: Option<String>,
    log_file: Option<String>,
    cluster_broadcast_addr: Option<String>,
    cluster_topology_interval: Option<u64>,
    cluster_topology_expire: Option<u64>,
    meta_key_number: Option<u16>,
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

    cmd_lrem_length_limit: Option<u32>,
    cmd_linsert_length_limit: Option<u32>,

    async_deletion_enabled: Option<bool>,

    async_del_list_threshold: Option<u32>,
    async_del_hash_threshold: Option<u32>,
    async_del_set_threshold: Option<u32>,
    async_del_zset_threshold: Option<u32>,

    async_expire_list_threshold: Option<u32>,
    async_expire_hash_threshold: Option<u32>,
    async_expire_set_threshold: Option<u32>,
    async_expire_zset_threshold: Option<u32>,
}

// Config
pub static mut SERVER_CONFIG: Option<Config> = None;

pub fn is_auth_enabled() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if c.server.password.clone().is_some() {
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
                return s;
            }
        }
    }

    "".to_owned()
}

pub fn config_tls_key_file_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.tls_key_file.clone() {
                return s;
            }
        }
    }

    "".to_owned()
}

pub fn config_tls_auth_client_or_default() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.tls_auth_client {
                return s;
            }
        }
    }

    false
}

pub fn config_tls_ca_cert_file_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.tls_ca_cert_file.clone() {
                return s;
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
            if let Some(s) = c.server.prometheus_port {
                return s.to_string();
            }
        }
    }
    "8080".to_owned()
}

pub fn config_local_pool_number() -> usize {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.backend.local_pool_number {
                return s;
            }
        }
    }
    // default use 8 localset pool to handle connections
    8
}

pub fn config_cluster_broadcast_addr_or_default() -> String {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.cluster_broadcast_addr.clone() {
                return s;
            }
        }
    }
    // use listen addr if broadcast address not set
    format!(
        "{}:{}",
        config_listen_or_default(),
        config_port_or_default()
    )
}

pub fn config_cluster_topology_interval_or_default() -> u64 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.cluster_topology_interval {
                return s;
            }
        }
    }

    // default update interval set to 10s
    10000
}

pub fn config_cluster_topology_expire_or_default() -> u64 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.cluster_topology_expire {
                return s;
            }
        }
    }

    // default expire set to 30s, 3 times to update interval
    30000
}

pub fn config_meta_key_number_or_default() -> u16 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.meta_key_number {
                return s;
            }
        }
    }

    // default metakey split number
    100
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

pub fn cmd_lrem_length_limit_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.cmd_lrem_length_limit {
                return b;
            }
        }
    }
    // default lrem length no limit
    0
}

pub fn cmd_linsert_length_limit_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.cmd_linsert_length_limit {
                return b;
            }
        }
    }
    // default linsert length no limit
    0
}

pub fn async_del_list_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_del_list_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_del_hash_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_del_hash_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_del_set_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_del_set_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_del_zset_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_del_zset_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_expire_list_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_expire_list_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_expire_hash_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_expire_hash_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_expire_set_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_expire_set_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_expire_zset_threshold_or_default() -> u32 {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_expire_zset_threshold {
                return b;
            }
        }
    }
    if async_deletion_enabled_or_default() {
        1000
    } else {
        u32::MAX
    }
}

pub fn async_deletion_enabled_or_default() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.async_deletion_enabled {
                return b;
            }
        }
    }
    // default async deletion enabled
    true
}
