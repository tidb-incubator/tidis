use serde::Deserialize;

use crate::DEFAULT_PORT;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    server: Server,
    backend: Backend,
}
#[derive(Debug, Deserialize, Clone)]
struct Server {
    listen: Option<String>,
    port: Option<u16>,
    pd_addrs: Option<String>,
    instance_id: Option<String>,
    prometheus_listen: Option<String>,
    prometheus_port: Option<u16>,
    // username: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct Backend {
    use_txn_api: Option<bool>,
    use_async_commit: Option<bool>,
    try_one_pc_commit: Option<bool>,
    use_pessimistic_txn: Option<bool>,
    local_pool_number: Option<usize>,
}

// Config
pub static mut SERVER_CONFIG: Option<Config> = None;

pub fn is_auth_enabled() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(s) = c.server.password.clone() {
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
                return b
            }
        }
        false
    }
}

pub fn is_use_async_commit() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.use_async_commit {
                return b
            }
        }
        false
    }
}

pub fn is_try_one_pc_commit() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.try_one_pc_commit {
                return b
            }
        }
        false
    }
}

pub fn is_use_pessimistic_txn() -> bool {
    unsafe {
        if let Some(c) = &SERVER_CONFIG {
            if let Some(b) = c.backend.use_pessimistic_txn {
                return b
            }
        }
        // default use pessimistic txn mode
        true
    }
}