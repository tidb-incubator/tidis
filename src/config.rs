use serde::Deserialize;

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
}
#[derive(Debug, Deserialize, Clone)]
struct Backend {
    use_txn_api: Option<bool>,
    use_async_commit: Option<bool>,
    try_one_pc_commit: Option<bool>,
}

// Config
pub static mut SERVER_CONFIG: Option<Config> = None;

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