//! A minimal (i.e. very incomplete) implementation of a Redis server and
//! client.
//!
//! The purpose of this project is to provide a larger example of an
//! asynchronous Rust project built with Tokio. Do not attempt to run this in
//! production... seriously.
//!
//! # Layout
//!
//! The library is structured such that it can be used with guides. There are
//! modules that are public that probably would not be public in a "real" redis
//! client library.
//!
//! The major components are:
//!
//! * `server`: Redis server implementation. Includes a single `run` function
//!   that takes a `TcpListener` and starts accepting redis client connections.
//!
//! * `client`: an asynchronous Redis client implementation. Demonstrates how to
//!   build clients with Tokio.
//!
//! * `cmd`: implementations of the supported Redis commands.
//!
//! * `frame`: represents a single Redis protocol frame. A frame is used as an
//!   intermediate representation between a "command" and the byte
//!   representation.

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;

extern crate hyper;
extern crate thiserror;

pub mod cmd;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;

pub use cmd::Command;

mod connection;
pub use connection::Connection;

pub mod frame;
pub use frame::Frame;

mod db;
use db::Db;
use db::DbDropGuard;

mod parse;
use parse::{Parse, ParseError};

pub mod server;

mod shutdown;
use shutdown::Shutdown;

mod metrics;
pub use metrics::PrometheusServer;

mod tikv;
pub use tikv::do_async_connect;
pub use tikv::do_async_raw_connect;
pub use tikv::do_async_txn_connect;
pub use tikv::set_instance_id;

pub mod cluster;

pub mod client;

pub mod utils;

pub mod config;
pub use config::async_del_hash_threshold_or_default;
pub use config::async_del_list_threshold_or_default;
pub use config::async_del_set_threshold_or_default;
pub use config::async_del_zset_threshold_or_default;
pub use config::async_deletion_enabled_or_default;
pub use config::async_expire_hash_threshold_or_default;
pub use config::async_expire_list_threshold_or_default;
pub use config::async_expire_set_threshold_or_default;
pub use config::async_expire_zset_threshold_or_default;
pub use config::async_gc_interval_or_default;
pub use config::async_gc_worker_number_or_default;
pub use config::async_gc_worker_queue_size_or_default;
pub use config::backend_allow_batch_or_default;
pub use config::backend_ca_file_or_default;
pub use config::backend_cert_file_or_default;
pub use config::backend_completion_queue_size_or_default;
pub use config::backend_grpc_keepalive_time_or_default;
pub use config::backend_grpc_keepalive_timeout_or_default;
pub use config::backend_key_file_or_default;
pub use config::backend_max_batch_size_or_default;
pub use config::backend_max_batch_wait_time_or_default;
pub use config::backend_max_inflight_requests_or_default;
pub use config::backend_overload_threshold_or_default;
pub use config::backend_timeout_or_default;
pub use config::cmd_linsert_length_limit_or_default;
pub use config::cmd_lrem_length_limit_or_default;
pub use config::config_cluster_broadcast_addr_or_default;
pub use config::config_cluster_topology_expire_or_default;
pub use config::config_cluster_topology_interval_or_default;
pub use config::config_instance_id_or_default;
pub use config::config_listen_or_default;
pub use config::config_local_pool_number;
pub use config::config_meta_key_number_or_default;
pub use config::config_pd_addrs_or_default;
pub use config::config_port_or_default;
pub use config::config_prometheus_listen_or_default;
pub use config::config_prometheus_port_or_default;
pub use config::config_tls_auth_client_or_default;
pub use config::config_tls_ca_cert_file_or_default;
pub use config::config_tls_cert_file_or_default;
pub use config::config_tls_key_file_or_default;
pub use config::config_tls_listen_or_default;
pub use config::config_tls_port_or_default;
pub use config::conn_concurrency_or_default;
pub use config::get_global_config;
pub use config::is_auth_enabled;
pub use config::is_auth_matched;
pub use config::is_try_one_pc_commit;
pub use config::is_use_async_commit;
pub use config::is_use_pessimistic_txn;
pub use config::is_use_txn_api;
pub use config::set_global_config;
pub use config::txn_lock_backoff_delay_attemps;
pub use config::txn_lock_backoff_delay_ms;
pub use config::txn_region_backoff_delay_attemps;
pub use config::txn_region_backoff_delay_ms;
pub use config::txn_retry_count;
pub use config::Config;

pub mod gc;

use rand::{rngs::SmallRng, Rng, SeedableRng};

/// Default port that a redis server listens on.
///
/// Used if no port is specified.
pub const DEFAULT_PORT: &str = "6379";
pub const DEFAULT_TLS_PORT: &str = "6443";

/// Error returned by most functions.
///
/// When writing a real application, one might want to consider a specialized
/// error handling crate or defining an error type as an `enum` of causes.
/// However, for our example, using a boxed `std::error::Error` is sufficient.
///
/// For performance reasons, boxing is avoided in any hot path. For example, in
/// `parse`, a custom error `enum` is defined. This is because the error is hit
/// and handled during normal execution when a partial frame is received on a
/// socket. `std::error::Error` is implemented for `parse::Error` which allows
/// it to be converted to `Box<dyn std::error::Error>`.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// This is defined as a convenience.
pub type Result<T> = std::result::Result<T, Error>;

lazy_static! {
    pub static ref INDEX_COUNT: AtomicU16 =
        AtomicU16::new(SmallRng::from_entropy().gen_range(0..u16::MAX));
}

pub fn fetch_idx_and_add() -> u16 {
    // fetch_add wraps around on overflow, see https://github.com/rust-lang/rust/issues/34618
    INDEX_COUNT.fetch_add(1, Ordering::Relaxed)
}
