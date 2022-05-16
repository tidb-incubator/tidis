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

extern crate thiserror;
extern crate hyper;

pub mod cmd;
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
pub use tikv::set_instance_id;
pub use tikv::do_async_raw_connect;
pub use tikv::do_async_txn_connect;
pub use tikv::do_async_connect;

mod utils;

pub mod config;
pub use config::Config;
pub use config::set_global_config;
pub use config::get_global_config;
pub use config::is_try_one_pc_commit;
pub use config::is_use_async_commit;
pub use config::is_use_txn_api;
pub use config::is_use_pessimistic_txn;
pub use config::config_instance_id_or_default;
pub use config::config_listen_or_default;
pub use config::config_pd_addrs_or_default;
pub use config::config_port_or_default;
pub use config::config_prometheus_listen_or_default;
pub use config::config_prometheus_port_or_default;
pub use config::config_local_pool_number;
pub use config::is_auth_enabled;
pub use config::is_auth_matched;
pub use config::txn_retry_count;
pub use config::txn_region_backoff_delay_ms;
pub use config::txn_region_backoff_delay_attemps;
pub use config::txn_lock_backoff_delay_ms;
pub use config::txn_lock_backoff_delay_attemps;

/// Default port that a redis server listens on.
///
/// Used if no port is specified.
pub const DEFAULT_PORT: &str = "6379";

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