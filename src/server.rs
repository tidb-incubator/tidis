use crate::cluster::Cluster;
use crate::gc::GcMaster;
use crate::metrics::{
    CURRENT_CONNECTION_COUNTER, CURRENT_TLS_CONNECTION_COUNTER, REQUEST_CMD_COUNTER,
    REQUEST_CMD_ERROR_COUNTER, REQUEST_CMD_FINISH_COUNTER, REQUEST_CMD_HANDLE_TIME,
    REQUEST_COUNTER, TOTAL_CONNECTION_PROCESSED,
};
use crate::tikv::encoding::KeyDecoder;
use crate::tikv::{get_txn_client, KEY_ENCODER};
use crate::utils::{self, resp_err, resp_invalid_arguments, resp_ok, resp_queued, sleep};
use crate::{
    async_gc_worker_number_or_default, config_cluster_broadcast_addr_or_default,
    config_cluster_topology_expire_or_default, config_cluster_topology_interval_or_default,
    config_local_pool_number, is_auth_enabled, is_auth_matched, Command, Connection, Db,
    DbDropGuard, Shutdown,
};
use std::collections::HashMap;

use async_std::net::{TcpListener, TcpStream};
use futures::FutureExt;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use tikv_client::{BoundRange, Key};

use async_std::prelude::StreamExt;
use async_tls::TlsAcceptor;
use rand::Rng;
use slog::{debug, error, info, warn};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::time::{self, Duration, Instant, MissedTickBehavior};

use mlua::{HookTriggers, Lua};

use crate::config::LOGGER;

use crate::client::Client;
use tokio_util::task::LocalPoolHandle;

use crate::tikv::errors::{
    REDIS_AUTH_INVALID_PASSWORD_ERR, REDIS_AUTH_REQUIRED_ERR, REDIS_AUTH_WHEN_DISABLED_ERR,
    REDIS_DISCARD_WITHOUT_MULTI_ERR, REDIS_EXEC_WITHOUT_MULTI_ERR, REDIS_MULTI_NESTED_ERR,
};

use crate::cmd::{script_clear_killed, script_interuptted};

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
#[derive(Debug)]
struct Listener {
    /// Shared database handle.
    ///
    /// Contains the key / value store as well as the broadcast channels for
    /// pub/sub.
    ///
    /// This holds a wrapper around an `Arc`. The internal `Db` can be
    /// retrieved and passed into the per connection state (`Handler`).
    db_holder: DbDropGuard,

    topo_holder: Cluster,
    clients: Arc<Mutex<HashMap<u64, Arc<Mutex<Client>>>>>,

    /// TCP listener supplied by the `run` caller.
    listener: TcpListener,

    /// Limit the max number of connections.
    ///
    /// A `Semaphore` is used to limit the max number of connections. Before
    /// attempting to accept a new connection, a permit is acquired from the
    /// semaphore. If none are available, the listener waits for one.
    ///
    /// When handlers complete processing a connection, the permit is returned
    /// to the semaphore.
    // limit_connections: Arc<Semaphore>,

    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// The initial `shutdown` trigger is provided by the `run` caller. The
    /// server is responsible for gracefully shutting down active connections.
    /// When a connection task is spawned, it is passed a broadcast receiver
    /// handle. When a graceful shutdown is initiated, a `()` value is sent via
    /// the broadcast::Sender. Each active connection receives it, reaches a
    /// safe terminal state, and completes the task.
    notify_shutdown: broadcast::Sender<()>,

    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing.
    ///
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is
    /// leveraged to detect all connection handlers completing. When a
    /// connection handler is initialized, it is assigned a clone of
    /// `shutdown_complete_tx`. When the listener shuts down, it drops the
    /// sender held by this `shutdown_complete_tx` field. Once all handler tasks
    /// complete, all clones of the `Sender` are also dropped. This results in
    /// `shutdown_complete_rx.recv()` completing with `None`. At this point, it
    /// is safe to exit the server process.
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

struct TlsListener {
    db_holder: DbDropGuard,
    topo_holder: Cluster,
    clients: Arc<Mutex<HashMap<u64, Arc<Mutex<Client>>>>>,
    tls_listener: TcpListener,
    tls_acceptor: TlsAcceptor,
    tls_notify_shutdown: broadcast::Sender<()>,
    tls_shutdown_complete_rx: mpsc::Receiver<()>,
    tls_shutdown_complete_tx: mpsc::Sender<()>,
}
#[derive(Debug, Clone)]
struct TopologyManager {
    /// address of this instance
    ///
    /// address will use listen:port if port is non-zero, otherwise tls_listen:tls_port instead
    address: String,

    topo_holder: Cluster,

    interval: u64, // in milliseconds
    expire: u64,   // in milliseconds
}

/// Per-connection handler. Reads requests from `connection` and applies the
/// commands to `db`.
#[derive(Debug)]
struct Handler {
    /// Shared database handle.
    ///
    /// When a command is received from `connection`, it is applied with `db`.
    /// The implementation of the command is in the `cmd` module. Each command
    /// will need to interact with `db` in order to complete the work.
    db: Db,

    topo: Cluster,
    cur_client: Arc<Mutex<Client>>,
    clients: Arc<Mutex<HashMap<u64, Arc<Mutex<Client>>>>>,

    /// The TCP connection decorated with the redis protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    connection: Connection,

    /// The txn state of this connection.
    inner_txn: bool,
    queued_commands: Vec<Command>,

    /// Max connection semaphore.
    ///
    /// When the handler is dropped, a permit is returned to this semaphore. If
    /// the listener is waiting for connections to close, it will be notified of
    /// the newly available permit and resume accepting connections.
    // limit_connections: Arc<Semaphore>,

    /// Listen for shutdown notifications.
    ///
    /// A wrapper around the `broadcast::Receiver` paired with the sender in
    /// `Listener`. The connection handler processes requests from the
    /// connection until the peer disconnects **or** a shutdown notification is
    /// received from `shutdown`. In the latter case, any in-flight work being
    /// processed for the peer is continued until it reaches a safe state, at
    /// which point the connection is terminated.
    shutdown: Shutdown,

    /// Connection authorized.
    /// authorized is true when no requirepass set to the server, otherwise false by default
    /// set authorized to true after `AUTH password` command executed.
    authorized: bool,

    /// Lua vm context, lazy initialized when eval/evalsha called
    lua: Option<Lua>,

    /// Not used directly. Instead, when `Handler` is dropped...?
    _shutdown_complete: mpsc::Sender<()>,
}

/// Maximum number of concurrent connections the redis server will accept.
///
/// When this limit is reached, the server will stop accepting connections until
/// an active connection terminates.
///
/// A real application will want to make this value configurable, but for this
/// example, it is hard coded.
///
/// This is also set to a pretty low value to discourage using this in
/// production (you'd think that all the disclaimers would make it obvious that
/// this is not a serious project... but I thought that about mini-http as
/// well).
// const MAX_CONNECTIONS: usize = 5000;

/// Run the server.
///
/// Accepts connections from the supplied listener. For each inbound connection,
/// a task is spawned to handle that connection. The server runs until the
/// `shutdown` future completes, at which point the server shuts down
/// gracefully.
///
/// `tokio::signal::ctrl_c()` can be used as the `shutdown` argument. This will
/// listen for a SIGINT signal.
pub async fn run(
    listener: Option<TcpListener>,
    tls_listener: Option<TcpListener>,
    tls_acceptor: Option<TlsAcceptor>,
    shutdown: impl Future,
) {
    let tcp_enabled = listener.is_some();
    let tls_enabled = tls_listener.is_some();

    let topo_addr = config_cluster_broadcast_addr_or_default();

    let topo_holder = Cluster::build_myself(&topo_addr);

    // When the provided `shutdown` future completes, we must send a shutdown
    // message to all active connections. We use a broadcast channel for this
    // purpose. The call below ignores the receiver of the broadcast pair, and when
    // a receiver is needed, the subscribe() method on the sender is used to create
    // one.
    let db_holder = DbDropGuard::new();

    let topo_manager = TopologyManager {
        address: topo_addr,
        topo_holder: topo_holder.clone(),
        interval: config_cluster_topology_interval_or_default(),
        expire: config_cluster_topology_expire_or_default(),
    };

    let mut gc_master = GcMaster::new(async_gc_worker_number_or_default(), topo_holder.clone());
    gc_master.start_workers().await;

    if tcp_enabled && !tls_enabled {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

        // Initialize the listener state
        let mut server = Listener {
            listener: listener.unwrap(),
            db_holder: db_holder.clone(),
            topo_holder: topo_holder.clone(),
            clients: Arc::new(Mutex::new(HashMap::new())),
            // limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
            notify_shutdown,
            shutdown_complete_tx,
            shutdown_complete_rx,
        };

        tokio::select! {
            res = server.run() => {
                if let Err(err) = res {
                    error!(LOGGER, "failed to accept, cause {}", err.to_string());
                }
            }
            _ = topo_manager.run() => {
                error!(LOGGER, "topology manager exit");
            }
            _ = gc_master.run() => {
                error!(LOGGER, "gc master exit");
            }
            _ = shutdown => {
                // The shutdown signal has been received.
                info!(LOGGER, "shutting down");
            }
        }

        let Listener {
            mut shutdown_complete_rx,
            shutdown_complete_tx,
            notify_shutdown,
            ..
        } = server;

        drop(notify_shutdown);
        drop(shutdown_complete_tx);

        shutdown_complete_rx.recv().await;
    } else if tls_enabled && !tcp_enabled {
        let (tls_notify_shutdown, _) = broadcast::channel(1);
        let (tls_shutdown_complete_tx, tls_shutdown_complete_rx) = mpsc::channel(1);
        let mut tls_server = TlsListener {
            tls_listener: tls_listener.unwrap(),
            db_holder: db_holder.clone(),
            topo_holder: topo_holder.clone(),
            clients: Arc::new(Mutex::new(HashMap::new())),
            tls_acceptor: tls_acceptor.unwrap(),
            tls_notify_shutdown,
            tls_shutdown_complete_tx,
            tls_shutdown_complete_rx,
        };

        tokio::select! {
            tls_res = tls_server.run() => {
                if let Err(err) = tls_res {
                    error!(LOGGER, "failed to accept, cause {}", err.to_string());
                }
            }
            _ = topo_manager.run() => {
                error!(LOGGER, "topology manager exit");
            }
            _ = gc_master.run() => {
                error!(LOGGER, "gc master exit");
            }
            _ = shutdown => {
                // The shutdown signal has been received.
                info!(LOGGER, "shutting down");
            }
        }

        let TlsListener {
            mut tls_shutdown_complete_rx,
            tls_shutdown_complete_tx,
            tls_notify_shutdown,
            ..
        } = tls_server;

        drop(tls_notify_shutdown);
        drop(tls_shutdown_complete_tx);
        tls_shutdown_complete_rx.recv().await;
    } else if tcp_enabled && tls_enabled {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

        // Initialize the listener state
        let mut server = Listener {
            listener: listener.unwrap(),
            db_holder: db_holder.clone(),
            topo_holder: topo_holder.clone(),
            clients: Arc::new(Mutex::new(HashMap::new())),
            // limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
            notify_shutdown,
            shutdown_complete_tx,
            shutdown_complete_rx,
        };

        let (tls_notify_shutdown, _) = broadcast::channel(1);
        let (tls_shutdown_complete_tx, tls_shutdown_complete_rx) = mpsc::channel(1);
        let mut tls_server = TlsListener {
            tls_listener: tls_listener.unwrap(),
            db_holder: db_holder.clone(),
            topo_holder: topo_holder.clone(),
            clients: Arc::new(Mutex::new(HashMap::new())),
            tls_acceptor: tls_acceptor.unwrap(),
            tls_notify_shutdown,
            tls_shutdown_complete_tx,
            tls_shutdown_complete_rx,
        };

        tokio::select! {
            res = server.run() => {
                if let Err(err) = res {
                    error!(LOGGER, "failed to accept, cause {}", err.to_string());
                }
            }
            tls_res = tls_server.run() => {
                if let Err(err) = tls_res {
                    error!(LOGGER, "failed to accept, cause {}", err.to_string());
                }
            }
            _ = topo_manager.run() => {
                error!(LOGGER, "topology manager exit");
            }
            _ = gc_master.run() => {
                error!(LOGGER, "gc master exit");
            }
            _ = shutdown => {
                // The shutdown signal has been received.
                info!(LOGGER, "shutting down");
            }
        }
        let Listener {
            mut shutdown_complete_rx,
            shutdown_complete_tx,
            notify_shutdown,
            ..
        } = server;

        drop(notify_shutdown);
        drop(shutdown_complete_tx);
        shutdown_complete_rx.recv().await;

        let TlsListener {
            mut tls_shutdown_complete_rx,
            tls_shutdown_complete_tx,
            tls_notify_shutdown,
            ..
        } = tls_server;
        drop(tls_notify_shutdown);
        drop(tls_shutdown_complete_tx);
        tls_shutdown_complete_rx.recv().await;
    } else {
        error!(LOGGER, "no listener enabled for tcp or tls");
    }
}

impl Listener {
    /// Run the server
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    ///
    /// # Errors
    ///
    /// Returns `Err` if accepting returns an error. This can happen for a
    /// number reasons that resolve over time. For example, if the underlying
    /// operating system has reached an internal limit for max number of
    /// sockets, accept will fail.
    ///
    /// The process is not able to detect when a transient error resolves
    /// itself. One strategy for handling this is to implement a back off
    /// strategy, which is what we do here.
    async fn run(&mut self) -> crate::Result<()> {
        info!(LOGGER, "accepting inbound connections");

        let local_pool_number = config_local_pool_number();
        let local_pool = LocalPoolHandle::new(local_pool_number);
        loop {
            // Wait for a permit to become available
            //
            // `acquire` returns a permit that is bound via a lifetime to the
            // semaphore. When the permit value is dropped, it is automatically
            // returned to the semaphore. This is convenient in many cases.
            // However, in this case, the permit must be returned in a different
            // task than it is acquired in (the handler task). To do this, we
            // "forget" the permit, which drops the permit value **without**
            // incrementing the semaphore's permits. Then, in the handler task
            // we manually add a new permit when processing completes.
            //
            // `acquire()` returns `Err` when the semaphore has been closed. We
            // don't ever close the sempahore, so `unwrap()` is safe.
            // self.limit_connections.acquire().await.unwrap().forget();

            // Accept a new socket. This will attempt to perform error handling.
            // The `accept` method internally attempts to recover errors, so an
            // error here is non-recoverable.
            let socket = self.accept().await?;
            let (kill_tx, kill_rx) = mpsc::channel(1);
            let client = Client::new(socket.clone(), kill_tx);
            let client_id = client.id();
            let arc_client = Arc::new(Mutex::new(client));
            self.clients
                .lock()
                .await
                .insert(client_id, arc_client.clone());

            // Create the necessary per-connection handler state.
            let mut handler = Handler {
                // Get a handle to the shared database.
                db: self.db_holder.db(),

                topo: self.topo_holder.clone(),
                cur_client: arc_client.clone(),
                clients: self.clients.clone(),

                // Initialize the connection state. This allocates read/write
                // buffers to perform redis protocol frame parsing.
                connection: Connection::new(socket),

                inner_txn: false,
                queued_commands: vec![],

                // The connection state needs a handle to the max connections
                // semaphore. When the handler is done processing the
                // connection, a permit is added back to the semaphore.
                // limit_connections: self.limit_connections.clone(),

                // Receive shutdown notifications.
                shutdown: Shutdown::new(self.notify_shutdown.subscribe(), kill_rx),

                authorized: !is_auth_enabled(),

                lua: None,

                // Notifies the receiver half once all clones are
                // dropped.
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };
            local_pool.spawn_pinned(|| async move {
                // Process the connection. If an error is encountered, log it.
                CURRENT_CONNECTION_COUNTER.inc();
                TOTAL_CONNECTION_PROCESSED.inc();
                if let Err(err) = handler.run().await {
                    error!(LOGGER, "connection error {:?}", err);
                }
                handler
                    .clients
                    .lock()
                    .await
                    .remove(&handler.cur_client.lock().await.id());
                CURRENT_CONNECTION_COUNTER.dec();
            });
        }
    }

    /// Accept an inbound connection.
    ///
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 1 second.
    /// After the second failure, the task waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    error!(LOGGER, "Accept Error! {:?}", &err);
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}

impl TlsListener {
    async fn run(&mut self) -> crate::Result<()> {
        info!(LOGGER, "accepting inbound tls connections");

        let local_pool_number = config_local_pool_number();
        let local_pool = LocalPoolHandle::new(local_pool_number);

        let mut incoming = self.tls_listener.incoming();
        while let Some(stream) = incoming.next().await {
            let acceptor = self.tls_acceptor.clone();
            let stream = stream?;
            let (kill_tx, kill_rx) = mpsc::channel(1);
            let client = Client::new(stream.clone(), kill_tx);
            let client_id = client.id();
            let arc_client = Arc::new(Mutex::new(client));
            self.clients
                .lock()
                .await
                .insert(client_id, arc_client.clone());

            let local_addr = stream.local_addr().unwrap().to_string();
            let peer_addr = stream.peer_addr().unwrap().to_string();

            // start tls handshake
            let handshake = acceptor.accept(stream);
            // handshake is a future, await to get an encrypted stream back
            let tls_stream = match handshake.await {
                Ok(stream) => stream,
                Err(e) => {
                    error!(
                        LOGGER,
                        "{} -> {} handshake failed, {}",
                        peer_addr,
                        local_addr,
                        e.to_string()
                    );
                    continue;
                }
            };

            let mut handler = Handler {
                db: self.db_holder.db(),
                topo: self.topo_holder.clone(),
                cur_client: arc_client.clone(),
                clients: self.clients.clone(),
                connection: Connection::new_tls(&local_addr, &peer_addr, tls_stream),
                inner_txn: false,
                queued_commands: vec![],
                shutdown: Shutdown::new(self.tls_notify_shutdown.subscribe(), kill_rx),
                authorized: !is_auth_enabled(),
                lua: None,
                _shutdown_complete: self.tls_shutdown_complete_tx.clone(),
            };

            local_pool.spawn_pinned(|| async move {
                // Process the connection. If an error is encountered, log it.
                CURRENT_TLS_CONNECTION_COUNTER.inc();
                TOTAL_CONNECTION_PROCESSED.inc();
                if let Err(err) = handler.run().await {
                    error!(LOGGER, "tls connection error {:?}", err);
                }
                handler
                    .clients
                    .lock()
                    .await
                    .remove(&handler.cur_client.lock().await.id());
                CURRENT_TLS_CONNECTION_COUNTER.dec();
            });
        }

        Ok(())
    }
}

impl TopologyManager {
    async fn run(self) -> crate::Result<()> {
        let mut interval = time::interval(Duration::from_millis(self.interval));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut rng = rand::thread_rng();
        let address = self.address;
        let expire = self.expire;
        let topo_holder = self.topo_holder.clone();
        loop {
            interval.tick().await;

            let mut txn_client = get_txn_client()?;
            // do all work in one txn
            let resp = txn_client
                .exec_in_txn(None, |txn_rc| {
                    let address = address.clone();
                    let expire = expire;
                    let mut topo_holder = topo_holder.clone();
                    async move {
                        let mut txn = txn_rc.lock().await;
                        // refresh myself infomation to backend store
                        let topo_key = KEY_ENCODER.encode_txnkv_cluster_topo(&address);
                        let ttl = utils::timestamp_from_ttl(expire);
                        let topo_value = KEY_ENCODER.encode_txnkv_cluster_topo_value(ttl);
                        txn.put(topo_key, topo_value).await?;

                        // expire stale entries
                        // expire should be configured as 3 times of interval at least
                        let topo_start_key = KEY_ENCODER.encode_txnkv_cluster_topo_start();
                        let topo_end_key = KEY_ENCODER.encode_txnkv_cluster_topo_end();
                        let range: Range<Key> = topo_start_key..topo_end_key;
                        let bound_range: BoundRange = range.into();
                        let iter = txn.scan(bound_range, u32::MAX).await?;

                        let mut remaining_node = Vec::new();

                        for kv in iter {
                            let ts = KeyDecoder::decode_topo_value(&kv.1);
                            let ttl = utils::ttl_from_timestamp(ts);
                            if ttl == 0 {
                                txn.delete(kv.0).await?;
                            } else {
                                let encoded_key: Vec<u8> = kv.0.into();
                                let addr = KeyDecoder::decode_topo_key_addr(&encoded_key);
                                remaining_node.push(String::from_utf8_lossy(addr).to_string());
                            }
                        }

                        // update topology snapshot and build topology in local if needed
                        // check if member changed
                        if topo_holder.cluster_member_changed(&remaining_node) {
                            topo_holder.update_topo(&remaining_node, &address);
                        }

                        Ok(())
                    }
                    .boxed()
                })
                .await;

            match resp {
                Ok(_) => {}
                Err(err) => {
                    warn!(LOGGER, "topology update failed: {}", err);
                }
            }

            // random sleep a small period for jitter tick
            let jitter = rng.gen::<u8>();
            sleep(jitter as u32).await;
        }
    }
}

impl Handler {
    /// Process a single connection.
    ///
    /// Request frames are read from the socket and processed. Responses are
    /// written back to the socket.
    ///
    /// Currently, pipelining is not implemented. Pipelining is the ability to
    /// process more than one request concurrently per connection without
    /// interleaving frames. See for more details:
    /// https://redis.io/topics/pipelining
    ///
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it is terminated.
    async fn run(&mut self) -> crate::Result<()> {
        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            // signal.
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            // If `None` is returned from `read_frame()` then the peer closed
            // the socket. There is no further work to do and the task can be
            // terminated.
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // Convert the redis frame into a command struct. This returns an
            // error if the frame is not a valid redis command or it is an
            // unsupported command.
            let cmd = Command::from_frame(frame)?;
            let cmd_name = cmd.get_name().to_owned();

            {
                let mut w_client = self.cur_client.lock().await;
                w_client.interact(&cmd_name);
            }

            let start_at = Instant::now();
            REQUEST_COUNTER.inc();
            REQUEST_CMD_COUNTER.with_label_values(&[&cmd_name]).inc();

            debug!(
                LOGGER,
                "req {} -> {}, {:?}",
                self.connection.peer_addr(),
                self.connection.local_addr(),
                cmd
            );

            match cmd {
                Command::Auth(c) => {
                    if !c.valid() {
                        self.connection
                            .write_frame(&resp_invalid_arguments())
                            .await?;
                    } else if !is_auth_enabled() {
                        // check password and update connection authorized flag
                        self.connection
                            .write_frame(&resp_err(REDIS_AUTH_WHEN_DISABLED_ERR))
                            .await?;
                    } else if is_auth_matched(c.passwd()) {
                        self.connection.write_frame(&resp_ok()).await?;
                        self.authorized = true;
                    } else {
                        self.connection
                            .write_frame(&resp_err(REDIS_AUTH_INVALID_PASSWORD_ERR))
                            .await?;
                    }
                }
                _ => {
                    if !self.authorized {
                        self.connection
                            .write_frame(&resp_err(REDIS_AUTH_REQUIRED_ERR))
                            .await?;
                    } else {
                        match cmd {
                            Command::Eval(_) | Command::Evalsha(_) => {
                                if self.lua.is_none() {
                                    // initialize the mlua once in same connection
                                    let lua = Lua::new();
                                    // set script interupt handler
                                    lua.set_hook(HookTriggers::every_line(), |_lua, _debug| {
                                        if script_interuptted() {
                                            warn!(
                                                LOGGER,
                                                "Script kiiled by user with SCRIPT KILL..."
                                            );

                                            script_clear_killed();

                                            Err(mlua::Error::RuntimeError(
                                                "Script kiiled by user with SCRIPT KILL..."
                                                    .to_string(),
                                            ))
                                        } else {
                                            Ok(())
                                        }
                                    })
                                    .unwrap();

                                    self.lua = Some(lua);
                                }
                            }
                            Command::Multi(_) => {
                                if self.inner_txn {
                                    self.connection
                                        .write_frame(&resp_err(REDIS_MULTI_NESTED_ERR))
                                        .await?;
                                } else {
                                    self.inner_txn = true;
                                    self.queued_commands.clear();
                                    self.connection.write_frame(&resp_ok()).await?;
                                }
                            }
                            Command::Exec(c) => {
                                if !self.inner_txn {
                                    self.connection
                                        .write_frame(&resp_err(REDIS_EXEC_WITHOUT_MULTI_ERR))
                                        .await?;
                                } else {
                                    self.inner_txn = false;
                                    c.clone()
                                        .exec(&mut self.connection, self.queued_commands.clone())
                                        .await?;
                                }

                                let duration = Instant::now() - start_at;
                                REQUEST_CMD_HANDLE_TIME
                                    .with_label_values(&[&cmd_name])
                                    .observe(duration_to_sec(duration));
                                REQUEST_CMD_FINISH_COUNTER
                                    .with_label_values(&[&cmd_name])
                                    .inc();
                                continue;
                            }
                            Command::Discard(_) => {
                                if self.inner_txn {
                                    self.inner_txn = false;
                                    self.queued_commands.clear();
                                    self.connection.write_frame(&resp_ok()).await?;
                                } else {
                                    self.connection
                                        .write_frame(&resp_err(REDIS_DISCARD_WITHOUT_MULTI_ERR))
                                        .await?;
                                }
                            }
                            _ => {
                                if self.inner_txn {
                                    self.queued_commands.push(cmd);
                                    self.connection.write_frame(&resp_queued()).await?;
                                    continue;
                                }
                            }
                        }
                        // Perform the work needed to apply the command. This may mutate the
                        // database state as a result.
                        //
                        // The connection is passed into the apply function which allows the
                        // command to write response frames directly to the connection. In
                        // the case of pub/sub, multiple frames may be send back to the
                        // peer.
                        match cmd
                            .apply(
                                &self.db,
                                &self.topo,
                                &mut self.connection,
                                self.cur_client.clone(),
                                self.clients.clone(),
                                &mut self.lua,
                                &mut self.shutdown,
                            )
                            .await
                        {
                            Ok(_) => (),
                            Err(e) => {
                                REQUEST_CMD_ERROR_COUNTER
                                    .with_label_values(&[&cmd_name])
                                    .inc();
                                return Err(e);
                            }
                        };
                    }
                }
            }

            let duration = Instant::now() - start_at;
            REQUEST_CMD_HANDLE_TIME
                .with_label_values(&[&cmd_name])
                .observe(duration_to_sec(duration));
            REQUEST_CMD_FINISH_COUNTER
                .with_label_values(&[&cmd_name])
                .inc();
        }

        Ok(())
    }
}

#[inline]
pub fn duration_to_sec(d: Duration) -> f64 {
    let nanos = f64::from(d.subsec_nanos());
    d.as_secs() as f64 + (nanos / 1_000_000_000.0)
}

impl Drop for Handler {
    fn drop(&mut self) {
        // Add a permit back to the semaphore.
        //
        // Doing so unblocks the listener if the max number of
        // connections has been reached.
        //
        // This is done in a `Drop` implementation in order to guarantee that
        // the permit is added even if the task handling the connection panics.
        // If `add_permit` was called at the end of the `run` function and some
        // bug causes a panic. The permit would never be returned to the
        // semaphore.
        // self.limit_connections.add_permits(1);
        // println!("Drop Handler")
        // CURRENT_CONNECTION_COUNTER.dec();
    }
}
