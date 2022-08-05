use tokio::select;
use tokio::sync::{broadcast, mpsc};

/// Listens for the server shutdown signal.
///
/// Shutdown is signalled using a `broadcast::Receiver`. Only a single value is
/// ever sent. Once a value has been sent via the broadcast channel, the server
/// should shutdown.
///
/// The `Shutdown` struct listens for the signal and tracks that the signal has
/// been received. Callers may query for whether the shutdown signal has been
/// received or not.
#[derive(Debug)]
pub(crate) struct Shutdown {
    /// `true` if the shutdown signal has been received
    shutdown: bool,

    /// The receive half of the channel used to listen for server shutdown.
    notify: broadcast::Receiver<()>,

    /// used to listen for P2P kill signal
    kill_rx: mpsc::Receiver<()>,
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
    pub(crate) fn new(notify: broadcast::Receiver<()>, kill_rx: mpsc::Receiver<()>) -> Shutdown {
        Shutdown {
            shutdown: false,
            notify,
            kill_rx,
        }
    }

    /// Returns `true` if the shutdown signal has been received.
    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    /// Receive the shutdown notice, waiting if necessary.
    pub(crate) async fn recv(&mut self) {
        // If the shutdown signal has already been received, then return
        // immediately.
        if self.shutdown {
            return;
        }

        select! {
            // Cannot receive a "lag error" as only one value is ever sent.
            _ = self.notify.recv() => {
                // server shutdown caused shutdown
                self.shutdown = true;
            },
            _ = self.kill_rx.recv() => {
                // kill signal caused shutdown
                self.shutdown = true;
            }
        };
    }
}
