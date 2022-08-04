use tokio::sync::mpsc;

#[derive(Debug)]
pub(crate) struct Kill {
    /// `true` if the kill signal has been received
    killed: bool,

    /// The receive half of the channel used to listen for shutdown.
    kill_rx: mpsc::Receiver<()>,
}

impl Kill {
    /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
    pub(crate) fn new(kill_rx: mpsc::Receiver<()>) -> Kill {
        Kill {
            killed: false,
            kill_rx,
        }
    }

    /// Returns `true` if the shutdown signal has been received.
    pub(crate) fn is_killed(&self) -> bool {
        self.killed
    }

    /// Receive the kill notice, waiting if necessary.
    pub(crate) async fn recv(&mut self) {
        // If the kill signal has already been received, then return
        // immediately.
        if self.killed {
            return;
        }

        // Cannot receive a "lag error" as only one value is ever sent.
        let _ = self.kill_rx.recv().await;

        // Remember that the signal has been received.
        self.killed = true;
    }
}
