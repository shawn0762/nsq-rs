use tokio::sync::broadcast::{self, Sender};

pub(super) struct Shutdown {
    is_shutdown: bool,
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    pub fn new(notify: broadcast::Receiver<()>) -> Self {
        Self {
            is_shutdown: false,
            notify,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    pub async fn recv(&mut self) {
        if self.is_shutdown {
            return;
        }

        let _ = self.notify.recv().await;
        self.is_shutdown = true;
    }
}

impl From<&Sender<()>> for Shutdown {
    fn from(tx: &Sender<()>) -> Self {
        Self::new(tx.subscribe())
    }
}

impl Clone for Shutdown {
    fn clone(&self) -> Self {
        Self {
            is_shutdown: self.is_shutdown,
            notify: self.notify.resubscribe(),
        }
    }
}
