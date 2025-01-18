mod backend_queue;
mod channel;
mod client_v2;
mod guid;
mod message;
mod nsqd;
mod options;
mod protocol;
mod protocol_v2;
mod shutdown;
mod tcp_server;
mod test;
mod topic;
use client_v2::{ClientV2, SubscriberV2};
pub use message::{Message, MessageID};

pub(crate) enum Client {
    V2(ClientV2),
    SubV2(SubscriberV2),
}

impl Client {
    pub fn id(&self) -> i64 {
        match self {
            Client::V2(c) => c.id(),
            Client::SubV2(c) => c.id(),
        }
    }

    pub fn close(&mut self) {
        match self {
            Client::V2(c) => c.close(),
            Client::SubV2(c) => c.close(),
        };
    }
    pub async fn serve(&mut self) {
        match self {
            Client::V2(c) => c.serve().await,
            Client::SubV2(c) => c.serve().await,
        };
    }
}
