use std::sync::Arc;

use tokio::sync::oneshot;
use tracing::debug;

use super::{
    client_v2::{Client, ClientV2},
    message::Message,
    nsqd::NSQD,
};
use crate::common::Result;

const separator_bytes: &str = " ";
const heartbeat_bytes: &str = "_heartbeat_";
const ok_bytes: &str = "OK";

pub(super) enum FrameType {
    Response,
    Error,
    Message,
}

pub(super) struct ProtocolV2 {
    nsqd: Arc<NSQD>,
}

impl ProtocolV2 {
    pub fn new(nsqd: Arc<NSQD>) -> Self {
        Self { nsqd }
    }

    pub fn io_loop(&mut self, c: ClientV2) -> Result<()> {
        // TODO: 等待pump

        Ok(())
    }

    fn message_pump(&self, c: ClientV2, started_chan: oneshot::Sender<()>) {
        //
    }

    pub async fn send_msg(&self, c: &ClientV2, msg: Message) -> Result<()> {
        debug!(
            "PROTOCOL(V2): writing msg({:#?}) to client({:#?}) - {:#?}",
            msg.id,
            c.addr(),
            msg.body
        );

        // TODO:优化Vec的扩容开销
        // TODO:这里发生了多次写入，能不能更直接一点，直接发给用户？
        let mut buf = Vec::new();
        msg.write_to(&mut buf).await?;

        self.send(c, FrameType::Message, &buf);

        Ok(())
    }

    fn send(&self, c: &ClientV2, ft: FrameType, data: &[u8]) {}
}
