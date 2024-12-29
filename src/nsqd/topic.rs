use std::{mem, sync::Arc};

use dashmap::DashMap;
use tokio::{
    select,
    sync::broadcast::{self, error::SendError},
};
use tokio_util::task::TaskTracker;
use tracing::{debug, error};

use crate::{common::Result, errors::NsqError};

use super::{channel::Channel, message::Message, nsqd::NSQD};

pub(super) struct Topic {
    name: String,

    channel_map: Arc<DashMap<String, Channel>>,

    // 向所有Channel广播消息
    mem_msg_tx: broadcast::Sender<Message>,

    nsqd: Arc<NSQD>,
    // exit_tx: async_channel::Sender<()>,
    // exit_rx: async_channel::Receiver<()>,
}

impl Topic {
    pub fn new(name: String, nsqd: Arc<NSQD>) -> Self {
        let (mem_msg_tx, _) = broadcast::channel(nsqd.get_opts().mem_queue_size);
        let channel_map: Arc<DashMap<String, Channel>> = Arc::new(DashMap::new());

        Self {
            name,
            channel_map,
            mem_msg_tx,
            nsqd,
        }
    }

    pub fn add_channel(&mut self, name: String) -> Result<()> {
        if self.channel_map.contains_key(&name) {
            return Ok(());
        }

        self.channel_map.insert(
            name.clone(),
            Channel::new(name, self.nsqd.clone(), self.mem_msg_tx.subscribe()),
        );

        Ok(())
    }

    pub fn close(&mut self) {
        // 通知所有Channel退出，如果可以drop掉Sender，就能够实现
        // 但是这里不能drop
        // 销毁原来那个sender，让所有channel退出
        let (tmp_tx, _) = broadcast::channel(1);
        self.mem_msg_tx = tmp_tx;

        for mut c in self.channel_map.iter_mut() {
            c.close();
        }
    }

    async fn msg_pump(&mut self) {}

    pub fn put_msg(&mut self, msg: Message) -> Result<()> {
        match self.mem_msg_tx.send(msg) {
            Ok(num) => {
                // 这里并不能保证所有channel都能接收到msg，
                // 因为接收端可能在接收msg前就drop了。
                // 又或者某个Receiver长时间没有recv，当堆积在通道中的msg超出容量时，
                // 最先发送的msg将会被丢弃，此后该Receiver再也无法recv到这些被丢弃的msg。
                debug!("Message has sent to {num} channels");
                Ok(())
            }
            Err(SendError(_msg)) => {
                // 如果连一个Receiver都没有，则发送失败
                // TODO: writeMessageToBackend
                // Err(NsqError::TopicMsgSendError(msg))
                Ok(())
            }
        }
    }
}
