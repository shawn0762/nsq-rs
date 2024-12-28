use std::sync::Arc;

use dashmap::{DashMap, Map};
use tokio::select;
use tokio_util::task::TaskTracker;
use tracing::error;

use super::{channel::Channel, message::Message, nsqd::NSQD};

pub(super) struct Topic {
    name: String,

    channel_map: Arc<DashMap<String, Channel>>,

    mem_msg_tx: async_channel::Sender<Message>,

    nsqd: Arc<NSQD>,
}

impl Topic {
    pub fn new(name: String, nsqd: Arc<NSQD>) -> Self {
        let (mem_msg_tx, mem_msg_rx) = async_channel::bounded(nsqd.get_opts().mem_queue_size);

        let channel_map: Arc<DashMap<String, Channel>> = Arc::new(DashMap::new());
        let channel_map_cp = channel_map.clone();

        let mut shutdown = nsqd.shutdown_rx();
        nsqd.tracker().spawn(async move {
            loop {
                select! {
                    msg = mem_msg_rx.recv() => {
                        for mut c in channel_map_cp.iter_mut() {
                            // 消息内部的body并没有发生复制
                            let Ok(msg) = msg.clone() else {
                                error!("Failed to clone message");
                                break;
                            };
                            c.put_msg(msg);
                        }
                    }
                    _ = shutdown.recv() => {
                        break;
                    }
                }
            }

            // TODO: 准备退出
        });

        Self {
            name,
            channel_map,
            mem_msg_tx,
            nsqd,
        }
    }

    async fn msg_pump(&mut self) {}
}
