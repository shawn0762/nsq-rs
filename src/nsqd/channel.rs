use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use dashmap::DashMap;
use tokio::select;
use tracing::{error, info};

use crate::{common::Result, errors::NsqError};

use super::{
    client_v2::{Client, ClientV2, SubscriberV2},
    message::Message,
    nsqd::NSQD,
};

pub struct Channel {
    name: String,
    nsqd: Arc<NSQD>,

    requeue_count: AtomicU64,
    msg_count: AtomicU64,
    timeout_count: AtomicU64,

    // 通过此通道向客户端发送消息
    mem_msg_tx: async_channel::Sender<Message>,
    mem_msg_rx: async_channel::Receiver<Message>,

    clients: DashMap<i64, SubscriberV2>,

    exit_flag: AtomicBool,
}

impl Channel {
    pub fn new(name: String, nsqd: Arc<NSQD>) -> Self {
        let (mem_msg_tx, mem_msg_rx) = async_channel::bounded(nsqd.get_opts().mem_queue_size);
        Self {
            name,
            requeue_count: 0.into(),
            msg_count: 0.into(),
            timeout_count: 0.into(),
            nsqd,
            mem_msg_tx,
            mem_msg_rx,
            clients: DashMap::new(),
            exit_flag: false.into(),
        }
    }

    pub fn add_client(&mut self, c: ClientV2) -> Result<()> {
        // 当客户端开始订阅时，将转换成SucScriber，此后只能进行订阅相关的操作

        if self.exiting() {
            return Err(NsqError::Exiting);
        }

        let id = c.id();
        if self.clients.contains_key(&id) {
            return Ok(());
        }

        let max = self.nsqd.get_opts().max_channel_subscribers;
        if self.clients.len() >= max {
            return Err(NsqError::MaxSubscriberReached(max));
        }

        self.clients
            .insert(id, SubscriberV2::new(c, self.mem_msg_rx.clone()));
        Ok(())
    }

    pub fn exit(&mut self, deleted: bool) -> Result<()> {
        if let Err(_) =
            self.exit_flag
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
        {
            return Err(NsqError::Exiting);
        }

        if deleted {
            info!("CHANNEL({}): deleting", self.name);
            // TODO: since we are explicitly deleting a channel (not just at system exit time)
            //       de-register this from the lookupd
        } else {
            info!("CHANNEL({}): closing", self.name);
        }

        // 只有一个线程能够执行到这里，所以不需要额外上锁
        for mut c in self.clients.iter_mut() {
            c.close();
        }

        if deleted {
            // TODO: empty the queue (deletes the backend files, too)
            //       return c.backend.Delete()
        }

        // write anything leftover to disk
        // TODO: c.flush()
        // return c.backend.Close()

        Ok(())
    }

    pub fn exiting(&mut self) -> bool {
        self.exit_flag.load(Ordering::SeqCst) == true
    }

    pub async fn put_msg(&mut self, msg: Message) -> Result<()> {
        if self.exiting() {
            return Err(NsqError::Exiting);
        }

        self.put(msg).await?;

        self.msg_count.fetch_add(1, Ordering::SeqCst);

        Ok(())
    }

    async fn put(&mut self, msg: Message) -> Result<()> {
        select! {
            ret = self.mem_msg_tx.send(msg) => ret,
            else => {
                // TODO: writeMessageToBackend
                Err(NsqError::DiskQueueNotImplement)
            }
        }
    }
}
