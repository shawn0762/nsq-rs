use std::{
    collections::{BinaryHeap, HashMap},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use tokio::{
    select,
    sync::broadcast::{self, error::RecvError, Receiver},
};
use tokio_util::task::TaskTracker;
use tracing::{error, info};

use crate::{common::Result, errors::NsqError};

use super::{
    client_v2::SubscriberV2,
    message::{Message, MsgItem},
    options::Options,
    Client, MessageID,
};

pub struct Channel {
    name: String,
    opts: Arc<Options>,

    requeue_count: AtomicU64,
    msg_count: AtomicU64,
    timeout_count: AtomicU64,

    // 通过此通道向客户端发送消息
    mem_msg_tx: async_channel::Sender<Message>,
    mem_msg_rx: async_channel::Receiver<Message>,

    exit_flag: AtomicBool,
    task_tracker: TaskTracker,
    state: Mutex<State>,
}

struct State {
    clients: DashMap<i64, Client>,
    // pq stands for priority queue
    defered_pq: BinaryHeap<MsgItem>,
    defered_msgs: HashMap<MessageID, MsgItem>,
}

impl Channel {
    pub fn new(
        name: String,
        opts: Arc<Options>,
        // client: Client,
    ) -> Self {
        let (mem_msg_tx, mem_msg_rx) = async_channel::bounded(opts.mem_queue_size);
        let task_tracker = TaskTracker::new();
        let exit_flag = AtomicBool::new(false);

        // task_tracker.spawn(recv_from_topin(
        //     topic_msg_rx,
        //     mem_msg_tx.clone(),
        //     mem_msg_rx.clone(),
        // ));

        let clients = DashMap::new();
        // clients.insert(client.id(), client);

        let defered_pq = BinaryHeap::new();
        let defered_msgs = HashMap::new();

        let state = Mutex::new(State {
            clients,
            defered_pq,
            defered_msgs,
            // topic_msg_rx,
        });

        Self {
            name,
            requeue_count: 0.into(),
            msg_count: 0.into(),
            timeout_count: 0.into(),
            opts,
            mem_msg_tx,
            mem_msg_rx,
            exit_flag,
            task_tracker,
            state,
        }
    }

    pub fn add_client(&self, c: Client) -> Result<()> {
        // 当客户端开始订阅时，将转换成Subscriber，此后只能进行订阅相关的操作

        if self.exiting() {
            return Err(NsqError::Exiting);
        }

        let state = self.state.lock().unwrap();

        let id = c.id();
        if state.clients.contains_key(&id) {
            return Ok(());
        }

        let max = self.opts.max_channel_subscribers;
        if state.clients.len() >= max {
            return Err(NsqError::MaxSubscriberReached(max));
        }

        state.clients.insert(
            id,
            Client::SubV2(SubscriberV2::new(c, self.mem_msg_rx.clone())),
        );
        Ok(())
    }

    pub fn close(&self) {
        // TODO:
    }

    pub fn exit(&self, deleted: bool) -> Result<()> {
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
        for mut c in self.state.lock().unwrap().clients.iter_mut() {
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

    pub fn exiting(&self) -> bool {
        self.exit_flag.load(Ordering::SeqCst) == true
    }

    pub fn pop_defered_msg(&mut self, curr: u128) -> Option<Message> {
        let mut pq = self.state.lock().unwrap();
        match pq.defered_pq.peek() {
            Some(m) if m.0 <= curr => Some(pq.defered_pq.pop().unwrap().1),
            _ => None,
        }
    }

    pub fn push_defered_msg(&self, msg: Message) -> Result<()> {
        let Some(t) = SystemTime::now().checked_add(msg.get_defered()) else {
            return Err(NsqError::FatalClientErr(
                "E_INVALID".into(),
                "The defer time is too large".into(),
            ));
        };

        let msg_item = MsgItem(t.duration_since(UNIX_EPOCH).unwrap().as_nanos(), msg);

        self.state.lock().unwrap().defered_pq.push(msg_item);
        Ok(())
    }

    pub async fn serve(self: &Arc<Self>, mut topic_msg_rx: broadcast::Receiver<Message>) {
        loop {
            select! {
                // 广播通道无法收回通道中的消息，所以只能等到通道关闭，
                // 确保已经收到所有消息
                ret = topic_msg_rx.recv() => {
                    match ret {
                        Ok(msg) if msg.is_defered() => {
                            // TODO: 如果通道满了，要落盘
                            self.push_defered_msg(msg);
                        },
                        Ok(msg) => {
                            self.mem_msg_tx.send(msg).await;
                            // TODO: 如果通道满了，要落盘
                        },
                        Err(RecvError::Lagged(num)) => {
                            error!("Receive lagged, {num} messages was missed");
                        },
                        Err(RecvError::Closed) => {
                            // TODO: Topic已关闭，channel也要开始退出
                            break;
                        }
                    };
                },
            }
        }

        // TODO: 通知客户端退出
        self.mem_msg_tx.close();

        while let Ok(msg) = self.mem_msg_rx.recv().await {
            //TODO: 将channel_msg_rx中剩余的消息落盘
        }
    }
}

mod test {

    #[test]
    fn test_multi() {
        //
    }
}
