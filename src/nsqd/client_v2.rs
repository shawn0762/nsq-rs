use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use async_channel::Receiver;
use tokio::{
    io::{AsyncWriteExt, BufReader, BufWriter},
    net::TcpStream,
    select,
    sync::{mpsc, oneshot},
    time::{self},
};

use crate::nsqd::command::FrameSub;

use super::{channel::Channel, command::Frame, message::Message, nsqd::NSQD};
use async_trait::async_trait;

const DEFAULT_BUF_SIZE: i32 = 16 * 1024;

#[async_trait]
pub(super) trait Client: Send + Sync {
    fn id(&self) -> i64;
    fn close(&mut self);
    async fn serve(&mut self);
}

pub(super) enum State {
    Init,
    Disconnected,
    Connected,
    Subscribed,
    Closing,
}

pub(super) struct ClientV2 {
    id: i64,

    // TODO: 需要锁？
    //      writeLock
    //      metaLock
    nsqd: Arc<NSQD>,

    user_agent: Option<String>,

    stream: TcpStream,

    // tls_conn,
    // flate_writer,
    output_buffer_size: i32,
    output_buffer_timeout: Duration,
    heartbeat_interval: Duration,
    msg_timeout: Duration,

    state: State,
    connect_time: Instant,

    ready_state_tx: mpsc::Sender<isize>,
    ready_state_rx: mpsc::Receiver<isize>,

    // shutdown: Shutdown,
    client_id: String,
    client_addr: SocketAddr,
    // pub hostname: String,
    sample_rate: i32,

    identify_event_tx: oneshot::Sender<IdentifyEvent>,
    identify_event_rx: oneshot::Receiver<IdentifyEvent>,

    pub sub_event_tx: oneshot::Sender<Channel>,
    pub sub_event_rx: oneshot::Receiver<Channel>,

    tls: bool,
    snappy: bool,
    deflate: bool,

    // 可重复使用的缓冲区，用于从报文中读取4字节长度
    // len_buf: [u8; 4], // Vec内部就是数组，这里不需要自己维护一个数组
    len_slice: Vec<u8>,

    auth_secret: String,
    // auth_state: auth::State,
}

impl ClientV2 {
    pub fn new(id: i64, stream: TcpStream, addr: SocketAddr, nsqd: Arc<NSQD>) -> Self {
        let opts = nsqd.get_opts();
        let (ready_state_tx, ready_state_rx) = mpsc::channel(1);
        let (sub_event_tx, sub_event_rx) = oneshot::channel();
        let (identify_event_tx, identify_event_rx) = oneshot::channel();

        let ip = addr.ip();

        Self {
            id,

            nsqd: nsqd.clone(),
            user_agent: None,
            stream,
            output_buffer_size: DEFAULT_BUF_SIZE,
            output_buffer_timeout: opts.output_buffer_timeout,
            heartbeat_interval: opts.client_timeout / 2,
            msg_timeout: opts.msg_timeout,
            state: State::Init,
            connect_time: Instant::now(),
            ready_state_tx,
            ready_state_rx,
            // shutdown: todo!(),
            client_id: ip.to_string(),
            client_addr: addr,
            sample_rate: 0,
            identify_event_tx,
            identify_event_rx,
            sub_event_tx,
            sub_event_rx,
            tls: false,
            snappy: false,
            deflate: false,
            len_slice: Vec::with_capacity(4),
            auth_secret: "to do".to_owned(),
        }
    }

    pub fn addr(&self) -> String {
        self.client_addr.to_string()
    }
}

#[async_trait]
impl Client for ClientV2 {
    fn id(&self) -> i64 {
        self.id
    }
    fn close(&mut self) {
        todo!()
    }

    async fn serve(&mut self) {
        let (read, write) = self.stream.split();
        let mut ticker = time::interval(self.heartbeat_interval);

        let mut buf_reader = BufReader::new(read);
        let mut buf_writer = BufWriter::new(write);
        loop {
            select! {
                ret = Frame::parse(&mut buf_reader) => match ret {
                    Ok(f) => match f {
                        Frame::AUTH(bytes) => todo!(),
                        Frame::PUB(_, bytes) => todo!(),
                        Frame::DPUB(_, duration, bytes) => todo!(),
                        Frame::MPUB(_, vec) => todo!(),
                        Frame::SUB(topic, channel) => {
                            todo!()
                        },
                        Frame::NOP => todo!(),
                    },
                    Err(_) => {},
                },
                _ = ticker.tick() => {
                    // TODO: 发送完整的心跳包
                    buf_writer.write(b"_heartbeat_").await;
                    buf_writer.flush().await;
                }
            }
        }
    }
}

struct IdentifyEvent {
    output_buffer_timeout: Duration,
    heartbeat_interval: Duration,
    sample_rate: i32,
    msg_timeout: Duration,
}

pub(super) struct SubscriberV2 {
    client: ClientV2,
    mem_msg_rx: async_channel::Receiver<Message>,

    pub_counts: HashMap<String, AtomicU64>,

    counter: Arc<Counter>,
}

impl SubscriberV2 {
    pub fn new(client: ClientV2, mem_msg_rx: Receiver<Message>) -> Self {
        // client.nsqd.tracker().spawn(msg_handle(mem_msg_rx.clone()));
        let counter = Arc::new(Counter::new());
        Self {
            client,
            mem_msg_rx,
            counter,
            pub_counts: HashMap::new(),
        }
    }

    fn send_msg(msg: Message) -> Result<()> {
        
        

        Ok(())
    }
}

#[async_trait]
impl Client for SubscriberV2 {
    fn id(&self) -> i64 {
        self.client.id()
    }

    fn close(&mut self) {
        //
    }

    // TODO: 用户发起订阅后，怎么让这个跑起来？
    async fn serve(&mut self) {
        let (read, write) = self.client.stream.split();
        let mut ticker = time::interval(self.client.heartbeat_interval);

        let mut buf_reader = BufReader::new(read);
        let mut buf_writer = BufWriter::new(write);
        loop {
            select! {
                ret = FrameSub::parse(&mut buf_reader) => match ret {
                    Ok(f) => match f {
                        FrameSub::CLS => todo!(),
                        FrameSub::FIN(_) => todo!(),
                        FrameSub::NOP => todo!(),
                        FrameSub::RDY(_) => todo!(),
                        FrameSub::REQ(_, duration) => todo!(),
                        FrameSub::TOUCH(_) => todo!(),
                    },
                    Err(_) => {},
                },
                ret = self.mem_msg_rx.recv() => match ret {
                    Ok(msg) => {},
                    Err(_) => {},
                },
                _ = ticker.tick() => {
                    // TODO: 发送完整的心跳包
                    buf_writer.write(b"_heartbeat_").await;
                    buf_writer.flush().await;
                }
            }
        }
    }
}

struct Counter {
    ready_count: AtomicI64,
    in_flight_count: AtomicI64,
    message_count: AtomicU64,
    finish_count: AtomicU64,
    requeue_count: AtomicU64,
}

impl Counter {
    fn new() -> Self {
        Self {
            ready_count: 0.into(),
            in_flight_count: 0.into(),
            message_count: 0.into(),
            finish_count: 0.into(),
            requeue_count: 0.into(),
        }
    }

    pub fn finished_msg(&self) {
        self.finish_count.fetch_add(1, Ordering::SeqCst);
        self.in_flight_count.fetch_sub(1, Ordering::SeqCst);
        // TODO: tryUpdateReadyState
    }

    // pub fn published_msg(&self, topic: &str, count: u64) {
    //     self.pub_counts
    //         .get_mut(topic)
    //         .unwrap()
    //         .fetch_add(count, Ordering::SeqCst);
    // }

    pub fn requeue_msg(&self) {
        self.requeue_count.fetch_add(1, Ordering::SeqCst);
        self.in_flight_count.fetch_sub(1, Ordering::SeqCst);
        // TODO: tryUpdateReadyState
    }

    pub fn sending_msg(&self) {
        self.in_flight_count.fetch_add(1, Ordering::SeqCst);
        self.message_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn timed_out_msg(&self) {
        self.in_flight_count.fetch_sub(1, Ordering::SeqCst);
        // TODO: tryUpdateReadyState
    }
}

async fn msg_handle(mem_msg_rx: Receiver<Message>) {
    while mem_msg_rx.is_closed() {
        let Ok(msg) = mem_msg_rx.recv().await else {
            break;
        };
        // TODO: 发送给客户端
    }
}
