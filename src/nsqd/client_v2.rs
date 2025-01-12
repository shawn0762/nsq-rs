use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tokio::{
    io::{BufReader, BufWriter},
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpStream,
    },
    sync::{mpsc, oneshot},
};

use super::{channel::Channel, nsqd::NSQD, shutdown::Shutdown};

const DEFAULT_BUF_SIZE: i32 = 16 * 1024;

pub(super) trait Client {
    fn close();
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

    ready_count: AtomicI64,
    in_flight_count: AtomicI64,
    message_count: AtomicU64,
    finish_count: AtomicU64,
    requeue_count: AtomicU64,

    pub_counts: HashMap<String, AtomicU64>,

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

    channel: Option<Channel>,

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
            ready_count: AtomicI64::new(0),
            in_flight_count: AtomicI64::new(0),
            message_count: AtomicU64::new(0),
            finish_count: AtomicU64::new(0),
            requeue_count: AtomicU64::new(0),
            pub_counts: HashMap::new(),
            nsqd: nsqd.clone(),
            user_agent: None,
            stream,
            output_buffer_size: DEFAULT_BUF_SIZE,
            output_buffer_timeout: opts.output_buffer_timeout,
            heartbeat_interval: opts.client_timeout / 2,
            msg_timeout: opts.msg_timeout,
            state: State::Init,
            connect_time: Instant::now(),
            channel: None,
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

impl ClientV2 {
    pub fn finished_msg(&mut self) {
        self.finish_count.fetch_add(1, Ordering::SeqCst);
        self.in_flight_count.fetch_sub(1, Ordering::SeqCst);
        // TODO: tryUpdateReadyState
    }

    pub fn published_msg(&mut self, topic: &str, count: u64) {
        self.pub_counts
            .get_mut(topic)
            .unwrap()
            .fetch_add(count, Ordering::SeqCst);
    }

    pub fn requeue_msg(&mut self) {
        self.requeue_count.fetch_add(1, Ordering::SeqCst);
        self.in_flight_count.fetch_sub(1, Ordering::SeqCst);
        // TODO: tryUpdateReadyState
    }

    pub fn sending_msg(&mut self) {
        self.in_flight_count.fetch_add(1, Ordering::SeqCst);
        self.message_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn timed_out_msg(&mut self) {
        self.in_flight_count.fetch_sub(1, Ordering::SeqCst);
        // TODO: tryUpdateReadyState
    }
}

impl Client for ClientV2 {
    fn close() {
        todo!()
    }
}

struct IdentifyEvent {
    output_buffer_timeout: Duration,
    heartbeat_interval: Duration,
    sample_rate: i32,
    msg_timeout: Duration,
}
