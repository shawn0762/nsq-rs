use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, AtomicI64},
    time::{self, Duration, Instant},
};

use dashmap::DashMap;
use tokio::{
    io::BufReader,
    net::{TcpListener, TcpStream},
    select,
    sync::{
        broadcast,
        mpsc::{self, Receiver, Sender},
    },
    time::sleep,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, info, warn};

use crate::{common::Result, nsqd::shutdown::Shutdown};

use super::{channel::Channel, options::Options, topic::Topic};

pub struct NSQD {
    client_id_seq: AtomicI64,

    is_loading: AtomicBool,
    is_exiting: AtomicBool,

    start_time: Instant,

    topic_map: DashMap<String, Topic>,

    // tcp_server:
    tcp_listener: TcpListener,
    http_listener: TcpListener,
    https_listener: TcpListener,

    exit_token: CancellationToken,

    // tls_config:,
    // client_tls_config:,
    pool_size: usize,

    notify_tx: Sender<NotifyType>,
    notify_rx: Receiver<NotifyType>,
    // 集群信息
    // ci,

    // golang中这个字段是一个原子类型
    opts: Options,

    task_tracker: TaskTracker,

    shutdown_tx: broadcast::Sender<()>,
    shutdown_rx: Shutdown,
}

impl NSQD {
    pub async fn new(opts: Options) -> (Self, CancellationToken) {
        let token = CancellationToken::new();
        let (notify_tx, notify_rx) = mpsc::channel(1);

        let tcp_listener = TcpListener::bind(opts.tcp_addr.clone()).await.unwrap();

        let http_listener = TcpListener::bind(opts.http_addr).await.unwrap();
        let https_listener = TcpListener::bind(opts.https_addr).await.unwrap();

        let (shutdown_tx, rx) = broadcast::channel(1);
        let shutdown_rx = Shutdown::new(rx);

        let nsqd = NSQD {
            client_id_seq: todo!(),
            is_loading: false.into(),
            is_exiting: false.into(),
            start_time: time::Instant::now(),
            topic_map: DashMap::new(),
            tcp_listener,
            http_listener,
            https_listener,
            exit_token: token.clone(),
            pool_size: todo!(),
            notify_tx,
            notify_rx,
            opts,
            task_tracker: TaskTracker::new(),
            shutdown_tx,
            shutdown_rx,
        };

        (nsqd, token)
    }

    pub async fn start(&self) -> Result<()> {
        let (tx, rx) = broadcast::channel(1);

        let mut shutdown: Shutdown = (&tx).into();
        let mut shutdown2: Shutdown = (&tx).into();

        // TODO: 启动tcp server
        self.task_tracker.spawn(async move {
            let tcp_listener = TcpListener::bind("127.0.0.1:6999").await.unwrap();
            let tracker = TaskTracker::new();
            loop {
                select! {
                    Ok((mut conn, addr)) = tcp_listener.accept() => {

                        let (mut reader, mut writer) = conn.split();

                        tracker.spawn(async move {
                            // 实际上这里是不断 read和write
                            // 当收到退出信号，应该先关闭read
                            debug!("Connection accept: {addr}");
                            // buf
                            // loop {
                                // _ = conn.
                            // }
                            sleep(Duration::from_secs(15)).await;
                            debug!("Connection close: {addr}");
                        });
                    },
                    _ = shutdown.recv() => {
                        info!("TCP Server shutting down");
                        break;
                    }

                }

                sleep(Duration::from_secs(5)).await;
            }

            // 不再接受新的连接
            drop(tcp_listener);

            // 等待所有task完成
            tracker.wait().await;
        });

        // TODO: 启动http server(if have)
        // TODO: 启动https server(if have)
        // TODO: 启动queue scan loop
        // TODO: 启动lookup loop
        // TODO: 启动statsd loop
        // TODO: 等待退出信号

        // self.exit_token.await;

        select! {
            _ = self.exit_token.cancelled() => {
                warn!("NSQD existing");
            }
        }

        self.task_tracker.close();

        // TODO:等待所有组件退出
        let _ = self.task_tracker.wait().await;
        Ok(())
    }

    pub fn stop(&mut self) {}

    pub fn get_opts(&self) -> &Options {
        &self.opts
    }

    pub fn tracker(&self) -> TaskTracker {
        self.task_tracker.clone()
    }

    pub fn shutdown_rx(&self) -> Shutdown {
        self.shutdown_rx.clone()
    }
}

pub enum NotifyType {
    Channel(Channel),
    Topic(Topic),
}
