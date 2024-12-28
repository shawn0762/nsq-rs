use core::time;
use std::{
    os,
    path::{Path, PathBuf},
    time::Duration,
};

use rustls::ProtocolVersion;

pub struct Options {
    id: u16,

    pub tcp_addr: String,
    pub http_addr: String,
    pub https_addr: String,
    broadcast_addr: String,
    broadcast_tcp_port: u16,
    broadcast_http_port: u16,
    nsq_lookup_tcp_addrs: Vec<String>,
    auth_http_addrs: Vec<String>,
    auth_http_request_method: String,
    http_client_connect_timeout: Duration,
    http_client_request_timeout: Duration,

    // diskqueue options
    data_path: PathBuf,
    pub mem_queue_size: usize,
    max_bytes_per_file: u32,
    sync_every: u32,
    sync_timeout: Duration,

    queue_scan_interval: Duration,
    queue_scan_refresh_interval: Duration,
    queue_scan_selection_count: usize,
    queue_scan_worker_pool_max: usize,
    queue_scan_dirty_percent: f64,

    // msg and command options
    pub msg_timeout: Duration,
    max_msg_timeout: Duration,
    max_msg_size: u32,
    max_body_size: u32,
    max_req_timeout: Duration,
    pub client_timeout: Duration,

    // 客户端可以更改的配置选项
    max_heartbeat_interval: Duration,
    max_rdy_count: i64,
    max_output_buffer_size: i64,
    max_output_buffer_timeout: Duration,
    min_output_buffer_timeout: Duration,
    pub output_buffer_timeout: Duration,
    pub max_channel_subscribers: usize,

    // TLS config
    tls_cert: PathBuf,
    tls_key: PathBuf,
    tls_client_auth_policy: String,
    tls_root_ca_file: PathBuf,
    tls_required: u32,
    tls_min_version: ProtocolVersion,

    // compression
    deflate_enabled: bool,
    max_deflate_level: u32,
    snappy_enabled: bool,
}

impl Options {
    pub fn new() -> Self {
        Self {
            id: 1, // :TODO
            tcp_addr: "0.0.0.0:4150".to_owned(),
            http_addr: "0.0.0.0:4151".to_owned(),
            https_addr: "0.0.0.0:4152".to_owned(),
            broadcast_addr: "待定".to_owned(),
            broadcast_tcp_port: 0,
            broadcast_http_port: 0,
            nsq_lookup_tcp_addrs: Vec::new(),
            auth_http_addrs: Vec::new(),
            auth_http_request_method: "GET".to_owned(),
            http_client_connect_timeout: time::Duration::from_secs(2),
            http_client_request_timeout: time::Duration::from_secs(5),

            data_path: "/tmp/nsqd".into(),
            mem_queue_size: 10000,
            max_bytes_per_file: 100 * 1024 * 1024,
            sync_every: 2500,
            sync_timeout: time::Duration::from_secs(2),

            queue_scan_interval: time::Duration::from_millis(100),
            queue_scan_refresh_interval: time::Duration::from_secs(5),
            queue_scan_selection_count: 20,
            queue_scan_worker_pool_max: 4,
            queue_scan_dirty_percent: 0.25,

            msg_timeout: time::Duration::from_secs(60),
            max_msg_timeout: time::Duration::from_secs(15 * 60),
            max_msg_size: 1024 * 1024,
            max_body_size: 5 * 1024 * 1024,
            max_req_timeout: time::Duration::from_secs(60 * 60),
            client_timeout: time::Duration::from_secs(60),

            tls_cert: "/path/to/do".into(),
            tls_key: "/path/to/do".into(),
            tls_client_auth_policy: "todo".to_owned(),
            tls_root_ca_file: "/path/to/do".into(),
            tls_required: 1,
            tls_min_version: rustls::ProtocolVersion::TLSv1_0,

            deflate_enabled: true,
            max_deflate_level: 6,
            snappy_enabled: true,

            max_heartbeat_interval: todo!(),
            max_rdy_count: todo!(),
            max_output_buffer_size: todo!(),
            max_output_buffer_timeout: todo!(),
            min_output_buffer_timeout: todo!(),
            output_buffer_timeout: todo!(),
            max_channel_subscribers: todo!(),
        }
    }
}
