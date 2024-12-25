use tokio::{
    select,
    signal::{
        self,
        unix::{signal, SignalKind},
    },
};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() {
    // 创建一个订阅者，将格式化trace输出到stdout
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    // 此后发生的所有trace都由这个订阅者处理
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let token = CancellationToken::new();

    let mut sign_term = signal(SignalKind::terminate()).unwrap();

    let handle = tokio::spawn(async { todo!("启动nsqd") });

    loop {
        select! {
            _ = signal::ctrl_c() => {
                println!("Signint received.");
                break;
            }
            _ = sign_term.recv() => {
                println!("Signterm received.");
                break;
            }
        }
    }
    token.cancel();
    println!("Waiting NSQD to shutdown!");
    let _ = handle.await;
    println!("NSQD shutdown successfully!");
}
