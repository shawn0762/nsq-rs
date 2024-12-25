use tokio::sync::mpsc;

use crate::common::Result;

pub(super) trait BackEndQueue {
    async fn put(b: &[u8]) -> Result<()>;
    fn read_chan() -> mpsc::Receiver<Vec<u8>>;
    fn close() -> Result<()>;
    fn delete() -> Result<()>;
    fn depth() -> i64;
    fn empty() -> Result<()>;
}
