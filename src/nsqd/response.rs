use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use crate::common::Result;

use super::Message;

pub enum Resp {
    Response = 0,
    Error = 1,
    Message = 2,
}

//          response frame
// [x][x][x][x][x][x][x][x][x][x][x][x]...
// |  (int32) ||  (int32) || (binary)
// |  4-byte  ||  4-byte  ||  N-byte
// ------------------------------------...
//     size     frame type     data
// size = 4 + N

impl Resp {
    pub async fn send<W>(self, msg: Message, writer: &W) -> Result<()>
    where
        W: AsyncWriteExt,
    {
        //

        Ok(())
    }
}
