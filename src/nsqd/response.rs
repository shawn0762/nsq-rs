use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use crate::common::Result;

use super::Message;

pub enum Resp {
    Resp = 0,
    Err = 1,
    Msg = 2,
}

//          response frame
// [x][x][x][x][x][x][x][x][x][x][x][x]...
// |  (int32) ||  (int32) || (binary)
// |  4-byte  ||  4-byte  ||  N-byte
// ------------------------------------...
//     size     frame type     data
// size = 4 + N
impl Resp {
    pub async fn send_msg<W>(msg: Message, writer: &mut W) -> Result<i64>
    where
        W: AsyncWriteExt + Unpin,
    {
        let size = msg.len() + 4;
        writer.write_u32(size).await?;
        writer.write_u32(Resp::Msg as u32).await?;

        let n = msg.write_to(writer).await?;
        Ok(n + 8)
    }

    pub async fn send_err<W>(err: &[u8], writer: &mut W) -> Result<i64>
    where
        W: AsyncWriteExt + Unpin,
    {
        let size = err.len() + 4;
        writer.write_u32(size as u32).await?;
        writer.write_u32(Resp::Err as u32).await?;

        let n = writer.write(err).await? as i64;

        Ok(n + 8)
    }

    pub async fn send_resp<W>(resp: &[u8], writer: &mut W) -> Result<i64>
    where
        W: AsyncWriteExt + Unpin,
    {
        let size = resp.len() + 4;
        writer.write_u32(size as u32).await?;
        writer.write_u32(Resp::Resp as u32).await?;

        let n = writer.write(resp).await? as i64;

        Ok(n + 8)
    }
}
