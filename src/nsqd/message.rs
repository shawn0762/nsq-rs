use core::time;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;

use crate::common::Result;
use crate::errors::NsqError;

use super::backend_queue::BackEndQueue;

// use tokio::time::Instant;

const MSG_ID_LENGTH: usize = 16;
const MIN_VALID_MSG_LEN: usize = MSG_ID_LENGTH + 8 + 2; // Timestamp + Attempts

pub(super) type MessageID = [u8; MSG_ID_LENGTH];

pub(super) struct Message {
    id: MessageID,
    body: Vec<u8>,

    timestamp: i64,
    attempts: u16,

    delivery_ts: Option<Instant>,
    client_id: Option<i64>,
    pri: i64,
    index: isize,
    deferred: Option<time::Duration>,
}

impl Message {
    pub fn new(id: MessageID, body: Vec<u8>) -> Self {
        // 这里不存在panic的情况；
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap() // 这里不可能panic
            .as_nanos() as i64;
        Self {
            id,
            body,
            timestamp,
            attempts: 0,
            delivery_ts: None,
            client_id: None,
            pri: 0,
            index: 0,
            deferred: None,
        }
    }

    pub async fn write_to<W>(&self, w: &mut W) -> Result<i64>
    where
        W: AsyncWriteExt + Unpin,
    {
        let mut buf_writer = BufWriter::new(w);

        // 采用大端序
        buf_writer.write_u64(self.timestamp as u64).await?;
        buf_writer.write_u16(self.attempts).await?;
        let mut total = 10;

        total += buf_writer.write(&self.id).await?;

        total += buf_writer.write(&self.body).await?;

        buf_writer.flush().await?;
        Ok(total as i64)
    }

    // decodeMessage deserializes data (as []byte) and creates a new Message
    //
    //	[x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
    //	|       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
    //	|       8-byte         ||    ||                 16-byte                      || N-byte
    //	------------------------------------------------------------------------------------------...
    //	  nanosecond timestamp    ^^                   message ID                       message body
    //	                       (uint16)
    //	                        2-byte
    //	                       attempts
    pub fn decode(b: &[u8]) -> Result<Message> {
        if b.len() < MIN_VALID_MSG_LEN {
            return Err(NsqError::InvalidMsgLength);
        }
        let timestamp = u64::from_be_bytes(b[..8].try_into().unwrap()) as i64;
        let attempts = u16::from_be_bytes(b[8..10].try_into().unwrap());
        let id = b[10..10 + MSG_ID_LENGTH].try_into().unwrap();
        let body = b[10 + MSG_ID_LENGTH..].try_into().unwrap();
        Ok(Message {
            id,
            body,
            timestamp,
            attempts,
            delivery_ts: None,
            client_id: None,
            pri: 0,
            index: 0,
            deferred: None,
        })
    }

    // 将消息写入到后端队列，缓解内存压力
    pub async fn write_to_backend<Q>(&mut self, bq: &mut Q) -> Result<()>
    where
        Q: BackEndQueue,
    {
        // 这里要不要用buf pool优化一下？
        let mut buf = Vec::with_capacity(MIN_VALID_MSG_LEN);
        // 这里写入一次到buf，然后再写入一次到bq，能不能优化？
        self.write_to(&mut buf).await?;

        bq.put(buf.as_slice()).await?;

        Ok(())
    }
}
