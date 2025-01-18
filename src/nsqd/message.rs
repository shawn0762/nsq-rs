use core::time;
use std::fmt::Write;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::{BufMut, Bytes};

use crate::common::Result;
use crate::errors::NsqError;

use super::backend_queue::BackEndQueue;

pub const MSG_ID_LENGTH: usize = 16;
const MIN_VALID_MSG_LEN: usize = MSG_ID_LENGTH + 8 + 2; // Timestamp + Attempts

pub type MessageID = [u8; MSG_ID_LENGTH];

#[derive(Clone, Debug)]
pub struct Message {
    id: MessageID,
    body: Bytes,

    timestamp: u64,
    attempts: u16,

    // 投递给客户端的时间
    delivery_ts: Option<Instant>,
    client_id: Option<i64>,
    pri: u64,
    index: isize,
    deferred: Option<time::Duration>,
}

impl Message {
    /// Put all the message bytes into the `dst`.
    /// # Panic
    /// This fn will panic if there is not enough capacity of `dst`.
    /// Make sure that you have reserve the `dst` to a suitable capacity before this fn.
    pub fn put_to(&self, dst: &mut bytes::BytesMut) {
        dst.put_u64(self.timestamp as u64);
        dst.put_u16(self.attempts);
        dst.put(&self.id[..]);
        dst.put(self.body.clone());
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
    pub fn decode(b: Bytes) -> Result<Message> {
        if b.len() < MIN_VALID_MSG_LEN {
            return Err(NsqError::InvalidMsgLength);
        }
        let timestamp = u64::from_be_bytes(b[..8].try_into().unwrap()) as u64;
        let attempts = u16::from_be_bytes(b[8..10].try_into().unwrap());
        let id = b[10..10 + MSG_ID_LENGTH].try_into().unwrap();
        let body = b.slice(10..MSG_ID_LENGTH);
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
    // pub async fn write_to_backend<Q>(&mut self, bq: &mut Q) -> Result<()>
    // where
    //     Q: BackEndQueue,
    // {
    //     // 这里要不要用buf pool优化一下？
    //     let mut buf = Vec::with_capacity(MIN_VALID_MSG_LEN);
    //     // 这里写入一次到buf，然后再写入一次到bq，能不能优化？
    //     self.put_to(&mut buf).await?;

    //     bq.put(buf.as_slice()).await?;

    //     Ok(())
    // }
}

impl Message {
    pub fn new(id: MessageID, body: Bytes) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
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

    pub fn id(&self) -> MessageID {
        self.id
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn is_defered(&self) -> bool {
        self.deferred.is_some()
    }

    pub fn len(&self) -> i32 {
        26 + self.body.len() as i32
    }

    pub fn get_defered(&self) -> Duration {
        self.deferred.unwrap_or_else(|| Duration::from_secs(0))
    }
}

// For defered messages min-heap
pub struct MsgItem(pub u128, pub Message);

impl Eq for MsgItem {}

impl PartialOrd for MsgItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.0.partial_cmp(&self.0)
    }
}

impl PartialEq for MsgItem {
    fn eq(&self, other: &Self) -> bool {
        &self.0 == &other.0
    }
}

impl Ord for MsgItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.0.cmp(&self.0)
    }
}
