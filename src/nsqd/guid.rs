use hex;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

use super::MessageID;

const NODE_ID_BITS: u64 = 10;
const SEQUENCE_BITS: u64 = 12;
const NODE_ID_SHIFT: u64 = SEQUENCE_BITS;
const TIMESTAMP_SHIFT: u64 = SEQUENCE_BITS + NODE_ID_BITS;
const SEQUENCE_MASK: i64 = -1 ^ (-1 << SEQUENCE_BITS);

// ( 2012-10-28 16:23:42 UTC ).UnixNano() >> 20
const TWEPOCH: i64 = 1288834974288;

#[derive(Error, Debug)]
pub enum GuidError {
    #[error("time has gone backwards")]
    TimeBackwards,
    #[error("sequence expired")]
    SequenceExpired,
    #[error("ID went backward")]
    IDBackwards,
}

type Guid = i64;

pub struct GuidFactory {
    node_id: i64,
    sequence: i64,
    last_timestamp: i64,
    last_id: Guid,
}

impl GuidFactory {
    pub fn new(node_id: i64) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            node_id,
            sequence: 0,
            last_timestamp: 0,
            last_id: 0,
        }))
    }

    pub fn new_guid(&mut self) -> Result<Guid, GuidError> {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64
            >> 20;

        if ts < self.last_timestamp {
            return Err(GuidError::TimeBackwards);
        }

        if self.last_timestamp == ts {
            self.sequence = (self.sequence + 1) & SEQUENCE_MASK;
            if self.sequence == 0 {
                return Err(GuidError::SequenceExpired);
            }
        } else {
            self.sequence = 0;
        }

        self.last_timestamp = ts;

        let id =
            ((ts - TWEPOCH) << TIMESTAMP_SHIFT) | (self.node_id << NODE_ID_SHIFT) | self.sequence;

        if id <= self.last_id {
            return Err(GuidError::IDBackwards);
        }

        self.last_id = id;

        Ok(id)
    }
}

pub fn guid_to_hex(guid: Guid) -> MessageID {
    let bytes = guid.to_be_bytes();
    let mut message_id = [0u8; 16];
    hex::encode_to_slice(&bytes, &mut message_id).unwrap();
    message_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guid_generation() {
        let factory = GuidFactory::new(1);
        let mut factory = factory.lock().unwrap();

        let guid1 = factory.new_guid().unwrap();
        let guid2 = factory.new_guid().unwrap();

        assert!(guid2 > guid1);
    }

    #[test]
    fn test_guid_to_hex() {
        let guid: Guid = 1234567890;
        let hex = guid_to_hex(guid);
        assert_eq!(hex, *b"00000000499602d2");
    }
}
