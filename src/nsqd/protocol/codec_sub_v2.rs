use std::usize;

use bytes::{Buf, BufMut as _};
use regex::bytes::Regex;
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    errors::NsqError,
    nsqd::{message::MSG_ID_LENGTH, MessageID},
};

use super::{
    codec_v2::{read_timeout, read_u64},
    frame_v2::FrameSub,
    frame_v2::Resp,
    Codec,
};

/// CodecSubV2 handles the stream from a subscribed client
pub struct CodecSubV2 {
    next_index: usize,
}

impl CodecSubV2 {
    pub fn new() -> Self {
        Self { next_index: 0 }
    }
}

impl Codec<Resp<'_>> for CodecSubV2 {}

impl Decoder for CodecSubV2 {
    type Item = FrameSub;

    type Error = NsqError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let read_to = src.len();
        let Some(lf_index) = src[self.next_index..src.len()]
            .iter()
            .position(|b| *b == b'\n')
        else {
            self.next_index = read_to;
            return Ok(None);
        };

        let buf = src.split_to(lf_index);
        src.advance(1);
        self.next_index = 0;

        let mut parts = buf.splitn(3, |b| *b == b' ');
        // Currently our command has at most 3 parts seperated by white space
        let cmd = (parts.next().unwrap(), parts.next(), parts.next());

        let frame = match cmd {
            (b"CLS", None, None) => FrameSub::CLS,
            (b"NOP", None, None) => FrameSub::NOP,
            (b"FIN", Some(msg_id), None) => FrameSub::FIN(read_msg_id(msg_id)?),
            (b"RDY", Some(cnt), None) => FrameSub::RDY(read_u64(cnt.to_vec())? as i64),
            (b"REQ", Some(msg_id), Some(ms)) => {
                FrameSub::REQ(read_msg_id(msg_id)?, read_timeout(ms.to_vec())?)
            }
            (b"TOUCH", Some(msg_id), None) => FrameSub::TOUCH(read_msg_id(msg_id)?),

            _ => {
                return Err(NsqError::FatalClientErr(
                    "E_BAD_CMD".into(),
                    "Invalid command".into(),
                ));
            }
        };
        Ok(Some(frame))
    }
}

fn read_msg_id(buf: &[u8]) -> Result<MessageID, NsqError> {
    if buf.len() < MSG_ID_LENGTH {
        return Err(NsqError::FatalClientErr(
            "E_INVALID".into(),
            "Invalid message ID".into(),
        ));
    }

    let mut msg_id = [0u8; MSG_ID_LENGTH];
    msg_id.copy_from_slice(&buf[..MSG_ID_LENGTH]);

    Ok(msg_id)
}

impl Encoder<Resp<'_>> for CodecSubV2 {
    type Error = NsqError;

    fn encode(&mut self, resp: Resp, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let size = resp.get_inner_size() + 4;

        dst.put_u32(size as u32);
        dst.put_u32(resp.get_code().into());
        resp.put_to(dst);
        Ok(())
    }
}
