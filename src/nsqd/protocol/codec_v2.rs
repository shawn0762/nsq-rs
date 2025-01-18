use std::{mem, time::Duration, usize};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use regex::bytes::Regex;
use tokio_util::codec::{Decoder, Encoder};

use crate::errors::NsqError;

use super::{frame_v2::Frame, frame_v2::Resp, Codec};

use lazy_static::lazy_static;

enum FrameState {
    // We have received a whole frame
    Completed,
    // We need more data
    Incomplete,
    // We encounter some error
    Err(NsqError),
}

pub struct CodecV2 {
    next_index: usize,

    frame: Option<Frame>,
}

impl CodecV2 {
    pub fn new() -> Self {
        Self {
            next_index: 0,
            frame: None,
        }
    }
}

impl Codec<Resp<'_>> for CodecV2 {}

impl Decoder for CodecV2 {
    type Item = Frame;

    type Error = NsqError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if self.frame.is_none() {
            match self.init_frame(src) {
                Ok(f) if f.is_some() => {
                    self.frame = f;
                }
                // 需要更多数据 or 解析出错
                other => {
                    return other;
                }
            };
        }

        // TODO: 缺少订阅后的几个命令

        let state = match self.frame.as_mut().unwrap() {
            Frame::NOP => FrameState::Completed,
            Frame::SUB(_, _) => FrameState::Completed,
            Frame::PUB(_, bytes) => read_msg_body(src, bytes),
            Frame::DPUB(_, _, bytes) => read_msg_body(src, bytes),
            Frame::MPUB(_, msgs) => read_multi_msg_body(src, msgs),
            Frame::AUTH(bytes) => FrameState::Incomplete,
        };

        match state {
            FrameState::Completed => Ok(self.frame.take()),
            FrameState::Incomplete => Ok(None),
            FrameState::Err(e) => Err(e),
        }
    }
}

impl CodecV2 {
    fn init_frame(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Frame>, NsqError> {
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
            (b"NOP", None, None) => Frame::NOP,
            (b"SUB", Some(topic_name), Some(channel_name)) => Frame::SUB(
                try_to_string_name(topic_name)?,
                try_to_string_name(channel_name)?,
            ),
            (b"PUB", Some(topic_name), None) => {
                Frame::PUB(try_to_string_name(topic_name)?, bytes::Bytes::new())
            }
            (b"DPUB", Some(topic_name), Some(ms_str)) => Frame::DPUB(
                try_to_string_name(topic_name)?,
                read_timeout(ms_str.to_vec())?,
                bytes::Bytes::new(),
            ),
            (b"MPUB", Some(topic_name), None) => {
                Frame::MPUB(try_to_string_name(topic_name)?, Vec::new())
            }
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

lazy_static! {
    static ref NAME_REGEX: Regex = Regex::new(r"^[.a-zA-Z0-9_-]+(#ephemeral)?$").unwrap();
}

// 检查topic或channel名称是否合法
// 字符组成：.a-zA-Z0-9_-
// 长度：[2, 64]
fn try_to_string_name(name: &[u8]) -> Result<String, NsqError> {
    match name.len() {
        2..=64 if NAME_REGEX.is_match(name) => {
            let name = unsafe { String::from_utf8_unchecked(name.to_vec()) };
            Ok(name)
        }
        _ => Err(NsqError::FatalClientErr(
            "E_BAD_TOPIC".into(),
            format!("PUB topic name {:?} is not valid", name),
        )),
    }
}

// 从一个数字字符串解析出毫秒数
pub(super) fn read_timeout(ms: Vec<u8>) -> Result<Duration, NsqError> {
    Ok(Duration::from_millis(read_u64(ms)?))
}

pub(super) fn read_u64(n: Vec<u8>) -> Result<u64, NsqError> {
    let err = Err(NsqError::FatalClientErr(
        "E_INVALID".into(),
        "Invalid message timeout".into(),
    ));

    let Ok(num_str) = String::from_utf8(n) else {
        return err;
    };
    num_str.parse::<u64>().or(err)
}

/// Read one message body into the Bytes
/// # return
/// Once we received a whole message, we return the `FrameState::Completed`.
/// Otherwise return the `FrameState::Incomplete`.
fn read_msg_body(src: &mut BytesMut, bytes: &mut Bytes) -> FrameState {
    use FrameState::*;

    if src.len() < 4 {
        return Incomplete;
    }
    // TODO: 这里可能会重复多次执行
    let body_size = u32::from_be_bytes(src[0..4].try_into().unwrap()) as usize;

    if src.len() < body_size + 4 {
        return Incomplete;
    }
    mem::replace(bytes, bytes::Bytes::copy_from_slice(&src[4..body_size + 4]));
    src.advance(body_size + 4);
    Completed
}

/// Read message bodies into the vec of Bytes
/// # return
/// This fn returns `FrameState::Err(e)` if we encounter some invalid data. Once
/// all message bodies are received, `FrameState::Completed` will be returned. Otherwise
/// returns `FrameState::Incomplete`.
fn read_multi_msg_body(src: &mut BytesMut, msgs: &mut Vec<Bytes>) -> FrameState {
    if msgs.capacity() == 0 {
        if src.len() < 8 {
            return FrameState::Incomplete;
        }

        // The first 4-bytes represent the total number of bytes that follow
        let size = u32::from_be_bytes(src[0..4].try_into().unwrap()) as usize;
        // 8 = <4 bytes for message count> + <4 bytes for the first message size>
        if size <= 8 {
            return FrameState::Err(NsqError::FatalClientErr(
                "E_BAD_BODY".into(),
                "MPUB invalid total message size".into(),
            ));
        }

        // The second 4-bytes represent the total number of messages
        let msg_cnt = u32::from_be_bytes(src[4..8].try_into().unwrap()) as usize;
        if msg_cnt < 1 {
            return FrameState::Err(NsqError::FatalClientErr(
                "E_BAD_BODY".into(),
                "MPUB invalid message count".into(),
            ));
        }

        src.advance(8);
        mem::replace(msgs, Vec::with_capacity(msg_cnt));
    }

    while msgs.len() != msgs.capacity() {
        let mut body = bytes::Bytes::new();

        match read_msg_body(src, &mut body) {
            FrameState::Completed => {
                msgs.push(body);
                continue;
            }
            other => return other,
        }
    }
    FrameState::Completed
}

impl Encoder<Resp<'_>> for CodecV2 {
    type Error = NsqError;

    fn encode(&mut self, resp: Resp, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let size = resp.get_inner_size() + 4;

        dst.put_u32(size as u32);
        dst.put_u32(resp.get_code().into());
        resp.put_to(dst);
        Ok(())
    }
}
