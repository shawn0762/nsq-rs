use std::{mem, str::FromStr, time::Duration, usize};

use bytes::{Buf, Bytes, BytesMut};
use regex::bytes::Regex;
use tokio_util::codec::{Decoder, Encoder};

use crate::errors::NsqError;

use super::{frame::Frame, Codec};

use lazy_static::lazy_static;

pub(super) struct CodecV2 {
    next_index: usize,

    // The index of \n
    // lf_index: Option<usize>,

    // The index of last byte of this frame
    // frame_end_index: Option<usize>,
    frame: Option<Frame>,
}

impl CodecV2 {
    pub(super) fn new() -> Self {
        Self {
            next_index: 0,
            frame: None,
        }
    }
}

impl Codec for CodecV2 {}

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

        let ret = match self.frame.as_mut().unwrap() {
            Frame::NOP => Ok(Some(())),
            Frame::SUB(_, _) => Ok(Some(())),
            Frame::PUB(_, bytes) => read_msg_body(src, bytes),
            Frame::DPUB(_, _, bytes) => read_msg_body(src, bytes),
            Frame::MPUB(_, vec) => {
                //
                if src.len() < 8 {
                    return Ok(None);
                }

                // 第一个四字节先不管

                let msg_cnt = u32::from_be_bytes(src[4..8].try_into().unwrap()) as usize;
                for _ in (0..msg_cnt) {}

                Ok(None)
            }
            Frame::AUTH(bytes) => Ok(None),
        };

        match ret {
            Ok(Some(_)) => Ok(self.frame.take()),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
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

        let mut parts = src[0..lf_index].splitn(3, |b| *b == b' ');
        // 目前的命令中最长的有三段
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
                src.advance(lf_index + 1);
                return Err(NsqError::FatalClientErr(
                    "E_BAD_CMD".into(),
                    "Invalid command".into(),
                ));
            }
        };
        src.advance(lf_index + 1);
        Ok(Some(frame))
    }
}

impl Encoder<Frame> for CodecV2 {
    type Error = NsqError;

    fn encode(&mut self, item: Frame, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        todo!()
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
fn read_timeout(ms: Vec<u8>) -> Result<Duration, NsqError> {
    let err = Err(NsqError::FatalClientErr(
        "E_INVALID".into(),
        "Invalid message timeout".into(),
    ));

    let Ok(num_str) = String::from_utf8(ms) else {
        return err;
    };
    let Ok(ms) = num_str.parse::<u64>() else {
        return err;
    };
    Ok(Duration::from_millis(ms))
}

fn read_msg_body(src: &mut BytesMut, bytes: &mut Bytes) -> Result<Option<()>, NsqError> {
    if src.len() < 4 {
        return Ok(None);
    }
    // TODO: 这里可能会重复多次执行
    let body_size = u32::from_be_bytes(src[0..4].try_into().unwrap()) as usize;

    if src.len() < body_size + 4 {
        return Ok(None);
    }
    mem::replace(bytes, bytes::Bytes::copy_from_slice(&src[4..body_size + 4]));
    src.advance(body_size + 4);
    Ok(Some(()))
}
