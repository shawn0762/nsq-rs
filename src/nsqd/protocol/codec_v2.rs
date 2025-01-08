use std::usize;

use bytes::{Buf, Bytes};
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
        // loop {
        //     let read_to = src.len();
        //     match (self.lf_index, self.frame_end_index) {
        //         (None, None) => {
        //             let lf_index = src[0..src.len()].iter().position(|b| *b == b'\n');
        //             if lf_index.is_none() {
        //                 self.next_index = read_to;
        //                 return Ok(None);
        //             };

        //             self.lf_index = lf_index;
        //             self.next_index = lf_index.unwrap() + 1;
        //         }
        //         (Some(_), None) => {}
        //         (Some(lf_index), Some(frame_end_index)) => {
        //             //
        //         }
        //         _ => {}
        //     }
        // }
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

        let frame = self.frame.take().unwrap();

        match frame {
            Frame::NOP => Ok(self.frame.take()),
            Frame::SUB(_, _) => Ok(self.frame.take()),
            Frame::PUB(topic_name, _) => {
                if src.len() < 4 {
                    return Ok(None);
                }
                let body_size = u32::from_be_bytes(src[0..4].try_into().unwrap()) as usize;

                if src.len() < body_size + 4 {
                    return Ok(None);
                }

                Ok(Some(Frame::PUB(
                    topic_name,
                    bytes::Bytes::copy_from_slice(&src[4..body_size + 4]),
                )))
            }
            Frame::DPUB(_, duration, bytes) => Ok(None),
            Frame::MPUB(_, vec) => Ok(None),
            Frame::AUTH(bytes) => Ok(None),
        }

        // Ok(None)
        // Ok(self.frame.take())
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
