use core::slice::SlicePattern;
use std::{
    io::{self, BufRead, Cursor, Read},
    str::{self, FromStr},
    time::Duration,
    u64,
};

use axum::middleware::from_fn;
use bytes::{Bytes, BytesMut};
use lazy_static::lazy_static;
use regex::bytes::Regex;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt},
    net::TcpStream,
};

use crate::{common, errors::NsqError};

use super::{message::MSG_ID_LENGTH, Message, MessageID};

pub enum Error {
    Incomplete,

    FatalClientErr(String, String),

    Other(NsqError),
}

pub(super) enum Frame {
    AUTH(Bytes),

    // IDENTIFY(),
    PUB(String, Bytes), // 0: Topic name 1: Msg body

    DPUB(String, Message), // Topic Name

    MPUB(String, Vec<Message>), // Topic Name

    // Topic Name and Channel Name
    SUB(String, String),

    // TODO: 配置文件有一个最大值，usize未必合适
    RDY(usize),

    FIN(MessageID),

    REQ(MessageID, Duration),

    TOUCH(MessageID),
    CLS,
    NOP,
}

impl Frame {
    pub async fn parse(src: &mut Cursor<&[u8]>) -> Result<Self, Error> {
        // 最长的命令有8个字节，一次性分配好可能需要的最大内存
        let mut buf = Vec::with_capacity(8);

        let Ok(cmd_size) = AsyncBufReadExt::read_until(src, b'\n', &mut buf).await else {
            return Err(Error::FatalClientErr(
                "E_EOF".into(),
                "Unexcepted EOF".into(),
            ));
        };
        buf.trim_ascii_end(); // 去掉最后的\n和可能会出现的\r

        let mut sp = buf.splitn(2, |b| b' ' == *b);

        // PUB aaa\n => 3
        // NOP\n => 3
        // let pos = buf.iter().position(|b| b' ' == *b).unwrap_or(cmd_size);

        let Some(cmd) = sp.next() else {
            return Err(Error::FatalClientErr(
                "E_BAD_CMD".into(),
                "Invalid command".into(),
            ));
        };
        match cmd {
            // b"AUTH" => {}
            // b"IDENTIFY" => {}
            b"CLS" => Ok(Frame::CLS),
            b"PUB" => {
                buf.drain(..4);
                parse_pub(src, buf).await
            }
            // b"DPUB" => {}
            b"FIN" => {
                buf.drain(..4);
                parse_fin(buf)
            }
            // b"MPUB" => {}
            b"NOP" => Ok(Frame::NOP),
            // b"REQ" => {}
            // b"RDY" => {}
            // b"SUB" => {}
            // b"TOUCH" => {}
            _ => Err(Error::FatalClientErr(
                "E_BAD_CMD".into(),
                "Invalid command".into(),
            )),
        }
    }
}

fn parse_fin(buf: Vec<u8>) -> Result<Frame, Error> {
    if buf.len() != 16 {
        return Err(Error::FatalClientErr(
            "E_INVALID".into(),
            "Invalid message ID".into(),
        ));
    }
    let mut msg_id = [0u8; MSG_ID_LENGTH];
    msg_id.copy_from_slice(&buf[..MSG_ID_LENGTH]);
    Ok(Frame::FIN(msg_id))
}

fn parse_req(mut buf: Vec<u8>) -> Result<Frame, Error> {
    // 17 = 16(msg id) + 1(timeout)
    if buf.len() < MSG_ID_LENGTH {
        return Err(Error::FatalClientErr(
            "E_INVALID".into(),
            "Invalid message ID".into(),
        ));
    }

    let mut msg_id = [0u8; MSG_ID_LENGTH];
    msg_id.copy_from_slice(&buf[..MSG_ID_LENGTH]);

    buf.drain(..MSG_ID_LENGTH);

    let err = Err(Error::FatalClientErr(
        "E_INVALID".into(),
        "Invalid message timeout".into(),
    ));

    let Ok(num_str) = String::from_utf8(buf) else {
        return err;
    };
    let Ok(ms) = num_str.parse::<u64>() else {
        return err;
    };

    Ok(Frame::REQ(msg_id, Duration::from_millis(ms)))
}

async fn parse_dpub(src: &mut Cursor<&[u8]>, topic_name: Vec<u8>) -> Result<Frame, Error> {
    check_name(&topic_name)?;

    // 接下来4个字节表示消息体的长度
    let Ok(size) = src.read_u32().await else {
        return Err(Error::FatalClientErr(
            "E_BAD_MESSAGE".into(),
            "PUB failed to read message body size".into(),
        ));
    };

    // TODO: 限制消息体大小：max_msg_size

    let mut msg_body = bytes::BytesMut::with_capacity(size as usize);
    let Ok(_) = AsyncReadExt::read_exact(src, &mut msg_body).await else {
        return Err(Error::FatalClientErr(
            "E_BAD_MESSAGE".into(),
            "PUB failed to read message body".into(),
        ));
    };

    Ok(Frame::PUB(
        String::from_utf8(topic_name).unwrap(),
        msg_body.into(),
    ))
}

async fn parse_pub(src: &mut Cursor<&[u8]>, topic_name: Vec<u8>) -> Result<Frame, Error> {
    check_name(&topic_name)?;

    // 接下来4个字节表示消息体的长度
    let Ok(size) = src.read_u32().await else {
        return Err(Error::FatalClientErr(
            "E_BAD_MESSAGE".into(),
            "PUB failed to read message body size".into(),
        ));
    };

    // TODO: 限制消息体大小：max_msg_size

    let mut msg_body = bytes::BytesMut::with_capacity(size as usize);
    let Ok(_) = AsyncReadExt::read_exact(src, &mut msg_body).await else {
        return Err(Error::FatalClientErr(
            "E_BAD_MESSAGE".into(),
            "PUB failed to read message body".into(),
        ));
    };

    Ok(Frame::PUB(
        String::from_utf8(topic_name).unwrap(),
        msg_body.into(),
    ))
}

lazy_static! {
    static ref NAME_REGEX: Regex = Regex::new(r"^[.a-zA-Z0-9_-]+(#ephemeral)?$").unwrap();
}

// 检查topic或channel名称是否合法
fn check_name(name: &Vec<u8>) -> Result<(), Error> {
    match name.len() {
        2..=64 if NAME_REGEX.is_match(name) => Ok(()),
        _ => Err(Error::FatalClientErr(
            "E_BAD_TOPIC".into(),
            format!("PUB topic name {:?} is not valid", name),
        )),
    }
}
