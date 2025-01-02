use std::{io::Cursor, time::Duration, u64};

use bytes::Bytes;
use lazy_static::lazy_static;
use regex::bytes::Regex;
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use crate::errors::NsqError;

use super::{message::MSG_ID_LENGTH, MessageID};

pub enum Error {
    Incomplete,

    FatalClientErr(String, String),

    Other(NsqError),
}

pub type TopicName = String;
pub type ChannelName = String;
pub type Timeout = Duration;
pub type MsgBody = Bytes;

pub(super) enum Frame {
    AUTH(Bytes),

    // IDENTIFY(),
    PUB(TopicName, MsgBody),

    DPUB(TopicName, Timeout, MsgBody),

    MPUB(TopicName, Vec<MsgBody>),

    SUB(TopicName, ChannelName),

    NOP,
}

pub(super) enum FrameSub {
    CLS, // unsubscribe
    FIN(MessageID),
    NOP,
    // TODO: 配置文件有一个最大值，usize未必合适
    RDY(usize),

    REQ(MessageID, Timeout),

    TOUCH(MessageID),
}

impl Frame {
    /// parse frames for client unsubscribed
    pub async fn parse<R>(src: &mut R) -> Result<Self, Error>
    where
        R: AsyncBufReadExt + Unpin,
    {
        // 最长的命令有8个字节，一次性分配好可能需要的最大内存
        let mut buf = Vec::with_capacity(8);

        let Ok(_) = AsyncBufReadExt::read_until(src, b'\n', &mut buf).await else {
            return Err(Error::FatalClientErr(
                "E_EOF".into(),
                "Unexcepted EOF".into(),
            ));
        };
        buf.trim_ascii_end(); // 去掉最后的\n和可能会出现的\r

        let mut sp = buf.splitn(2, |b| b' ' == *b);
        let Some(cmd) = sp.next() else {
            return Err(Error::FatalClientErr(
                "E_BAD_CMD".into(),
                "Invalid command".into(),
            ));
        };
        match cmd {
            // b"AUTH" => {}
            b"DPUB" => {
                buf.drain(..4);
                parse_dpub(src, buf).await
            }
            // b"IDENTIFY" => {}
            b"MPUB" => {
                buf.drain(..4);
                parse_mpub(src, buf).await
            }
            b"NOP" => Ok(Frame::NOP),
            b"PUB" => {
                buf.drain(..3);
                parse_pub(src, buf).await
            }
            b"SUB" => {
                buf.drain(..3);
                parse_sub(buf)
            }

            _ => Err(Error::FatalClientErr(
                "E_BAD_CMD".into(),
                "Invalid command".into(),
            )),
        }
    }
}

impl FrameSub {
    /// parse frames for client subscribing
    pub async fn parse<R>(src: &mut R) -> Result<Self, Error>
    where
        R: AsyncBufReadExt + Unpin,
    {
        // 最长的命令有8个字节，一次性分配好可能需要的最大内存
        let mut buf = Vec::with_capacity(8);

        let Ok(_) = AsyncBufReadExt::read_until(src, b'\n', &mut buf).await else {
            return Err(Error::FatalClientErr(
                "E_EOF".into(),
                "Unexcepted EOF".into(),
            ));
        };
        buf.trim_ascii_end(); // 去掉最后的\n和可能会出现的\r

        let mut sp = buf.splitn(2, |b| b' ' == *b);
        let Some(cmd) = sp.next() else {
            return Err(Error::FatalClientErr(
                "E_BAD_CMD".into(),
                "Invalid command".into(),
            ));
        };
        match cmd {
            b"CLS" => Ok(FrameSub::CLS),
            b"FIN" => {
                buf.drain(..3);
                parse_fin(buf)
            }
            b"NOP" => Ok(FrameSub::NOP),
            b"REQ" => {
                buf.drain(..3);
                parse_req(buf)
            }
            b"RDY" => {
                buf.drain(..3);
                parse_rdy(buf)
            }
            b"TOUCH" => {
                buf.drain(..5);
                parse_touch(buf)
            }
            _ => Err(Error::FatalClientErr(
                "E_BAD_CMD".into(),
                "Invalid command".into(),
            )),
        }
    }
}

fn parse_touch(buf: Vec<u8>) -> Result<FrameSub, Error> {
    if buf.len() != MSG_ID_LENGTH {
        return Err(Error::FatalClientErr(
            "E_INVALID".into(),
            "Invalid message ID".into(),
        ));
    }
    let mut msg_id = [0u8; MSG_ID_LENGTH];
    msg_id.copy_from_slice(&buf[..MSG_ID_LENGTH]);
    Ok(FrameSub::TOUCH(msg_id))
}

fn parse_rdy(buf: Vec<u8>) -> Result<FrameSub, Error> {
    let err = Err(Error::FatalClientErr(
        "E_INVALID".into(),
        "Invalid message timeout".into(),
    ));

    // 从一个数字字符串解析出u64
    let Ok(num_str) = String::from_utf8(buf) else {
        return err;
    };
    let Ok(count) = num_str.parse::<u64>() else {
        return err;
    };
    Ok(FrameSub::RDY(count as usize))
}

fn parse_fin(buf: Vec<u8>) -> Result<FrameSub, Error> {
    if buf.len() != 16 {
        return Err(Error::FatalClientErr(
            "E_INVALID".into(),
            "Invalid message ID".into(),
        ));
    }
    let mut msg_id = [0u8; MSG_ID_LENGTH];
    msg_id.copy_from_slice(&buf[..MSG_ID_LENGTH]);
    Ok(FrameSub::FIN(msg_id))
}

fn parse_req(mut buf: Vec<u8>) -> Result<FrameSub, Error> {
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

    Ok(FrameSub::REQ(msg_id, read_timeout(buf)?))
}

async fn parse_dpub<R>(src: &mut R, mut buf: Vec<u8>) -> Result<Frame, Error>
where
    R: AsyncBufReadExt + Unpin,
{
    let mut sp = buf.splitn(2, |b| b' ' == *b);
    let topic_name = sp.next().unwrap_or(&Vec::new()).to_vec();
    check_name(&topic_name)?;
    let topic_name = String::from_utf8(topic_name).unwrap();

    // 剩下的字节直接转移所有权，避免内存分配
    buf.drain(..topic_name.len() + 1);
    let timeout = read_timeout(buf)?;

    let msg_body = read_msg_body(src).await?;

    Ok(Frame::DPUB(topic_name, timeout, msg_body))
}

async fn parse_mpub<R>(src: &mut R, topic_name: Vec<u8>) -> Result<Frame, Error>
where
    R: AsyncBufReadExt + Unpin,
{
    check_name(&topic_name)?;

    // 所有消息的长度之和
    let Ok(_) = src.read_u32().await else {
        return Err(Error::FatalClientErr(
            "E_BAD_BODY".into(),
            "MPUB failed to read body size".into(),
        ));
    };
    // 消息总数
    let msg_num = match src.read_u32().await {
        Ok(msg_num) if msg_num > 0 => Ok(msg_num),
        _ => Err(Error::FatalClientErr(
            "E_BAD_BODY".into(),
            "MPUB invalid message count".into(),
        )),
    }?;
    let mut msgs = Vec::with_capacity(msg_num as usize);
    for _ in 0..msg_num {
        msgs.push(read_msg_body(src).await?);
    }

    Ok(Frame::MPUB(String::from_utf8(topic_name).unwrap(), msgs))
}

fn parse_sub(buf: Vec<u8>) -> Result<Frame, Error> {
    let mut sp = buf.splitn(2, |b| b' ' == *b);
    let topic_name = sp.next().unwrap_or(&Vec::new()).to_vec();
    check_name(&topic_name)?;

    let channel_name = sp.next().unwrap_or(&Vec::new()).to_vec();
    check_name(&channel_name)?;

    let topic_name = String::from_utf8(topic_name).unwrap();
    let channel_name = String::from_utf8(channel_name).unwrap();

    Ok(Frame::SUB(topic_name, channel_name))
}

async fn parse_pub<R>(src: &mut R, topic_name: Vec<u8>) -> Result<Frame, Error>
where
    R: AsyncBufReadExt + Unpin,
{
    check_name(&topic_name)?;

    Ok(Frame::PUB(
        String::from_utf8(topic_name).unwrap(),
        read_msg_body(src).await?,
    ))
}

lazy_static! {
    static ref NAME_REGEX: Regex = Regex::new(r"^[.a-zA-Z0-9_-]+(#ephemeral)?$").unwrap();
}

// 检查topic或channel名称是否合法
// 字符组成：.a-zA-Z0-9_-
// 长度：[2, 64]
fn check_name(name: &[u8]) -> Result<(), Error> {
    match name.len() {
        2..=64 if NAME_REGEX.is_match(name) => Ok(()),
        _ => Err(Error::FatalClientErr(
            "E_BAD_TOPIC".into(),
            format!("PUB topic name {:?} is not valid", name),
        )),
    }
}

// 从一个数字字符串解析出毫秒数
fn read_timeout(buf: Vec<u8>) -> Result<Duration, Error> {
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
    Ok(Duration::from_millis(ms))
}

//             msg frame
//	[x][x][x][x][x][x][x][x][x][x][x]...
//	|  (int32) ||   (binary)
//	|  4-byte  ||    N-byte
//	---------------------------------...
//	 body size      msg body
async fn read_msg_body<R>(src: &mut R) -> Result<Bytes, Error>
where
    R: AsyncBufReadExt + Unpin,
{
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
    Ok(msg_body.into())
}
