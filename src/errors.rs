use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum NsqError<T> {
    #[error("IO error")]
    IoError(#[from] io::Error),

    #[error("Invalid message size")]
    AsyncChannelSendError(#[from] async_channel::SendError<T>),

    #[error("Invalid message size")]
    InvalidMsgLength,

    #[error("The number of subscribers has reached its limit: {0}")]
    MaxSubscriberReached(usize),

    #[error("NSQD is on the way out")]
    Exiting,

    #[error("NSQD Disk queue is not implement")]
    DiskQueueNotImplement,
}
