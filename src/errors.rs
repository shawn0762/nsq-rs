use std::io;

use thiserror::Error;

use crate::nsqd::Message;

#[derive(Error, Debug)]
pub enum NsqError {
    #[error("IO error")]
    IoError(#[from] io::Error),

    #[error("Async channel send error")]
    AsyncChannelSendError(Message),

    #[error("Failed to send msgs to topic")]
    TopicMsgSendError(Message),

    #[error("Invalid message size")]
    InvalidMsgLength,

    #[error("The number of subscribers has reached its limit: {0}")]
    MaxSubscriberReached(usize),

    #[error("NSQD is on the way out")]
    Exiting,

    #[error("NSQD Disk queue is not implement")]
    DiskQueueNotImplement,

    #[error("{0} {1}")]
    FatalClientErr(String, String),
}
