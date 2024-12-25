use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum NsqError {
    #[error("IO error")]
    IoError(#[from] io::Error),

    #[error("Invalid message size")]
    InvalidMsgLength,
}
