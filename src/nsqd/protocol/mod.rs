use frame::Frame;
use tokio_util::codec::{Decoder, Encoder};

mod codec_v2;
mod frame;

pub enum Version {
    V1,
    V2,
}

pub trait Codec: Encoder<Frame> + Decoder {}
