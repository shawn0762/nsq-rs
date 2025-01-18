use tokio_util::codec::{Decoder, Encoder};

pub mod codec_sub_v2;
pub mod codec_v2;
pub mod frame_v2;

// pub use codec_v2::CodecV2;

pub trait Codec<I>: Encoder<I> + Decoder {}
