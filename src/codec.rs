//! CBOR bodies and length-prefixed framing for byte streams (a big-endian `u32` length, then
//! the message).

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::ServiceError;

/// Largest message accepted by default (16 MiB). Large content belongs in a chunked transfer,
/// not in one message.
pub const MAX_FRAME: usize = 16 << 20;

/// Encodes a message as CBOR.
pub fn to_cbor<T: Serialize>(msg: &T) -> Result<Vec<u8>, ServiceError> {
    cbor4ii::serde::to_vec(Vec::new(), msg).map_err(|e| ServiceError::protocol(e.to_string()))
}

/// Decodes a CBOR message.
pub fn from_cbor<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ServiceError> {
    cbor4ii::serde::from_slice(bytes).map_err(|e| ServiceError::protocol(e.to_string()))
}

/// One framed message, ready to write.
pub fn encode_frame(message: &[u8], max: usize) -> Result<Vec<u8>, ServiceError> {
    if message.len() > max || message.len() > u32::MAX as usize {
        return Err(ServiceError::protocol(format!("message of {} bytes is over the {max}-byte limit", message.len())));
    }
    let mut out = Vec::with_capacity(4 + message.len());
    out.extend_from_slice(&(message.len() as u32).to_be_bytes());
    out.extend_from_slice(message);
    Ok(out)
}

/// Collects bytes as they are read and hands out whole messages.
#[derive(Debug)]
pub struct FrameDecoder {
    buf: Vec<u8>,
    max: usize,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new(MAX_FRAME)
    }
}

impl FrameDecoder {
    pub fn new(max: usize) -> Self {
        Self { buf: Vec::new(), max }
    }

    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// The next whole message, if one has arrived.
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, ServiceError> {
        if self.buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]) as usize;
        if len > self.max {
            return Err(ServiceError::protocol(format!("incoming message of {len} bytes is over the {}-byte limit", self.max)));
        }
        if self.buf.len() < 4 + len {
            return Ok(None);
        }
        let msg = self.buf[4..4 + len].to_vec();
        self.buf.drain(..4 + len);
        Ok(Some(msg))
    }

    /// Bytes held that are not a whole message yet.
    pub fn pending(&self) -> usize {
        self.buf.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_survive_any_split() {
        let mut wire = encode_frame(b"hello", MAX_FRAME).unwrap();
        wire.extend(encode_frame(b"", MAX_FRAME).unwrap());
        wire.extend(encode_frame("wörld".as_bytes(), MAX_FRAME).unwrap());
        for split in 0..wire.len() {
            let mut d = FrameDecoder::default();
            let mut got = Vec::new();
            for chunk in [&wire[..split], &wire[split..]] {
                d.push(chunk);
                while let Some(m) = d.next_frame().unwrap() {
                    got.push(m);
                }
            }
            assert_eq!(got, [b"hello".to_vec(), Vec::new(), "wörld".as_bytes().to_vec()], "split at {split}");
            assert_eq!(d.pending(), 0);
        }
    }

    #[test]
    fn oversized_frames_are_refused_both_ways() {
        assert!(encode_frame(&[0; 11], 10).is_err());
        let mut d = FrameDecoder::new(10);
        d.push(&11u32.to_be_bytes());
        assert!(matches!(d.next_frame(), Err(ServiceError::Protocol { .. })));
    }
}
