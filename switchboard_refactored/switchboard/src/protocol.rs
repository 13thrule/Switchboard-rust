//! Binary protocol definition and zero-copy frame parser.

use bytes::{Buf, Bytes};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("unknown message type: 0x{0:02x}")]
    UnknownMessageType(u8),

    #[error("publish payload too short to contain topic_len field")]
    PublishTooShort,

    #[error("publish topic_len ({0}) exceeds available payload bytes ({1})")]
    TopicLenOverflow(usize, usize),

    #[error("topic is not valid UTF-8: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),

    #[error("frame length field is zero")]
    EmptyFrame,
}

#[derive(Debug, Clone)]
pub enum Frame {
    Subscribe {
        topic: Bytes,
    },
    Publish {
        topic: Bytes,
        payload: Bytes,
    },
    /// Queue Subscribe (Consumer Group) - Frame type 0x03
    /// Format: [0x03] + [topic_len (2 bytes)] + [topic] + [group_name]
    QueueSubscribe {
        topic: Bytes,
        group: Bytes,
    },
}

impl Frame {
    pub fn parse(mut raw: Bytes) -> Result<Self, ProtocolError> {
        if raw.len() >= 4 {
            let length_prefix = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
            if length_prefix == raw.len() - 4 {
                raw.advance(4);
            }
        }

        if raw.is_empty() {
            return Err(ProtocolError::EmptyFrame);
        }

        let msg_type = raw.get_u8();

        match msg_type {
            0x01 => {
                let topic = raw;
                std::str::from_utf8(&topic)?;
                Ok(Frame::Subscribe { topic })
            }
            0x02 => {
                if raw.remaining() < 2 {
                    return Err(ProtocolError::PublishTooShort);
                }
                let topic_len = raw.get_u16() as usize;

                let remaining = raw.remaining();
                if topic_len > remaining {
                    return Err(ProtocolError::TopicLenOverflow(topic_len, remaining));
                }

                let topic   = raw.slice(..topic_len);
                let payload = raw.slice(topic_len..);

                std::str::from_utf8(&topic)?;

                Ok(Frame::Publish { topic, payload })
            }
            0x03 => {
                // Queue Subscribe: [type (1)] [topic_len (2)] [topic (N)] [group (remaining)]
                if raw.remaining() < 2 {
                    return Err(ProtocolError::PublishTooShort);
                }
                let topic_len = raw.get_u16() as usize;

                let remaining = raw.remaining();
                if topic_len > remaining {
                    return Err(ProtocolError::TopicLenOverflow(topic_len, remaining));
                }

                let topic = raw.slice(..topic_len);
                let group = raw.slice(topic_len..);

                std::str::from_utf8(&topic)?;
                std::str::from_utf8(&group)?;

                Ok(Frame::QueueSubscribe { topic, group })
            }
            other => Err(ProtocolError::UnknownMessageType(other)),
        }
    }

    pub fn topic(&self) -> &Bytes {
        match self {
            Frame::Subscribe { topic }                => topic,
            Frame::Publish   { topic, .. }            => topic,
            Frame::QueueSubscribe { topic, .. }       => topic,
        }
    }
}

pub fn encode_subscribe(topic: &str) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let length = 1 + topic_bytes.len();
    let mut buf = Vec::with_capacity(4 + length);
    buf.extend_from_slice(&(length as u32).to_be_bytes());
    buf.push(0x01);
    buf.extend_from_slice(topic_bytes);
    buf
}

pub fn encode_publish(topic: &str, message: &[u8]) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let length = 1 + 2 + topic_bytes.len() + message.len();
    let mut buf = Vec::with_capacity(4 + length);
    buf.extend_from_slice(&(length as u32).to_be_bytes());
    buf.push(0x02);
    buf.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(topic_bytes);
    buf.extend_from_slice(message);
    buf
}

/// Encode a Queue Subscribe frame (Consumer Group subscription)
/// Format: [4-byte length prefix] + [0x03] + [2-byte topic_len] + [topic] + [group_name]
pub fn encode_queue_subscribe(topic: &str, group: &str) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let group_bytes = group.as_bytes();
    let length = 1 + 2 + topic_bytes.len() + group_bytes.len();
    let mut buf = Vec::with_capacity(4 + length);
    buf.extend_from_slice(&(length as u32).to_be_bytes());
    buf.push(0x03);
    buf.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(topic_bytes);
    buf.extend_from_slice(group_bytes);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn make_bytes(data: Vec<u8>) -> Bytes {
        BytesMut::from(data.as_slice()).freeze()
    }

    #[test]
    fn roundtrip_subscribe() {
        let encoded = encode_subscribe("telemetry");
        let raw = make_bytes(encoded[4..].to_vec());
        let frame = Frame::parse(raw).unwrap();
        match frame {
            Frame::Subscribe { topic } => {
                assert_eq!(&topic[..], b"telemetry");
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn roundtrip_publish() {
        let msg = b"hello world";
        let encoded = encode_publish("telemetry", msg);
        let raw = make_bytes(encoded[4..].to_vec());
        let frame = Frame::parse(raw).unwrap();
        match frame {
            Frame::Publish { topic, payload } => {
                assert_eq!(&topic[..],   b"telemetry");
                assert_eq!(&payload[..], b"hello world");
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn parse_prefixed_publish() {
        let encoded = encode_publish("telemetry", b"hello world");
        let raw = make_bytes(encoded);
        let frame = Frame::parse(raw).unwrap();
        match frame {
            Frame::Publish { topic, payload } => {
                assert_eq!(&topic[..],   b"telemetry");
                assert_eq!(&payload[..], b"hello world");
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn unknown_type_is_error() {
        let raw = make_bytes(vec![0xFF, 0x01, 0x02]);
        assert!(matches!(
            Frame::parse(raw),
            Err(ProtocolError::UnknownMessageType(0xFF))
        ));
    }

    #[test]
    fn publish_too_short_is_error() {
        let raw = make_bytes(vec![0x02]);
        assert!(matches!(
            Frame::parse(raw),
            Err(ProtocolError::PublishTooShort)
        ));
    }

    #[test]
    fn bytes_clone_is_zero_copy() {
        let msg = b"payload data";
        let encoded = encode_publish("t", msg);
        let raw = make_bytes(encoded[4..].to_vec());

        let ptr_before = raw.as_ptr();

        let frame = Frame::parse(raw).unwrap();
        let clone = frame.clone();

        match (frame, clone) {
            (Frame::Publish { payload: p1, .. }, Frame::Publish { payload: p2, .. }) => {
                assert!(p1.as_ptr() >= ptr_before);
                assert!(p2.as_ptr() >= ptr_before);
                assert_eq!(p1, p2);
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn roundtrip_queue_subscribe() {
        let encoded = encode_queue_subscribe("tasks", "workers");
        let raw = make_bytes(encoded[4..].to_vec());
        let frame = Frame::parse(raw).unwrap();
        match frame {
            Frame::QueueSubscribe { topic, group } => {
                assert_eq!(&topic[..], b"tasks");
                assert_eq!(&group[..], b"workers");
            }
            _ => panic!("expected QueueSubscribe"),
        }
    }

    #[test]
    fn queue_subscribe_with_prefix() {
        let encoded = encode_queue_subscribe("images", "gpu_cluster");
        let raw = make_bytes(encoded);
        let frame = Frame::parse(raw).unwrap();
        match frame {
            Frame::QueueSubscribe { topic, group } => {
                assert_eq!(&topic[..], b"images");
                assert_eq!(&group[..], b"gpu_cluster");
            }
            _ => panic!("expected QueueSubscribe"),
        }
    }
}
