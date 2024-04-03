use std::sync::atomic;

use serde::{Deserialize, Serialize};

static MESSAGE_COUNTER: atomic::AtomicUsize = atomic::AtomicUsize::new(0);

#[derive(Clone, Copy)]
pub struct MessageId(u64);

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for MessageId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MessageId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        u64::deserialize(deserializer).map(MessageId)
    }
}

impl MessageId {
    pub fn next() -> Self {
        MessageId(MESSAGE_COUNTER.fetch_add(1, atomic::Ordering::SeqCst) as u64)
    }
}

#[derive(Clone, Copy, Default)]
pub struct NodeId(u64);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "n{}", self.0)
    }
}

impl Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        if !s.starts_with('n') {
            return Err(serde::de::Error::custom("NodeId must start with 'n'"));
        }
        let num = s[1..].parse().map_err(serde::de::Error::custom)?;
        Ok(NodeId(num))
    }
}

#[derive(Clone, Copy)]
pub struct ClientId(u64);

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "c{}", self.0)
    }
}

impl Serialize for ClientId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ClientId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        if !s.starts_with('c') {
            return Err(serde::de::Error::custom("ClientId must start with 'c'"));
        }
        let num = s[1..].parse().map_err(serde::de::Error::custom)?;
        Ok(ClientId(num))
    }
}

#[derive(Clone, Copy)]
pub enum PeerId {
    Node(NodeId),
    Client(ClientId),
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerId::Node(node_id) => write!(f, "{}", node_id),
            PeerId::Client(client_id) => write!(f, "{}", client_id),
        }
    }
}

impl Serialize for PeerId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PeerId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        if s.starts_with('n') {
            let num = s[1..].parse().map_err(serde::de::Error::custom)?;
            Ok(PeerId::Node(NodeId(num)))
        } else if s.starts_with('c') {
            let num = s[1..].parse().map_err(serde::de::Error::custom)?;
            Ok(PeerId::Client(ClientId(num)))
        } else {
            Err(serde::de::Error::custom(
                "PeerId must start with 'n' or 'c'",
            ))
        }
    }
}
