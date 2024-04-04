use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct MessageId(u64);

impl From<u64> for MessageId {
    fn from(id: u64) -> Self {
        MessageId(id)
    }
}

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

#[derive(Debug, Clone, Copy, Default, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct NodeId(u64);

impl From<NodeId> for u64 {
    fn from(node_id: NodeId) -> u64 {
        node_id.0
    }
}

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
        if let Some(stripped) = s.strip_prefix('n') {
            let num = stripped.parse().map_err(serde::de::Error::custom)?;
            Ok(NodeId(num))
        } else {
            Err(serde::de::Error::custom("NodeId must start with 'n'"))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
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
        if let Some(striped) = s.strip_prefix('c') {
            let num = striped.parse().map_err(serde::de::Error::custom)?;
            Ok(ClientId(num))
        } else {
            Err(serde::de::Error::custom("ClientId must start with 'c'"))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PeerId {
    Node(NodeId),
    Client(ClientId),
}

impl From<NodeId> for PeerId {
    fn from(node_id: NodeId) -> Self {
        PeerId::Node(node_id)
    }
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
        if let Some(stripped) = s.strip_prefix('n') {
            let num = stripped.parse().map_err(serde::de::Error::custom)?;
            Ok(PeerId::Node(NodeId(num)))
        } else if let Some(stripped) = s.strip_prefix('c') {
            let num = stripped.parse().map_err(serde::de::Error::custom)?;
            Ok(PeerId::Client(ClientId(num)))
        } else {
            Err(serde::de::Error::custom("Unknown id type"))
        }
    }
}
