use crate::ids;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub src: ids::PeerId,
    pub dest: ids::PeerId,
    pub body: Body,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub value: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Payload {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },

    Init {
        node_id: ids::NodeId,
        node_ids: Vec<ids::NodeId>,
    },
    InitOk {},

    Broadcast {
        message: u64,
    },
    BroadcastOk {},

    Read {},
    ReadOk {
        messages: Vec<u64>,
    },

    Generate {},
    GenerateOk {
        id: u64,
    },

    Topology {
        topology: HashMap<ids::NodeId, Vec<ids::NodeId>>,
    },
    TopologyOk {},

    Error {
        code: ErrorCode,
        text: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorCode {
    Timeout = 0,
    NodeNotFound = 1,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    KeyAlreadyExists = 21,
    PreconditionFailed = 22,
    TransactionConflict = 30,
}
