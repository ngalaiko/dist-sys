mod id;

use std::io;
use std::io::BufRead;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Message {
    src: id::PeerId,
    dest: id::PeerId,
    body: Body,
}

#[derive(Serialize, Deserialize)]
struct Body {
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<id::MessageId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<id::MessageId>,
    #[serde(flatten)]
    value: Value,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Value {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },

    Init {
        node_id: id::NodeId,
        node_ids: Vec<id::NodeId>,
    },
    InitOk {},

    Generate {},
    GenerateOk {
        id: u64,
    },

    Error {
        code: ErrorCode,
        text: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
enum ErrorCode {
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

#[derive(Debug)]
struct Error {
    code: ErrorCode,
    text: String,
}

impl From<Error> for Value {
    fn from(error: Error) -> Self {
        Value::Error {
            code: error.code,
            text: error.text,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.text)
    }
}

impl std::error::Error for Error {}

struct Node {
    id: id::NodeId,

    ids_counter: std::sync::atomic::AtomicU64,
}

impl Node {
    fn new(id: id::NodeId) -> Self {
        Self {
            id,
            ids_counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn generate_id(&self) -> u64 {
        let count = self.ids_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        u64::from(self.id) << 32 | count
    }
}

fn main() {
    let mut node: Option<Node> = None;
    let stdin = io::stdin();

    while let Some(line) = stdin.lock().lines().next() {
        let line = line.expect("IO error");
        let msg = serde_json::from_str::<Message>(&line).expect("JSON parse error");

        let value = if let Some(node) = node.as_ref() { 
            match msg.body.value {
                Value::Echo { echo } => Value::EchoOk { echo },
                Value::Generate {  } => Value::GenerateOk { 
                    id: node.generate_id()
                },
                _ => Value::Error {
                    code: ErrorCode::NotSupported,
                    text: "Not supported".to_string(),
                },
            }
        } else if let Value::Init { node_id, .. } = msg.body.value {
            node = Some(Node::new(node_id));
            Value::InitOk {}
        } else {
            Value::Error {
                code: ErrorCode::PreconditionFailed,
                text: "node is not initialised".to_string(),
            }
        };

        let reply = Message {
            src: id::PeerId::Node(node.as_ref().map(|node| node.id).unwrap_or_default()),
            dest: msg.src,
            body: Body {
                msg_id: Some(id::MessageId::next()),
                in_reply_to: msg.body.msg_id,
                value,
            },
        };

        println!(
            "{}",
            serde_json::to_string(&reply).expect("JSON serialize error")
        );
    }
}