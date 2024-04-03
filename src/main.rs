mod id;

use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::sync::Mutex;
use std::{io, sync::Arc};

use log::LevelFilter;
use serde::{Deserialize, Serialize};
use simplelog::{Config, TermLogger};

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    src: id::PeerId,
    dest: id::PeerId,
    body: Body,
}

#[derive(Debug, Serialize, Deserialize)]
struct Body {
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<id::MessageId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<id::MessageId>,
    #[serde(flatten)]
    value: Value,
}

#[derive(Debug, Serialize, Deserialize)]
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

    Broadcast {
        message: id::MessageId,
    },
    BroadcastOk {},

    Read {},
    ReadOk {
        messages: Vec<id::MessageId>,
    },

    Generate {},
    GenerateOk {
        id: u64,
    },

    Topology {
        topology: HashMap<id::NodeId, Vec<id::NodeId>>,
    },
    TopologyOk {},

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
    messages: Arc<Mutex<HashSet<id::MessageId>>>,
}

impl Node {
    fn new(id: id::NodeId) -> Self {
        Self {
            id,
            ids_counter: std::sync::atomic::AtomicU64::new(0),
            messages: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn generate_id(&self) -> u64 {
        let count = self
            .ids_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        u64::from(self.id) << 32 | count
    }

    fn store_message(&self, message: id::MessageId) {
        self.messages
            .lock()
            .expect("Mutex lock error")
            .insert(message);
    }

    fn read_messages(&self) -> Vec<id::MessageId> {
        self.messages
            .lock()
            .expect("Mutex lock error")
            .iter()
            .copied()
            .collect()
    }
}

fn main() {
    let mut node: Option<Node> = None;
    let stdin = io::stdin();

    TermLogger::init(
        LevelFilter::Info,
        Config::default(),
        simplelog::TerminalMode::Stderr,
        simplelog::ColorChoice::Auto,
    ).expect("Logger init error");

    while let Some(line) = stdin.lock().lines().next() {
        let line = line.expect("IO error");
        log::info!("{:?}", line);

        let msg = serde_json::from_str::<Message>(&line).expect("JSON parse error");

        let value = if let Some(node) = node.as_ref() {
            match msg.body.value {
                Value::Echo { echo } => Value::EchoOk { echo },
                Value::Generate {} => Value::GenerateOk {
                    id: node.generate_id(),
                },
                Value::Broadcast { message } => {
                    node.store_message(message);
                    Value::BroadcastOk {}
                }
                Value::Read {} => Value::ReadOk {
                    messages: node.read_messages(),
                },
                Value::Topology { topology: _ } => Value::TopologyOk {},
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
