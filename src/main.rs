mod id;

use std::collections::{HashMap, HashSet};
use std::io;
use std::io::{BufRead, Stdout, Write};

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
        id: id::MessageId,
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

struct Node {
    id: id::NodeId,
    ids_counter: u64,
    messages: HashSet<id::MessageId>,
    neighbors: HashSet<id::NodeId>,

    out: Stdout,
}

impl Node {
    fn new(id: id::NodeId, out: Stdout) -> Self {
        Self {
            id,
            ids_counter: 0,
            messages: HashSet::new(),
            neighbors: HashSet::new(),
            out,
        }
    }

    fn generate_id(&mut self) -> id::MessageId {
        self.ids_counter += 1;
        let id = u64::from(self.id) << 32 | self.ids_counter;
        id.into()
    }

    fn broadcast(&mut self, dest: id::NodeId, value: Value) {
        let msg = Message {
            src: self.id.into(),
            dest: dest.into(),
            body: Body {
                msg_id: Some(self.generate_id()),
                in_reply_to: None,
                value,
            },
        };
        send(&mut self.out, msg);
    }

    fn reply(&mut self, src: Message, value: Value) {
        let msg = Message {
            src: self.id.into(),
            dest: src.src,
            body: Body {
                msg_id: None,
                in_reply_to: src.body.msg_id,
                value,
            },
        };
        send(&mut self.out, msg);
    }

    fn handle_message(&mut self, msg: Message) {
        if msg.dest != self.id.into() {
            // Ignore messages not addressed to this node
            return;
        }

        if msg.body.in_reply_to.is_some() {
            // Ignore replies for now
            return;
        }

        let value = match msg.body.value {
            Value::Echo { ref echo } => Value::EchoOk { echo: echo.clone() },
            Value::Generate {} => Value::GenerateOk {
                id: self.generate_id(),
            },
            Value::Broadcast { message } => {
                // store the message
                self.messages.insert(message);

                let neighbors = self.neighbors.clone();
                // broadcast to all neighbors except the sender
                // to avoid infinite loops
                for neighbor in neighbors {
                    if msg.src == neighbor.into() {
                        continue;
                    }
                    self.broadcast(neighbor, Value::Broadcast { message });
                }

                Value::BroadcastOk {}
            }
            Value::Read {} => Value::ReadOk {
                messages: self.messages.iter().copied().collect(),
            },
            Value::Topology { ref topology } => {
                self.neighbors = topology
                    .get(&self.id)
                    .map(|v| v.iter().copied().collect())
                    .unwrap_or_default();
                Value::TopologyOk {}
            }
            _ => Value::Error {
                code: ErrorCode::NotSupported,
                text: "Not supported".to_string(),
            },
        };
        self.reply(msg, value);
    }
}

fn main() {
    TermLogger::init(
        LevelFilter::Info,
        Config::default(),
        simplelog::TerminalMode::Stderr,
        simplelog::ColorChoice::Auto,
    )
    .expect("Logger init error");

    let mut node: Option<Node> = None;
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    while let Some(line) = stdin.lock().lines().next() {
        let line = line.expect("IO error");
        let message = serde_json::from_str::<Message>(&line).expect("JSON parse error");

        if let Some(node) = node.as_mut() {
            node.handle_message(message)
        } else if let Value::Init { node_id, .. } = message.body.value {
            node = Some(Node::new(node_id, io::stdout()));
            let msg = Message {
                src: node_id.into(),
                dest: message.src,
                body: Body {
                    msg_id: None,
                    in_reply_to: message.body.msg_id,
                    value: Value::InitOk {},
                },
            };
            send(&mut stdout, msg);
        } else {
            let msg = Message {
                src: id::NodeId::default().into(),
                dest: message.src,
                body: Body {
                    msg_id: None,
                    in_reply_to: message.body.msg_id,
                    value: Value::Error {
                        code: ErrorCode::PreconditionFailed,
                        text: "node is not initialised".to_string(),
                    },
                },
            };
            send(&mut stdout, msg);
        };
    }
}

fn send(stdout: &mut Stdout, msg: Message) {
    let mut handle = stdout.lock();
    let msg = serde_json::to_string(&msg).expect("JSON serialize error");
    handle.write_all(msg.as_bytes()).expect("IO error");
    handle.write_all(b"\n").expect("IO error");
}
