use std::collections::HashSet;
use std::sync::{atomic, Arc};

use log::LevelFilter;
use simplelog::{Config, TermLogger};
use tokio::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::sync::{self, RwLock};

use maelstrom::{ids, protocol};

#[tokio::main]
async fn main() {
    TermLogger::init(
        LevelFilter::Info,
        Config::default(),
        simplelog::TerminalMode::Stderr,
        simplelog::ColorChoice::Auto,
    )
    .expect("Logger init error");

    log::info!("Node starting");

    let mut requests_rx = read_incoming_messages().await;
    let (responses_tx, mut responses_rx) = sync::mpsc::channel(100);

    let handle = tokio::spawn(async move {
        let mut stdout = io::stdout();
        while let Some(response) = responses_rx.recv().await {
            let raw = serde_json::to_string(&response).expect("JSON serialize error");
            stdout.write_all(raw.as_bytes()).await.expect("IO error");
            stdout.write_all(b"\n").await.expect("IO error");
        }
    });

    if let Some(node) = wait_for_init(&mut requests_rx, responses_tx).await {
        listen(node, &mut requests_rx).await;
    }

    handle.await.expect("Task error");

    log::info!("Node shutting down");
}

async fn read_incoming_messages() -> sync::mpsc::Receiver<protocol::Message> {
    let (tx, rx) = sync::mpsc::channel(100);
    tokio::spawn(async move {
        let stdin = io::stdin();
        let mut lines = io::BufReader::new(stdin).lines();
        while let Some(line) = lines.next_line().await.expect("IO error") {
            let message =
                serde_json::from_str::<protocol::Message>(&line).expect("JSON parse error");
            tx.send(message).await.expect("Channel error");
        }
    });
    rx
}

async fn wait_for_init(
    messages_rx: &mut sync::mpsc::Receiver<protocol::Message>,
    responses_tx: sync::mpsc::Sender<protocol::Message>,
) -> Option<Node> {
    while let Some(message) = messages_rx.recv().await {
        if let protocol::Payload::Init { node_id, .. } = message.body.value {
            let msg = protocol::Message {
                src: node_id.into(),
                dest: message.src,
                body: protocol::Body {
                    msg_id: None,
                    in_reply_to: message.body.msg_id,
                    value: protocol::Payload::InitOk {},
                },
            };
            responses_tx.send(msg).await.expect("Channel error");
            return Some(Node::new(node_id, responses_tx.clone()));
        }
    }
    None
}

async fn listen(node: Node, messages_rx: &mut sync::mpsc::Receiver<protocol::Message>) {
    while let Some(message) = messages_rx.recv().await {
        let mut node = node.clone();
        tokio::spawn(async move {
            node.handle_message(&message).await;
        });
    }
}

#[derive(Clone)]
struct Node {
    id: ids::NodeId,
    ids_counter: Arc<atomic::AtomicU64>,
    messages: Arc<RwLock<HashSet<ids::MessageId>>>,
    neighbors: Arc<RwLock<HashSet<ids::NodeId>>>,

    out: sync::mpsc::Sender<protocol::Message>,
}

impl Node {
    fn new(id: ids::NodeId, out: sync::mpsc::Sender<protocol::Message>) -> Self {
        Self {
            id,
            ids_counter: Arc::new(atomic::AtomicU64::new(0)),
            messages: Arc::new(RwLock::new(HashSet::new())),
            neighbors: Arc::new(RwLock::new(HashSet::new())),
            out,
        }
    }

    fn generate_id(&mut self) -> ids::MessageId {
        let counter = self.ids_counter.fetch_add(1, atomic::Ordering::SeqCst);
        let id = u64::from(self.id) << 32 | counter;
        id.into()
    }

    async fn broadcast(&mut self, dest: ids::NodeId, value: protocol::Payload) {
        let msg = protocol::Message {
            src: self.id.into(),
            dest: dest.into(),
            body: protocol::Body {
                msg_id: None,
                in_reply_to: None,
                value,
            },
        };
        self.out.send(msg).await.expect("Channel error");
    }

    async fn reply(&mut self, src: &protocol::Message, value: protocol::Payload) {
        if let Some(in_reply_to) = src.body.msg_id {
            let msg = protocol::Message {
                src: self.id.into(),
                dest: src.src,
                body: protocol::Body {
                    msg_id: None,
                    in_reply_to: Some(in_reply_to),
                    value,
                },
            };
            self.out.send(msg).await.expect("Channel error");
        }
    }

    async fn handle_message(&mut self, msg: &protocol::Message) {
        if msg.dest != self.id.into() {
            // Ignore messages not addressed to this node
            return;
        }

        if msg.body.in_reply_to.is_some() {
            // Ignore replies for now
            return;
        }

        match &msg.body.value {
            protocol::Payload::Echo { echo } => {
                self.reply(msg, protocol::Payload::EchoOk { echo: echo.clone() })
                    .await;
            }
            protocol::Payload::Generate {} => {
                let id = self.generate_id();
                self.reply(msg, protocol::Payload::GenerateOk { id }).await;
            }
            protocol::Payload::Broadcast { message } => {
                // store the message
                self.messages.write().await.insert(*message);

                let neighbors = self.neighbors.clone();
                // broadcast to all neighbors except the sender
                // to avoid infinite loops
                for neighbor in neighbors.read().await.iter().copied() {
                    if msg.src == neighbor.into() {
                        continue;
                    }
                    self.broadcast(neighbor, protocol::Payload::Broadcast { message: *message })
                        .await;
                }

                self.reply(msg, protocol::Payload::BroadcastOk {}).await;
            }
            protocol::Payload::Read {} => {
                let messages = self.messages.read().await.iter().copied().collect();
                self.reply(msg, protocol::Payload::ReadOk { messages })
                    .await;
            }
            protocol::Payload::Topology { ref topology } => {
                let neighbors = topology
                    .get(&self.id)
                    .map(|v| v.to_vec())
                    .unwrap_or_default();
                self.neighbors.write().await.clear();
                self.neighbors.write().await.extend(neighbors);
                self.reply(msg, protocol::Payload::TopologyOk {}).await;
            }
            _ => {
                self.reply(
                    msg,
                    protocol::Payload::Error {
                        code: protocol::ErrorCode::NotSupported,
                        text: "Not supported".to_string(),
                    },
                )
                .await
            }
        }
    }
}
