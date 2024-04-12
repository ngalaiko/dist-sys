mod topology;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use tokio::sync::RwLock;
use tokio::time;
use tokio::time::Duration;
use tokio::{spawn, sync};

use maelstrom_node::{ids, protocol, read_from_stdin, write_to_stdout, Handler, Node};

#[derive(Default, Clone)]
struct BroadcastHandler {
    messages: Arc<RwLock<HashSet<u64>>>,
    broadcast_to: Arc<RwLock<Vec<ids::NodeId>>>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename = "topology")]
struct TopologyRequest {
    topology: HashMap<ids::NodeId, Vec<ids::NodeId>>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename = "broadcast")]
struct BroadcastRequest {
    message: u64,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename = "broadcast_ok")]
struct BroadcastOkResponse {}

#[derive(Deserialize)]
#[serde(tag = "type", rename = "read")]
struct ReadRequest {}

impl Handler for BroadcastHandler {
    async fn handle(&self, node: maelstrom_node::Node, message: maelstrom_node::protocol::Message) {
        if let Ok(request) = message.clone_into::<protocol::Request<TopologyRequest>>() {
            let t = topology::Topology::from(&request.payload.topology);
            {
                *self.broadcast_to.write().await = t.next(node.id);
            }
            node.reply(&message, json!({}))
                .await
                .expect("failed to send reply")
        } else if let Ok(request) = message.clone_into::<protocol::Request<BroadcastRequest>>() {
            if self
                .messages
                .read()
                .await
                .contains(&request.payload.message)
            {
                node.reply(&message, json!({}))
                    .await
                    .expect("failed to send reply");
            } else {
                {
                    // Remember the message
                    self.messages.write().await.insert(request.payload.message);
                }

                node.reply(&message, json!({}))
                    .await
                    .expect("failed to reply");

                let broadcast_to = if let ids::PeerId::Node(src_id) = message.source() {
                    self.broadcast_to
                        .read()
                        .await
                        .clone()
                        .iter()
                        .copied()
                        .filter(|node_id|
                            // Do not broadcast back to the sender
                            !src_id.eq(node_id))
                        .collect()
                } else {
                    self.broadcast_to.read().await.clone()
                };

                let broadcasts = broadcast_to.into_iter().map(|node_id| {
                    spawn({
                        let node = node.clone();
                        async move {
                            let mut timeout_ms = 100;
                            loop {
                                let response = node.send::<BroadcastOkResponse>(
                                    node_id,
                                    BroadcastRequest {
                                        message: request.payload.message,
                                    },
                                );
                                let Ok(response) =
                                    time::timeout(Duration::from_millis(timeout_ms), response)
                                        .await
                                else {
                                    timeout_ms = (timeout_ms as f64 * 1.5) as u64;
                                    continue;
                                };

                                if response.is_err() {
                                    timeout_ms = (timeout_ms as f64 * 1.5) as u64;
                                    continue;
                                }

                                break;
                            }
                        }
                    })
                });

                futures::future::join_all(broadcasts).await;
            }
        } else if message
            .clone_into::<protocol::Request<ReadRequest>>()
            .is_ok()
        {
            let messages = { self.messages.read().await.clone() };
            node.reply(&message, json!({"messages": messages}))
                .await
                .expect("failed to send message");
        } else {
            // Ignore unknown requests
        }
    }
}

#[tokio::main]
async fn main() {
    let mut requests_rx = read_from_stdin().await;

    let (responses_tx, responses_rx) = sync::mpsc::channel(100);
    let handle = spawn(write_to_stdout(responses_rx));

    let node = Node::initialize(&mut requests_rx, responses_tx.clone()).await;
    node.listen(&mut requests_rx, BroadcastHandler::default())
        .await;

    handle.await.expect("Task panic");
}
