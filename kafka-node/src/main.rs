use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::RwLock;
use tokio::{spawn, sync};

use maelstrom_node::{protocol, read_from_stdin, write_to_stdout, Handler, Node};

#[derive(Clone, Default)]
struct KafkaHandler {
    logs: Arc<RwLock<HashMap<String, Vec<u32>>>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Send { key: String, msg: u32 },
    Poll { offsets: HashMap<String, u32> },
    CommitOffsets { offsets: HashMap<String, u32> },
    ListCommittedOffsets { keys: HashSet<String> },
}

impl Handler for KafkaHandler {
    async fn handle(&self, node: maelstrom_node::Node, message: maelstrom_node::protocol::Message) {
        let Ok(request) = message.clone_into::<protocol::Request<Request>>() else {
            return;
        };

        match request.payload {
            Request::Send { key, msg } => {
                let mut logs = self.logs.write().await;
                let log = logs.entry(key).or_default();
                let offset = log.len();
                log.push(msg);
                node.reply(&message, json!({"offset": offset}))
                    .await
                    .expect("failed to send reply");
            }
            Request::Poll { offsets } => {
                let logs = { self.logs.read().await.clone() };

                let msgs = logs
                    .into_iter()
                    .filter_map(|(key, log)| {
                        offsets.get(&key).map(|offset| {
                            let log = log.into_iter().enumerate().collect::<Vec<_>>();
                            let sliced = log[*offset as usize..].to_vec();
                            (key, sliced)
                        })
                    })
                    .collect::<HashMap<_, _>>();

                node.reply(&message, json!({"msgs": msgs}))
                    .await
                    .expect("failed to send reply");
            }
            Request::CommitOffsets { offsets: _ } => {
                node.reply(&message, json!({}))
                    .await
                    .expect("failed to send reply");
            }
            Request::ListCommittedOffsets { keys } => {
                let logs = { self.logs.read().await.clone() };

                let offsets = logs
                    .into_iter()
                    .filter_map(|(key, log)| {
                        if keys.contains(&key) {
                            Some((key, log.len()))
                        } else {
                            None
                        }
                    })
                    .collect::<HashMap<_, _>>();

                node.reply(&message, json!({"offsets": offsets}))
                    .await
                    .expect("failed to send reply");
            }
        };
    }
}

#[tokio::main]
async fn main() {
    let mut requests_rx = read_from_stdin().await;

    let (responses_tx, responses_rx) = sync::mpsc::channel(100);
    let handle = spawn(write_to_stdout(responses_rx));

    let node = Node::initialize(&mut requests_rx, responses_tx.clone()).await;
    node.listen(&mut requests_rx, KafkaHandler::default()).await;

    handle.await.expect("Task panic");
}
