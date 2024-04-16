use std::sync::{atomic, Arc};

use serde::Deserialize;
use serde_json::json;
use tokio::{spawn, sync};

use maelstrom_node::{
    ids, protocol, read_from_stdin, write_to_stdout, ErrorCode, Handler, Node, SendError,
};

#[derive(Clone)]
struct GCounterHandler {
    store: kv::SeqKV,

    counter: Arc<atomic::AtomicI64>,
    delta: Arc<atomic::AtomicI64>,
}

impl GCounterHandler {
    pub fn new(store: kv::SeqKV) -> Self {
        Self {
            store,
            counter: Arc::new(atomic::AtomicI64::new(0)),
            delta: Arc::new(atomic::AtomicI64::new(0)),
        }
    }
}

impl GCounterHandler {
    async fn fetch_node_delta(
        &self,
        node: &maelstrom_node::Node,
        node_id: ids::NodeId,
    ) -> Result<i64, SendError> {
        if node_id == node.id {
            let delta = self.delta.fetch_add(0, atomic::Ordering::SeqCst);
            Ok(delta)
        } else {
            // by writing a value before read, we force sequentially consistent store
            // to return the latest value
            self.store
                .write(
                    format!("{node_id}_seq"),
                    self.counter.fetch_add(1, atomic::Ordering::SeqCst),
                )
                .await?;
            match self.store.read::<i64>(node_id).await {
                Ok(delta) => Ok(delta),
                Err(SendError::Response(error)) if error.code == ErrorCode::KeyDoesNotExist => {
                    Ok(0)
                }
                Err(error) => Err(error),
            }
        }
    }
}

impl Handler for GCounterHandler {
    async fn handle(&self, node: maelstrom_node::Node, message: maelstrom_node::protocol::Message) {
        #[derive(Deserialize)]
        #[serde(tag = "type", rename = "add")]
        struct AddRequest {
            delta: i64,
        }

        #[derive(Deserialize)]
        #[serde(tag = "type", rename = "read")]
        struct ReadRequest {}

        if let Ok(request) = message.clone_into::<protocol::Request<AddRequest>>() {
            let counter = self
                .delta
                .fetch_add(request.payload.delta, atomic::Ordering::SeqCst);
            self.store
                .write(node.id, counter + request.payload.delta)
                .await
                .unwrap();
            node.reply(&message, json!({}))
                .await
                .expect("failed to reply");
        } else if message
            .clone_into::<protocol::Request<ReadRequest>>()
            .is_ok()
        {
            let counter = futures::future::try_join_all(
                node.node_ids
                    .iter()
                    .map(|node_id| self.fetch_node_delta(&node, *node_id)),
            )
            .await
            .unwrap()
            .into_iter()
            .sum::<i64>();

            node.reply(&message, json!({"value": counter}))
                .await
                .expect("failed to respond");
        }
    }
}

#[tokio::main]
async fn main() {
    simplelog::TermLogger::init(
        log::LevelFilter::Info,
        simplelog::Config::default(),
        simplelog::TerminalMode::Stderr,
        simplelog::ColorChoice::Auto,
    )
    .expect("Logger init error");

    let mut requests_rx = read_from_stdin().await;

    let (responses_tx, responses_rx) = sync::mpsc::channel(100);
    let handle = spawn(write_to_stdout(responses_rx));

    let node = Node::initialize(&mut requests_rx, responses_tx.clone()).await;
    let store = kv::SeqKV::new(node.clone());

    node.listen(&mut requests_rx, GCounterHandler::new(store))
        .await;

    handle.await.expect("Task panic");
}
