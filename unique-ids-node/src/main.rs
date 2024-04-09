use std::sync::atomic;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::json;
use tokio::{spawn, sync};

use maelstrom_node::{protocol, read_from_stdin, wait_for_init, write_to_stdout, Handler};

#[derive(Default, Clone)]
struct UniqueIdsHandler {
    ids_counter: Arc<atomic::AtomicU64>,
}

#[derive(Deserialize)]
#[serde[tag = "generate", rename = "type"]]
struct GenerateRequest {}

impl Handler for UniqueIdsHandler {
    async fn handle(
        &self,
        node: maelstrom_node::Node,
        message: maelstrom_node::protocol::Message,
        out: sync::mpsc::Sender<maelstrom_node::protocol::Message>,
    ) {
        let Ok(_) = message.clone_into::<protocol::Request<GenerateRequest>>() else {
            return;
        };

        let counter = self.ids_counter.fetch_add(1, atomic::Ordering::SeqCst);

        // This gives us 2^32 unique ids for every of 2^32 nodes.
        let id = u64::from(node.id) << 32 | counter;

        let response = maelstrom_node::protocol::Message::reply_for(&message, json!({"id": id}))
            .expect("Message panic");
        out.send(response).await.expect("Channel panic");
    }
}

#[tokio::main]
async fn main() {
    let mut requests_rx = read_from_stdin().await;

    let (responses_tx, responses_rx) = sync::mpsc::channel(100);
    let handle = spawn(write_to_stdout(responses_rx));

    let node = wait_for_init(&mut requests_rx, responses_tx.clone()).await;
    node.listen(&mut requests_rx, responses_tx, UniqueIdsHandler::default())
        .await;

    handle.await.expect("Task panic");
}
