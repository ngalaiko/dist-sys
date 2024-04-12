use serde::Deserialize;
use serde_json::json;
use tokio::{spawn, sync};

use maelstrom_node::{protocol, read_from_stdin, write_to_stdout, Handler, Node};

#[derive(Clone)]
struct EchoHandler {}

#[derive(Deserialize)]
#[serde(tag = "type", rename = "echo")]
struct EchoRequest {
    echo: String,
}

impl Handler for EchoHandler {
    async fn handle(&self, node: maelstrom_node::Node, message: maelstrom_node::protocol::Message) {
        let Ok(request) = message.clone_into::<protocol::Request<EchoRequest>>() else {
            return;
        };

        node.reply(&message, json!({"echo": request.payload.echo}))
            .await
            .expect("failed to send reply")
    }
}

#[tokio::main]
async fn main() {
    let mut requests_rx = read_from_stdin().await;

    let (responses_tx, responses_rx) = sync::mpsc::channel(100);
    let handle = spawn(write_to_stdout(responses_rx));

    let node = Node::initialize(&mut requests_rx, responses_tx.clone()).await;
    node.listen(&mut requests_rx, EchoHandler {}).await;

    handle.await.expect("Task panic");
}
