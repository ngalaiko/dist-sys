use std::collections::HashMap;
use std::future::Future;
use std::sync::{atomic, Arc};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::{io, sync, spawn};
use tokio::sync::RwLock;

pub mod ids;
pub mod protocol;

pub trait Handler {
    fn handle(
        &self,
        node: Node,
        message: protocol::Message,
        out: sync::mpsc::Sender<protocol::Message>,
    ) -> impl Future<Output = ()> + Send;
}

pub async fn write_to_stdout(mut responses_rx: sync::mpsc::Receiver<protocol::Message>) {
    let mut stdout = io::stdout();
    while let Some(response) = responses_rx.recv().await {
        let raw = serde_json::to_string(&response).expect("JSON serialize error");
        stdout.write_all(raw.as_bytes()).await.expect("IO error");
        stdout.write_all(b"\n").await.expect("IO error");
    }
}

pub async fn read_from_stdin() -> sync::mpsc::Receiver<protocol::Message> {
    let (tx, rx) = sync::mpsc::channel(100);
    tokio::spawn(async move {
        let reader = io::BufReader::new(io::stdin());
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await.expect("IO error") {
            let Ok(message) = serde_json::from_str(&line) else {
                continue
            };
            tx.send(message).await.expect("Channel error");
        }
    });
    rx
}

pub async fn wait_for_init(
    messages_rx: &mut sync::mpsc::Receiver<protocol::Message>,
    responses_tx: sync::mpsc::Sender<protocol::Message>,
) -> Node {
    loop {
        let Some(message) = messages_rx.recv().await else {
            continue;
        };
        #[derive(Clone, Deserialize)]
        #[serde(tag = "type", rename = "init")]
        struct InitRequest {
            node_id: ids::NodeId,
        }
        let Ok(request) = message.clone_into::<protocol::Request<InitRequest>>() else {
            continue;
        };
        let response =
            protocol::Message::reply_for(&message, json!({})).expect("failed to make response");
        responses_tx.send(response).await.expect("Send failed");
        return Node::new(request.payload.node_id);
    }
}

#[derive(Clone)]
pub struct Node {
    pub id: ids::NodeId,

    latest_message_id: Arc<atomic::AtomicU64>,

    waiting_for: Arc<RwLock<HashMap<u64, sync::oneshot::Sender<protocol::Response>>>>,
}

impl Node {
    fn new(id: ids::NodeId) -> Self {
        Self {
            id,
            latest_message_id: Arc::new(atomic::AtomicU64::new(0)),
            waiting_for: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn listen(
        &self,
        messages_rx: &mut sync::mpsc::Receiver<protocol::Message>,
        responses_tx: sync::mpsc::Sender<protocol::Message>,
        handler: impl Handler + Send + Clone + 'static,
    ) {
        while let Some(message) = messages_rx.recv().await {
            if let Ok(response) = message.clone_into::<protocol::Response>() {
                if let Some(tx) = self.waiting_for.write().await.remove(&response.in_reply_to) {
                    // Forward the reply to the waiting task
                    let _ = tx.send(response);
                } else {
                    // Ignore unexpected replies
                }
            } else {
                let node = self.clone();
                let handler = handler.clone();
                let responses_tx = responses_tx.clone();
                spawn(async move {
                    handler.handle(node, message, responses_tx).await;
                });
            }
        }
    }

    pub async fn send<R: DeserializeOwned>(
        &self,
        dest: ids::NodeId,
        body: impl Serialize,
        responses_tx: sync::mpsc::Sender<protocol::Message>,
    ) -> Result<R, serde_json::Error> {
        let msg_id = self
            .latest_message_id
            .fetch_add(1, atomic::Ordering::SeqCst);

        let request = protocol::Message::request_to(self.id, dest.into(), msg_id, body)?;
        responses_tx.send(request).await.expect("Channel error");

        self.wait_for_reply(msg_id).await
    }

    async fn wait_for_reply<R: DeserializeOwned>(
        &self,
        msg_id: u64,
    ) -> Result<R, serde_json::Error> {
        let (tx, rx) = sync::oneshot::channel::<protocol::Response>();
        {
            self.waiting_for.write().await.insert(msg_id, tx);
        }

        let response = rx.await.expect("Channel error");

        {
            self.waiting_for.write().await.remove(&msg_id);
        }

        response.into()
    }
}
