use std::collections::HashMap;
use std::future::Future;
use std::sync::{atomic, Arc};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tokio::{io, spawn, sync};

pub mod ids;
pub mod protocol;

pub trait Handler {
    fn handle(&self, node: Node, message: protocol::Message) -> impl Future<Output = ()> + Send;
}

pub async fn write_to_stdout(mut responses_rx: sync::mpsc::Receiver<protocol::Message>) {
    let mut stdout = io::stdout();
    while let Some(response) = responses_rx.recv().await {
        let raw = serde_json::to_string(&response).expect("JSON serialize error");
        log::info!("-> {}", raw);
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
            log::info!("<- {}", line);
            match serde_json::from_str(&line) {
                Ok(message) => {
                    tx.send(message).await.expect("Channel error");
                }
                Err(error) => {
                    dbg!(&error);
                    log::error!("failed to parse message: {error}");
                }
            }
        }
    });
    rx
}

#[derive(Clone)]
pub struct Node {
    pub id: ids::NodeId,

    latest_message_id: Arc<atomic::AtomicU64>,
    waiting_for: Arc<RwLock<HashMap<u64, sync::oneshot::Sender<protocol::Response>>>>,

    responses_tx: sync::mpsc::Sender<protocol::Message>,
}

#[derive(Debug)]
pub enum SendError {
    Json(serde_json::Error),
    Response(ErrorResponse),
}

impl From<serde_json::Error> for SendError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<ErrorResponse> for SendError {
    fn from(value: ErrorResponse) -> Self {
        Self::Response(value)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename = "error")]
pub struct ErrorResponse {
    code: ErrorCode,
    text: String,
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.text)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ErrorCode {
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
    TxnConflict = 30,
}

impl Node {
    pub async fn initialize(
        messages_rx: &mut sync::mpsc::Receiver<protocol::Message>,
        responses_tx: sync::mpsc::Sender<protocol::Message>,
    ) -> Self {
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
            return Self::new(request.payload.node_id, responses_tx);
        }
    }

    fn new(id: ids::NodeId, responses_tx: sync::mpsc::Sender<protocol::Message>) -> Self {
        Self {
            id,
            latest_message_id: Arc::new(atomic::AtomicU64::new(0)),
            waiting_for: Arc::new(RwLock::new(HashMap::new())),
            responses_tx,
        }
    }

    pub async fn listen(
        &self,
        requests_tx: &mut sync::mpsc::Receiver<protocol::Message>,
        handler: impl Handler + Send + Clone + 'static,
    ) {
        while let Some(message) = requests_tx.recv().await {
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
                spawn(async move {
                    handler.handle(node, message).await;
                });
            }
        }
    }

    pub async fn reply(
        &self,
        request: &protocol::Message,
        body: impl Serialize,
    ) -> Result<(), serde_json::Error> {
        let response = protocol::Message::reply_for(request, body)?;
        self.responses_tx
            .send(response)
            .await
            .expect("Channel panic");
        Ok(())
    }

    pub async fn send<R: DeserializeOwned>(
        &self,
        dest: ids::PeerId,
        body: impl Serialize,
    ) -> Result<R, SendError> {
        let msg_id = self
            .latest_message_id
            .fetch_add(1, atomic::Ordering::SeqCst);

        let request = protocol::Message::request_to(self.id, dest, msg_id, body)?;
        self.responses_tx
            .send(request)
            .await
            .expect("Channel error");

        let response = self.wait_for_reply(msg_id).await;

        if let Ok(error) = response.clone().into::<ErrorResponse>() {
            Err(error.into())
        } else {
            Ok(response.into::<R>().map_err(SendError::from)?)
        }
    }

    async fn wait_for_reply(&self, msg_id: u64) -> protocol::Response {
        let (tx, rx) = sync::oneshot::channel::<protocol::Response>();
        {
            self.waiting_for.write().await.insert(msg_id, tx);
        }

        let response = rx.await.expect("Channel error");

        {
            self.waiting_for.write().await.remove(&msg_id);
        }

        response
    }
}
