use maelstrom_node::{ids, Node, SendError};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Clone)]
pub struct SeqKV {
    node: Node,
}

impl SeqKV {
    pub fn new(node: Node) -> Self {
        Self { node }
    }

    pub async fn read<R: DeserializeOwned>(&self, key: impl ToString) -> Result<R, SendError> {
        #[derive(Serialize)]
        #[serde(tag = "type", rename = "read")]
        struct ReadRequest {
            key: String,
        }
        #[derive(Deserialize)]
        #[serde(tag = "type", rename = "read_ok")]
        struct ReadResponse<R> {
            value: R,
        }

        let response = self
            .node
            .send::<ReadResponse<R>>(
                ids::Store::Seq.into(),
                ReadRequest {
                    key: key.to_string(),
                },
            )
            .await?;

        Ok(response.value)
    }

    pub async fn write(&self, key: impl ToString, value: impl Serialize) -> Result<(), SendError> {
        #[derive(Serialize)]
        #[serde(tag = "type", rename = "write")]
        struct WriteRequest<V> {
            key: String,
            value: V,
        }

        #[derive(Deserialize)]
        #[serde(tag = "type", rename = "write_ok")]
        struct WriteResponse {}

        self.node
            .send::<WriteResponse>(
                ids::Store::Seq.into(),
                WriteRequest {
                    key: key.to_string(),
                    value,
                },
            )
            .await?;

        Ok(())
    }

    pub async fn cas<R: DeserializeOwned>(
        &self,
        key: impl ToString,
        from: impl Serialize,
        to: impl Serialize,
        create_if_not_exists: bool,
    ) -> Result<(), SendError> {
        #[derive(Serialize)]
        #[serde(tag = "type", rename = "cas")]
        struct CasRequest<F, T> {
            key: String,
            from: F,
            to: T,
            create_if_not_exists: bool,
        }
        #[derive(Deserialize)]
        #[serde(tag = "type", rename = "cas_ok")]
        struct CasResponse {}

        self.node
            .send::<CasResponse>(
                ids::Store::Seq.into(),
                CasRequest {
                    key: key.to_string(),
                    from,
                    to,
                    create_if_not_exists,
                },
            )
            .await?;

        Ok(())
    }
}
