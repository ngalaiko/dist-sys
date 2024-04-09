use crate::ids;

use serde::{
    de::{DeserializeOwned, Error as SerdeError},
    Deserialize, Serialize,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    src: ids::PeerId,
    dest: ids::PeerId,
    body: serde_json::Map<String, serde_json::Value>,
}

impl Message {
    pub fn source(&self) -> &ids::PeerId {
        &self.src
    }

    pub fn in_reply_to(&self) -> Option<u64> {
        if let serde_json::Value::Number(in_reply_to) = self.body.get("in_reply_to")? {
            in_reply_to.as_u64()
        } else {
            None
        }
    }

    pub fn clone_into<P: DeserializeOwned>(&self) -> Result<P, serde_json::Error> {
        serde_json::from_value(serde_json::Value::Object(self.body.clone()))
    }

    pub fn request_to<B: Serialize>(
        src: ids::NodeId,
        dest: ids::PeerId,
        msg_id: u64,
        payload: B,
    ) -> Result<Self, serde_json::Error> {
        let serde_json::Value::Object(body) = serde_json::to_value(Request { msg_id, payload })?
        else {
            unreachable!()
        };

        Ok(Self {
            src: src.into(),
            dest,
            body,
        })
    }

    pub fn reply_for<B: Serialize>(
        message: &Message,
        payload: B,
    ) -> Result<Self, serde_json::Error> {
        let Some(msg_id) = message.body.get("msg_id") else {
            return Err(serde_json::Error::custom("message is not a request"));
        };

        let Some(request_type) = message.body.get("type") else {
            return Err(serde_json::Error::custom("message.type is undefined"));
        };
        let serde_json::Value::String(request_type) = request_type else {
            return Err(serde_json::Error::custom("message.type is not a string"));
        };

        let serde_json::Value::Object(mut body) = serde_json::to_value(payload)? else {
            return Err(serde_json::Error::custom("payload is not an object"));
        };

        body.insert(String::from("in_reply_to"), msg_id.clone());
        body.insert(
            String::from("type"),
            serde_json::Value::String(format!("{request_type}_ok")),
        );

        Ok(Self {
            src: message.dest,
            dest: message.src,
            body,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request<P> {
    pub msg_id: u64,
    #[serde(flatten)]
    pub payload: P,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub in_reply_to: u64,
    #[serde(flatten)]
    pub payload: serde_json::Map<String, serde_json::Value>,
}

impl Response {
    pub fn into<P: DeserializeOwned>(self) -> Result<P, serde_json::Error> {
        serde_json::from_value(serde_json::Value::Object(self.payload))
    }
}
