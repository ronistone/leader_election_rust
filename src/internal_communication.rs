use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum InternalMessage {
    AllConnectionsFailed {
    },
    ConnectionFailed {
        peer: u16,
    },
    ConnectionBroken {
        peer: u16
    }
}