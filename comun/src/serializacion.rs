//! Helpers de (de)serialización. Por ahora envolvemos `serde_json`; el framing
//! de los mensajes por la red (prefijo de longitud) se agrega en la Etapa 1.

use serde::{de::DeserializeOwned, Serialize};

/// Serializa cualquier mensaje a JSON.
pub fn a_json<T: Serialize>(valor: &T) -> Result<String, serde_json::Error> {
    serde_json::to_string(valor)
}

/// Reconstruye un mensaje a partir de su JSON.
pub fn desde_json<T: DeserializeOwned>(json: &str) -> Result<T, serde_json::Error> {
    serde_json::from_str(json)
}
