//! Identificadores del dominio.
//!
//! Cada entidad relevante tiene su propio tipo en vez de un `u32`/`String`
//! pelado, para que el compilador no deje mezclar un id de estación con uno de
//! bicicleta.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Identifica a una estación. Es ordenable porque el algoritmo de elección de
/// líder (Ring) elige al de id más alto.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EstacionId(pub u32);

/// Identifica a una bicicleta. Las estaciones la reconocen cuando llega a un slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BiciId(pub u32);

/// Identifica un alquiler (un ciclo completo retiro → devolución).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RentalId(pub String);

/// Identifica un evento dirigido al líder. Sirve para deduplicar reintentos.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(pub String);

/// Identifica una transacción del 2PC de alquiler.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransaccionId(pub String);

/// Identifica a un usuario de la app.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UsuarioId(pub String);

/// Datos de la tarjeta que el usuario usa para pagar. Es un mock: la pasarela
/// los valida de forma simulada.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatosTarjeta {
    pub numero: String,
    pub titular: String,
    pub vencimiento: String,
    pub cvv: String,
}

impl fmt::Display for EstacionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "estacion#{}", self.0)
    }
}

impl fmt::Display for BiciId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bici#{}", self.0)
    }
}
