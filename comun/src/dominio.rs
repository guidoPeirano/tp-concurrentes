//! Tipos de dominio que viajan por la red y que, por lo tanto, comparten varias
//! aplicaciones (la estación los usa internamente y también los manda al líder).

use serde::{Deserialize, Serialize};

use crate::{BiciId, EstacionId, RentalId, Timestamp, UsuarioId};

/// Un alquiler conocido por la estación de origen y por el líder.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Alquiler {
    pub rental_id: RentalId,
    pub bici_id: BiciId,
    pub usuario_id: UsuarioId,
    pub estacion_origen: EstacionId,
    pub inicio: Timestamp,
    pub fin: Option<Timestamp>,
    pub preauth_id: String,
    pub estado: EstadoAlquiler,
}

/// Ciclo de vida de un alquiler: nace `Activo` y se cierra al devolverse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EstadoAlquiler {
    Activo,
    Cerrado,
}

/// Snapshot del estado de una estación que el líder usa para responder consultas
/// de disponibilidad.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InfoEstacion {
    pub estacion_id: EstacionId,
    pub ubicacion: (f64, f64),
    pub bicis_disponibles: u32,
    pub slots_libres: u32,
    pub last_seen: Timestamp,
}
