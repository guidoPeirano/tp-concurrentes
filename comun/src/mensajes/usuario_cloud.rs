//! Usuario ↔ Cloud (TCP). Consulta de disponibilidad.

use serde::{Deserialize, Serialize};

use crate::{InfoEstacion, UsuarioId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeUsuarioACloud {
    ConsultaDisponibilidad {
        usuario_id: UsuarioId,
        ubicacion: (f64, f64),
        radio_max_km: f64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeCloudAUsuario {
    RespuestaDisponibilidad { estaciones: Vec<InfoEstacion> },
    ErrorSistema { motivo: String },
}
