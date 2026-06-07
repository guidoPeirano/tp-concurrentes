//! Cloud ↔ Líder (TCP). El cloud reenvía consultas y descubre quién es el líder.

use serde::{Deserialize, Serialize};

use crate::{EstacionId, InfoEstacion};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeCloudAEstacion {
    ConsultaDisponibilidad { ubicacion: (f64, f64), radio_max_km: f64 },
    PreguntarLider,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeEstacionACloud {
    RespuestaDisponibilidad { estaciones: Vec<InfoEstacion> },
    RespuestaLider { lider_id: Option<EstacionId>, term: u64 },
    SoyElLider { estacion_id: EstacionId, term: u64 },
}
