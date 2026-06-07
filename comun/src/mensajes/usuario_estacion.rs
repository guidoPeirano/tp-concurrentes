//! Usuario ↔ Estación (TCP). Toda la interacción del usuario pasa por acá: las
//! operaciones (alquiler/devolución) y las consultas (discovery del líder y
//! disponibilidad). Antes había un proceso `cloud` para las consultas; se
//! eliminó (era un punto único de falla) y el usuario habla directo con las
//! estaciones.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::{BiciId, DatosTarjeta, EstacionId, InfoEstacion, RentalId, UsuarioId};

/// Envelope que multiplexa las dos familias de mensajes que el usuario manda a
/// una misma estación por el mismo endpoint TCP.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeUsuario {
    Operacion(MensajeUsuarioAEstacion),
    Consulta(MensajeUsuarioAEstacionConsulta),
}

// --- Operaciones: alquiler y devolución ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeUsuarioAEstacion {
    SolicitudAlquiler {
        usuario_id: UsuarioId,
        slot_id: u32,
        tarjeta: DatosTarjeta,
    },
    SolicitudDevolucion {
        usuario_id: UsuarioId,
        bici_id: BiciId,
        rental_id: RentalId,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeEstacionAUsuario {
    AlquilerConfirmado {
        rental_id: RentalId,
        bici_id: BiciId,
        preauth_id: String,
    },
    AlquilerRechazado {
        motivo: String,
    },
    DevolucionAceptada {
        bici_id: BiciId,
    },
    DevolucionCompletada {
        rental_id: RentalId,
        monto_cobrado: f64,
        tiempo_uso_minutos: u32,
    },
}

// --- Consultas: discovery del líder y disponibilidad ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeUsuarioAEstacionConsulta {
    PreguntarLider,
    ConsultaDisponibilidad {
        usuario_id: UsuarioId,
        ubicacion: (f64, f64),
        radio_max_km: f64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeEstacionAUsuarioConsulta {
    /// La estación conoce al líder y lo informa (con `term` para descartar
    /// respuestas viejas).
    RespuestaLider {
        lider_id: EstacionId,
        lider_addr: SocketAddr,
        term: u64,
    },
    /// Hay una elección en curso.
    EnEleccion,
    /// La estación es follower y todavía no aprendió quién es el líder.
    LiderDesconocido,
    RespuestaDisponibilidad {
        estaciones: Vec<InfoEstacion>,
    },
}
