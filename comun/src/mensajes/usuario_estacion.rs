//! Usuario ↔ Estación (TCP). Toda la interacción del usuario pasa por acá: las
//! operaciones (alquiler/devolución) y las consultas (discovery del líder y
//! disponibilidad). Antes había un proceso `cloud` para las consultas; se
//! eliminó (era un punto único de falla) y el usuario habla directo con las
//! estaciones.
//!
//! Los flujos completos están en `docs/CASOS_DE_USO.md`. Las etiquetas `CUn` de
//! cada variante indican en qué caso de uso se usa.

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
    /// CU1 — dispara el 2PC de alquiler.
    SolicitudAlquiler {
        usuario_id: UsuarioId,
        slot_id: u32,
        tarjeta: DatosTarjeta,
    },
    /// CU2 — la estación asegura la bici y avisa al líder.
    SolicitudDevolucion {
        usuario_id: UsuarioId,
        bici_id: BiciId,
        rental_id: RentalId,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeEstacionAUsuario {
    /// CU1 — 2PC exitoso.
    AlquilerConfirmado {
        rental_id: RentalId,
        bici_id: BiciId,
        preauth_id: String,
    },
    /// CU1 — algún participante votó No (slot vacío, tarjeta inválida, ...).
    AlquilerRechazado { motivo: String },
    /// CU2 — bici asegurada en el slot; el usuario ya puede irse.
    DevolucionAceptada { bici_id: BiciId },
    /// CU2 — el líder cerró el alquiler y la pasarela cobró.
    DevolucionCompletada {
        rental_id: RentalId,
        monto_cobrado: f64,
        tiempo_uso_minutos: u32,
    },
}

// --- Consultas: discovery del líder y disponibilidad ---

/// CU3 — discovery del líder y consulta de disponibilidad.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeUsuarioAEstacionConsulta {
    PreguntarLider,
    ConsultaDisponibilidad {
        usuario_id: UsuarioId,
        ubicacion: (f64, f64),
        radio_max_km: f64,
    },
}

/// CU3 — respuestas de discovery y disponibilidad.
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
