//! Estación ↔ Estación. TCP para lo crítico (eventos al líder, Ring de elección),
//! UDP para el estado agregado periódico.

use serde::{Deserialize, Serialize};

use crate::{Alquiler, BiciId, EstacionId, EventId, RentalId, Timestamp, UsuarioId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeEntreEstacionesTCP {
    // --- Eventos hacia el líder ---
    AlquilerAbierto {
        event_id: EventId,
        rental_id: RentalId,
        bici_id: BiciId,
        usuario_id: UsuarioId,
        estacion_origen: EstacionId,
        t0: Timestamp,
        preauth_id: String,
    },
    NotificarDevolucion {
        event_id: EventId,
        bici_id: BiciId,
        estacion_destino: EstacionId,
        t1: Timestamp,
    },
    DevolucionProcesada {
        event_id: EventId,
        rental_id: RentalId,
        monto_cobrado: f64,
        tiempo_uso_minutos: u32,
    },
    CierreAlquiler {
        rental_id: RentalId,
        t1: Timestamp,
        monto_cobrado: f64,
    },
    EventoProcesadoAck {
        event_id: EventId,
    },

    // --- Reconstrucción del registro tras una elección ---
    SolicitarAlquileresAbiertos {
        term: u64,
    },
    RespuestaAlquileres {
        alquileres: Vec<Alquiler>,
    },
    IngresoTardio {
        alquileres: Vec<Alquiler>,
    },

    // --- Manejo de bicis huérfanas ---
    BuscarAlquilerPropio {
        event_id: EventId,
        bici_id: BiciId,
    },
    AlquilerEncontrado {
        event_id: EventId,
        alquiler: Alquiler,
    },
    NoLoTengo {
        event_id: EventId,
        bici_id: BiciId,
    },
    AlquilerNoEncontrado {
        bici_id: BiciId,
    },
    ReprocesarDevolucion {
        bici_id: BiciId,
    },
    BiciHuerfanaConfirmada {
        bici_id: BiciId,
    },

    // --- Ring de elección de líder ---
    Election {
        ids: Vec<EstacionId>,
        iniciador: EstacionId,
    },
    Coordinator {
        lider: EstacionId,
        term: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeEntreEstacionesUDP {
    EstadoEstacion {
        estacion_id: EstacionId,
        ubicacion: (f64, f64),
        bicis_disponibles: u32,
        slots_libres: u32,
        timestamp: Timestamp,
    },
}
