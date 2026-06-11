//! Estación ↔ Estación. TCP para lo crítico (eventos al líder, Ring de elección),
//! UDP para el estado agregado periódico. Flujos completos en `docs/CASOS_DE_USO.md`.

use serde::{Deserialize, Serialize};

use crate::{Alquiler, BiciId, EstacionId, EventId, RentalId, Timestamp, UsuarioId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeEntreEstacionesTCP {
    // --- Eventos hacia el líder (CU1 alta, CU2 cierre) ---
    /// CU1 — la estación origen reporta un alquiler nuevo al líder (async).
    AlquilerAbierto {
        event_id: EventId,
        rental_id: RentalId,
        bici_id: BiciId,
        usuario_id: UsuarioId,
        estacion_origen: EstacionId,
        t0: Timestamp,
        preauth_id: String,
    },
    /// CU2 — la estación destino avisa al líder que llegó una bici.
    NotificarDevolucion {
        event_id: EventId,
        bici_id: BiciId,
        estacion_destino: EstacionId,
        t1: Timestamp,
    },
    /// CU2 — el líder le pasa a la estación destino los datos para cobrar.
    DatosParaCobro {
        event_id: EventId,
        rental_id: RentalId,
        preauth_id: String,
        t0: Timestamp,
        estacion_origen: EstacionId,
    },
    /// CU2 — el líder todavía no tiene registrado ese alquiler (la destino reintenta).
    NoRegistradoAun {
        event_id: EventId,
    },
    /// CU2 — la estación destino le informa al líder el resultado del cobro.
    DevolucionProcesada {
        event_id: EventId,
        rental_id: RentalId,
        monto_cobrado: f64,
        tiempo_uso_minutos: u32,
    },
    /// CU2 — la estación destino le avisa a la estación origen que el alquiler se cerró.
    CierreAlquiler {
        rental_id: RentalId,
        t1: Timestamp,
        monto_cobrado: f64,
    },
    /// CU4 — ACK de los mensajes del Ring: el que reenvía solo da por
    /// entregado un `Election`/`Coordinator` si el receptor contesta esto
    /// (un nodo colgado acepta la conexión pero no responde).
    EventoProcesadoAck {
        event_id: EventId,
    },

    // --- CU4: reconstrucción del registro tras una elección ---
    SolicitarAlquileresAbiertos {
        term: u64,
    },
    RespuestaAlquileres {
        alquileres: Vec<Alquiler>,
    },
    /// CU4 — una estación que se reincorpora tarde manda sus alquileres.
    IngresoTardio {
        alquileres: Vec<Alquiler>,
    },

    // --- CU2 (recuperación): manejo de bicis huérfanas ---
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

    // --- CU4: Ring de elección de líder ---
    /// CU4 — circula por el anillo acumulando ids; gana el mayor.
    Election {
        ids: Vec<EstacionId>,
        iniciador: EstacionId,
    },
    /// CU4 — anuncia al líder electo por el anillo.
    Coordinator {
        lider: EstacionId,
        term: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeEntreEstacionesUDP {
    /// CU3 — snapshot periódico que alimenta la cache de disponibilidad del líder.
    EstadoEstacion {
        estacion_id: EstacionId,
        ubicacion: (f64, f64),
        bicis_disponibles: u32,
        slots_libres: u32,
        timestamp: Timestamp,
    },
}
