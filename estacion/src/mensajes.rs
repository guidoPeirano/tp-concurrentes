//! Mensajes internos entre los actores de la estación (vía `Addr`, en memoria).
//! No viajan por la red, así que no están en la crate `comun`.

use actix::prelude::*;
use comun::comunicador::Comunicador;
use comun::mensajes::usuario_estacion::{MensajeEstacionAUsuario, MensajeUsuarioAEstacion};
use comun::{BiciId, Timestamp, TransaccionId};

/// Mensaje que la estación destino se manda a sí misma para correr el cierre de
/// la devolución **en background** (consultar al líder, cobrar, cerrar), después
/// de haberle respondido `DevolucionAceptada` al usuario.
#[derive(Message)]
#[rtype(result = "()")]
pub struct ProcesarDevolucion {
    pub bici_id: BiciId,
    pub t1: Timestamp,
    /// `true` si este es el reintento posterior a una recuperación de huérfana:
    /// si vuelve a fallar, la bici se confirma huérfana (no se busca de nuevo).
    pub ya_reprocesada: bool,
}

/// Consulta de diagnóstico: cuántas bicis quedaron confirmadas como huérfanas
/// (llegaron a un slot sin alquiler asociado en ninguna estación).
#[derive(Message)]
#[rtype(result = "usize")]
pub struct ConsultarHuerfanas;

/// Un pedido de usuario (alquiler/devolución) que la estación procesa localmente:
/// rutea al slot que corresponde y coordina el 2PC. Envuelve el mensaje de red
/// para poder enviarlo como mensaje de actor. La respuesta es la misma que iría
/// de vuelta al usuario.
#[derive(Message)]
#[rtype(result = "MensajeEstacionAUsuario")]
pub struct SolicitudUsuario(pub MensajeUsuarioAEstacion);

/// Le da a la `Estacion` la `Addr` de su `Comunicador` (se cablea al arrancar,
/// porque el Comunicador se crea después de la Estacion). Así la estación le pide
/// al Comunicador que hable con la pasarela, sin tocar sockets ella misma.
#[derive(Message)]
#[rtype(result = "()")]
pub struct RegistrarComunicador(pub Addr<Comunicador>);

/// Consulta de diagnóstico: cuántos alquileres activos tiene el registro del líder
/// (devuelve 0 si esta estación no es el líder).
#[derive(Message)]
#[rtype(result = "usize")]
pub struct ConsultarRegistro;

/// Consulta de diagnóstico: cuántas estaciones tiene el líder en su cache de
/// estados (alimentada por el gossip UDP). Devuelve 0 si no es el líder.
#[derive(Message)]
#[rtype(result = "usize")]
pub struct ConsultarCache;

/// Consulta de diagnóstico: a quién reconoce esta estación como líder, en qué
/// term, y si el líder es ella misma.
#[derive(Message)]
#[rtype(result = "InfoLider")]
pub struct ConsultarLider;

/// Consulta de diagnóstico: cuántos eventos al líder esperan en la cola de
/// diferidos (no se pudieron entregar todavía).
#[derive(Message)]
#[rtype(result = "usize")]
pub struct ConsultarPendientes;

/// El 2PC decidió COMMIT: se deja constancia (persistida) ANTES de mandarle el
/// `CommitPreauth` a la pasarela, así un crash en el medio se completa al
/// reiniciar (Caso C). Se responde recién después de persistir.
#[derive(Message)]
#[rtype(result = "()")]
pub struct RegistrarCommitPendiente {
    pub tx_id: TransaccionId,
    pub preauth_id: String,
}

/// La pasarela confirmó el Commit: la constancia se borra.
#[derive(Message)]
#[rtype(result = "()")]
pub struct CommitConfirmado {
    pub tx_id: TransaccionId,
}

/// Consulta de diagnóstico: cuántos commits decididos siguen sin confirmación
/// de la pasarela.
#[derive(Message)]
#[rtype(result = "usize")]
pub struct ConsultarCommitsPendientes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InfoLider {
    /// `None` si hay una elección en curso o todavía no se conoce al líder.
    pub lider_id: Option<comun::EstacionId>,
    pub term: u64,
    pub soy_lider: bool,
}

/// Fase Prepare del 2PC sobre el `Slot`: pide reservar la bici para una
/// transacción. Responde con el voto.
#[derive(Message)]
#[rtype(result = "Voto")]
pub struct PrepareLiberacion {
    pub tx_id: TransaccionId,
}

/// Voto de un participante del 2PC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Voto {
    Si,
    No,
}

/// Fase Commit: libera la bici reservada por la transacción. Responde con la
/// bici liberada (o `None` si la reserva no coincide).
#[derive(Message)]
#[rtype(result = "Option<BiciId>")]
pub struct CommitLiberacion {
    pub tx_id: TransaccionId,
}

/// Aborta la reserva de la transacción (no toca la bici).
#[derive(Message)]
#[rtype(result = "()")]
pub struct AbortLiberacion {
    pub tx_id: TransaccionId,
}

/// Pide al slot asegurar una bici que llega. Responde `true` si la aseguró
/// (estaba vacío), `false` si ya estaba ocupado.
#[derive(Message)]
#[rtype(result = "bool")]
pub struct AceptarBici {
    pub bici_id: BiciId,
}

/// Consulta el estado actual del slot.
#[derive(Message)]
#[rtype(result = "EstadoSlot")]
pub struct ConsultarEstado;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EstadoSlot {
    pub ocupado: bool,
    pub bici_id: Option<BiciId>,
}
