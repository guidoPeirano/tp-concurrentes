//! Mensajes internos entre los actores de la estación (vía `Addr`, en memoria).
//! No viajan por la red, así que no están en la crate `comun`.

use actix::prelude::*;
use comun::mensajes::usuario_estacion::{MensajeEstacionAUsuario, MensajeUsuarioAEstacion};
use comun::{BiciId, TransaccionId};

/// Un pedido de usuario (alquiler/devolución) que la estación procesa localmente:
/// rutea al slot que corresponde y coordina el 2PC. Envuelve el mensaje de red
/// para poder enviarlo como mensaje de actor. La respuesta es la misma que iría
/// de vuelta al usuario.
#[derive(Message)]
#[rtype(result = "MensajeEstacionAUsuario")]
pub struct SolicitudUsuario(pub MensajeUsuarioAEstacion);

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
