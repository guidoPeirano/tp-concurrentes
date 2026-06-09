//! Mensaje interno de la pasarela (vía `Addr`).

use actix::prelude::*;
use comun::mensajes::estacion_pasarela::{MensajeEstacionAPasarela, MensajePasarelaAEstacion};

/// Un pedido de una estación (prepare/commit/abort/cobro) que el
/// `ProcesadorPagos` resuelve. Envuelve el mensaje de red como mensaje de actor;
/// la respuesta es la que va de vuelta a la estación.
#[derive(Message)]
#[rtype(result = "MensajePasarelaAEstacion")]
pub struct PedidoPasarela(pub MensajeEstacionAPasarela);
