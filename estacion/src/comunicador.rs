//! Actor Comunicador: aísla la lógica de red de la estación.
//!
//! Esqueleto de la Etapa 0: solo arranca. Los sockets TCP/UDP, el framing, las
//! colas de mensajes diferidos y el registro de servicios alcanzables se agregan
//! a partir de la Etapa 1.

use actix::prelude::*;
use comun::EstacionId;
use tracing::info;

pub struct Comunicador {
    estacion_id: EstacionId,
}

impl Comunicador {
    pub fn new(estacion_id: EstacionId) -> Self {
        Self { estacion_id }
    }
}

impl Actor for Comunicador {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(estacion = %self.estacion_id, "actor Comunicador iniciado");
    }
}
