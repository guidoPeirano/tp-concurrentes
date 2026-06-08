//! Actor Slot: una posición física que puede tener o no una bicicleta.
//!
//! Esqueleto de la Etapa 0: solo arranca. El estado (`bici`, `reservado_para`)
//! y los handlers del 2PC se agregan en la Etapa 2.

use actix::prelude::*;
use tracing::debug;

pub struct Slot {
    id: u32,
}

impl Slot {
    pub fn nuevo(id: u32) -> Self {
        Self { id }
    }
}

impl Actor for Slot {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        debug!(slot = self.id, "actor Slot iniciado");
    }
}
