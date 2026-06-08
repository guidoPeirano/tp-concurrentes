//! Actor coordinador de la estación.
//!
//! Esqueleto de la Etapa 0: arranca y avisa por consola. El estado (alquileres,
//! rol de líder, term) y los handlers (2PC, Ring, devolución) se agregan en las
//! etapas siguientes.

use actix::prelude::*;
use comun::EstacionId;

pub struct Estacion {
    id: EstacionId,
    ubicacion: (f64, f64),
}

impl Estacion {
    pub fn new(id: EstacionId, ubicacion: (f64, f64)) -> Self {
        Self { id, ubicacion }
    }
}

impl Actor for Estacion {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        println!(
            "[{}] actor Estacion iniciado (ubicacion {:?})",
            self.id, self.ubicacion
        );
    }
}
