//! Actor coordinador de la estación.
//!
//! Etapa 1: además de arrancar, recibe los paquetes de red que le reenvía el
//! `Comunicador` y los loguea. El procesamiento real de cada mensaje (2PC, Ring,
//! devolución) se agrega en las etapas siguientes.

use actix::prelude::*;
use comun::comunicador::{PaqueteRecibido, Transporte};
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

impl Handler<PaqueteRecibido> for Estacion {
    type Result = ();

    fn handle(&mut self, msg: PaqueteRecibido, _ctx: &mut Self::Context) {
        let via = match msg.transporte {
            Transporte::Tcp => "TCP",
            Transporte::Udp => "UDP",
        };
        println!(
            "[{}] paquete recibido por {} ({} bytes)",
            self.id,
            via,
            msg.datos.len()
        );
    }
}
