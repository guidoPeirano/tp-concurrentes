//! Actor que simula la pasarela bancaria.
//!
//! Esqueleto de la Etapa 0: arranca con la tarifa configurada y loguea. Las
//! pre-autorizaciones, el cálculo del cobro, la idempotencia y la persistencia
//! se agregan en la Etapa 3.

use actix::prelude::*;
use comun::config::TarifaConfig;
use tracing::info;

pub struct ProcesadorPagos {
    tarifa: TarifaConfig,
}

impl ProcesadorPagos {
    pub fn new(tarifa: TarifaConfig) -> Self {
        Self { tarifa }
    }
}

impl Actor for ProcesadorPagos {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(
            base = self.tarifa.base,
            por_minuto = self.tarifa.por_minuto,
            "actor ProcesadorPagos iniciado"
        );
    }
}
