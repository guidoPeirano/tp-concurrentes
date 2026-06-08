//! Binario de la pasarela de pagos (mock). Etapa 0: parsea config, levanta el
//! actor esqueleto y queda a la espera.

mod procesador_pagos;

use std::path::PathBuf;

use actix::prelude::*;
use anyhow::Context as _;
use clap::Parser;
use comun::Config;
use tracing::info;

use crate::procesador_pagos::ProcesadorPagos;

#[derive(Parser)]
#[command(about = "Pasarela de pagos (mock) del sistema de alquiler de bicicletas")]
struct Args {
    /// Puerto TCP en el que escucha la pasarela.
    #[arg(long)]
    puerto: u16,
    /// Ruta al archivo de configuración (estaciones.toml: usa [pasarela] y [tarifa]).
    #[arg(long)]
    config: PathBuf,
}

fn main() -> anyhow::Result<()> {
    comun::logging::init();
    let args = Args::parse();
    let config = Config::cargar(&args.config)?;

    info!(
        puerto = args.puerto,
        tarifa_base = config.tarifa.base,
        tarifa_por_minuto = config.tarifa.por_minuto,
        "configuración cargada"
    );

    let system = System::new();
    let _actor = system.block_on(async move {
        let procesador = ProcesadorPagos::new(config.tarifa.clone()).start();
        info!("pasarela a la espera (ctrl-c para salir)");
        procesador
    });

    system.run().context("el sistema de actores terminó con error")?;
    Ok(())
}
