//! Binario de la estación. Por ahora (Etapa 0) parsea la config, levanta los
//! actores esqueleto y queda a la espera. La lógica real se agrega etapa por etapa.

mod comunicador;
mod estacion;
mod slot;

use std::path::PathBuf;

use actix::prelude::*;
use anyhow::Context as _;
use clap::Parser;
use comun::{Config, EstacionId};
use tracing::info;

use crate::comunicador::Comunicador;
use crate::estacion::Estacion;
use crate::slot::Slot;

/// Cantidad de slots por estación. Provisorio hasta hacerlo configurable.
const SLOTS_POR_ESTACION: u32 = 10;

#[derive(Parser)]
#[command(about = "Nodo estación del sistema de alquiler de bicicletas")]
struct Args {
    /// Id de esta estación (debe figurar en el archivo de config).
    #[arg(long)]
    id: u32,
    /// Puerto TCP en el que escucha esta estación.
    #[arg(long)]
    puerto: u16,
    /// Ruta al archivo de topología (estaciones.toml).
    #[arg(long)]
    config: PathBuf,
}

fn main() -> anyhow::Result<()> {
    comun::logging::init();
    let args = Args::parse();

    let config = Config::cargar(&args.config)?;
    let id = EstacionId(args.id);
    let mi_config = config
        .estacion(id)
        .with_context(|| format!("la estación {} no está en el archivo de config", args.id))?
        .clone();

    info!(
        estacion = args.id,
        puerto = args.puerto,
        ubicacion = ?mi_config.ubicacion,
        pasarela_puerto = config.pasarela.puerto,
        estaciones_en_topologia = config.estaciones.len(),
        "configuración cargada"
    );

    let system = System::new();
    let _actores = system.block_on(async move {
        let comunicador = Comunicador::new(id).start();
        let slots: Vec<Addr<Slot>> = (0..SLOTS_POR_ESTACION).map(|i| Slot::nuevo(i).start()).collect();
        let estacion = Estacion::new(id, mi_config.ubicacion).start();
        info!(slots = slots.len(), "actores iniciados; estación a la espera (ctrl-c para salir)");
        (comunicador, slots, estacion)
    });

    system.run().context("el sistema de actores terminó con error")?;
    Ok(())
}
