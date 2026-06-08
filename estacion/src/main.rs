//! Binario de la estación. Por ahora (Etapa 0) parsea la config, levanta los
//! actores esqueleto y queda a la espera. La lógica real se agrega etapa por etapa.

mod comunicador;
mod estacion;
mod slot;

use std::error::Error;
use std::path::PathBuf;

use actix::prelude::*;
use comun::{Config, EstacionId};

use crate::comunicador::Comunicador;
use crate::estacion::Estacion;
use crate::slot::Slot;

/// Cantidad de slots por estación. Provisorio hasta hacerlo configurable.
const SLOTS_POR_ESTACION: u32 = 10;

fn main() -> Result<(), Box<dyn Error>> {
    let id_arg = comun::args::flag("--id").ok_or("falta el argumento --id")?;
    let puerto_arg = comun::args::flag("--puerto").ok_or("falta el argumento --puerto")?;
    let config_arg = comun::args::flag("--config").ok_or("falta el argumento --config")?;

    let id = EstacionId(id_arg.parse()?);
    let puerto: u16 = puerto_arg.parse()?;
    let config = Config::cargar(&PathBuf::from(config_arg))?;
    let mi_config = config
        .estacion(id)
        .ok_or_else(|| format!("la estación {} no está en el archivo de config", id_arg))?
        .clone();

    println!(
        "[estacion {}] config cargada: puerto={}, ubicacion={:?}, pasarela_puerto={}, estaciones={}",
        id_arg,
        puerto,
        mi_config.ubicacion,
        config.pasarela.puerto,
        config.estaciones.len()
    );

    let system = System::new();
    let _actores = system.block_on(async move {
        let comunicador = Comunicador::new(id).start();
        let slots: Vec<Addr<Slot>> = (0..SLOTS_POR_ESTACION)
            .map(|i| Slot::nuevo(i).start())
            .collect();
        let estacion = Estacion::new(id, mi_config.ubicacion).start();
        println!(
            "[estacion {}] {} slots creados; estación a la espera (ctrl-c para salir)",
            id_arg,
            slots.len()
        );
        (comunicador, slots, estacion)
    });

    system.run()?;
    Ok(())
}
