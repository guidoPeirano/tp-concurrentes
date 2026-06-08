//! Binario de la pasarela de pagos (mock). Etapa 0: parsea config, levanta el
//! actor esqueleto y queda a la espera.

mod procesador_pagos;

use std::error::Error;
use std::path::PathBuf;

use actix::prelude::*;
use comun::Config;

use crate::procesador_pagos::ProcesadorPagos;

fn main() -> Result<(), Box<dyn Error>> {
    let puerto_arg = comun::args::flag("--puerto").ok_or("falta el argumento --puerto")?;
    let config_arg = comun::args::flag("--config").ok_or("falta el argumento --config")?;

    let puerto: u16 = puerto_arg.parse()?;
    let config = Config::cargar(&PathBuf::from(config_arg))?;

    println!(
        "[pasarela] config cargada: puerto={}, tarifa base={}, por_minuto={}",
        puerto, config.tarifa.base, config.tarifa.por_minuto
    );

    let system = System::new();
    let _actor = system.block_on(async move {
        let procesador = ProcesadorPagos::new(config.tarifa.clone()).start();
        println!("[pasarela] a la espera (ctrl-c para salir)");
        procesador
    });

    system.run()?;
    Ok(())
}
