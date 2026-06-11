//! Binario de la pasarela de pagos (mock). Parsea config, levanta el
//! `ProcesadorPagos` detrás de un `Comunicador` (atiende pedidos de las
//! estaciones por TCP) y queda a la espera.

mod mensajes;
mod procesador_pagos;
mod tarifa;

use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;

use actix::prelude::*;
use comun::comunicador::Comunicador;
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

    let addr_red = SocketAddr::from(([127, 0, 0, 1], puerto));

    // Persistencia opcional: con `--estado <ruta>` el estado sobrevive reinicios.
    let estado_arg = comun::args::flag("--estado");

    let system = System::new();
    let _actores = system.block_on(async move {
        let mut procesador = ProcesadorPagos::new(config.tarifa.clone());
        if let Some(ruta) = estado_arg {
            procesador = procesador.con_persistencia(PathBuf::from(ruta));
        }
        let procesador = procesador.start();
        let comunicador =
            Comunicador::new(addr_red, addr_red, procesador.clone().recipient()).start();
        println!("[pasarela] escuchando en {addr_red} (TCP); a la espera (ctrl-c para salir)");
        (comunicador, procesador)
    });

    system.run()?;
    Ok(())
}
