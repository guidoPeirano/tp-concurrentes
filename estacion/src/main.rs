//! Binario de la estación. Parsea la config, levanta los actores (incluido el
//! Comunicador, que abre los sockets TCP/UDP) y queda a la espera. La lógica de
//! negocio se agrega etapa por etapa.

mod eleccion;
mod estacion;
mod mensajes;
mod registro;
mod slot;

use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;

use actix::prelude::*;
use comun::comunicador::Comunicador;
use comun::{BiciId, Config, EstacionId};

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

    // TCP y UDP comparten el número de puerto (son protocolos distintos).
    let addr_red = SocketAddr::from(([127, 0, 0, 1], puerto));
    // Dirección de la pasarela, para el 2PC de alquiler.
    let pasarela_addr = SocketAddr::from(([127, 0, 0, 1], config.pasarela.puerto));
    // Líder designado por config (sin elección todavía).
    let lider_addr = config
        .direccion_lider()
        .ok_or("el líder de la config no está en la lista de estaciones")?;
    let es_lider = id == config.lider;
    // Direcciones de todas las estaciones (para alcanzar al origen en la devolución).
    let estaciones: std::collections::HashMap<EstacionId, SocketAddr> = config
        .estaciones
        .iter()
        .map(|e| (e.id, SocketAddr::from(([127, 0, 0, 1], e.puerto))))
        .collect();

    let system = System::new();
    let _actores = system.block_on(async move {
        // La estación arranca con bicis en la primera mitad de sus slots. El id de
        // cada bici se deriva del id de la estación para que sea único en el sistema.
        let slots: Vec<Addr<Slot>> = (0..SLOTS_POR_ESTACION)
            .map(|i| {
                if i < SLOTS_POR_ESTACION / 2 {
                    Slot::con_bici(i, BiciId(id.0 * 100 + i)).start()
                } else {
                    Slot::nuevo(i).start()
                }
            })
            .collect();
        // La Estacion toma posesión de los slots (mantiene vivas sus Addr).
        let mut estacion = Estacion::new(
            id,
            mi_config.ubicacion,
            slots,
            pasarela_addr,
            (config.lider, lider_addr),
            es_lider,
            estaciones,
        );
        // Persistencia opcional: con `--estado <ruta>` el estado sobrevive reinicios.
        if let Some(ruta) = comun::args::flag("--estado") {
            estacion = estacion.con_persistencia(PathBuf::from(ruta));
        }
        let estacion = estacion.start();
        let comunicador =
            Comunicador::new(addr_red, addr_red, estacion.clone().recipient()).start();
        // Le damos a la estación la Addr de su Comunicador (para hablar con la pasarela).
        estacion.do_send(crate::mensajes::RegistrarComunicador(comunicador.clone()));
        println!(
            "[estacion {}] escuchando en {} (TCP+UDP); {} slots; a la espera (ctrl-c para salir)",
            id_arg, addr_red, SLOTS_POR_ESTACION
        );
        (comunicador, estacion)
    });

    system.run()?;
    Ok(())
}
