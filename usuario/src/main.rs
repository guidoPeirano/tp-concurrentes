//! Binario del cliente de usuario (no usa actores). Parsea la config, arma el
//! cliente y entra a la REPL para alquilar/devolver contra las estaciones por TCP.

mod repl;
mod usuario;

use std::error::Error;
use std::path::PathBuf;

use comun::{Config, UsuarioId};

use crate::usuario::Usuario;

fn main() -> Result<(), Box<dyn Error>> {
    let id_arg = comun::args::flag("--id").ok_or("falta el argumento --id")?;
    let config_arg = comun::args::flag("--config").ok_or("falta el argumento --config")?;

    let config = Config::cargar(&PathBuf::from(config_arg))?;
    let usuario = Usuario::new(UsuarioId(id_arg));

    println!(
        "[usuario {:?}] {} estaciones conocidas",
        usuario.id(),
        config.estaciones.len()
    );
    for estacion in &config.estaciones {
        println!(
            "  estación {} -> puerto {}, ubicacion {:?}",
            estacion.id, estacion.puerto, estacion.ubicacion
        );
    }

    repl::correr(usuario, config);
    Ok(())
}
