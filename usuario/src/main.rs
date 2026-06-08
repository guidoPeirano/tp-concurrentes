//! Binario del cliente de usuario (no usa actores). Etapa 0: parsea la config,
//! arma el cliente y lista las estaciones conocidas. La REPL interactiva se
//! agrega en la Etapa 2.

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
        "[usuario {:?}] topología cargada: {} estaciones conocidas",
        usuario.id(),
        config.estaciones.len()
    );
    for estacion in &config.estaciones {
        println!(
            "  estación {} -> puerto {}, ubicacion {:?}",
            estacion.id, estacion.puerto, estacion.ubicacion
        );
    }

    println!("(REPL interactiva pendiente — Etapa 2)");
    Ok(())
}
