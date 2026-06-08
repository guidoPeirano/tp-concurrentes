//! Binario del cliente de usuario (no usa actores). Etapa 0: parsea la config,
//! arma el cliente y lista las estaciones conocidas. La REPL interactiva se
//! agrega en la Etapa 2.

mod usuario;

use std::path::PathBuf;

use clap::Parser;
use comun::{Config, UsuarioId};
use tracing::info;

use crate::usuario::Usuario;

#[derive(Parser)]
#[command(about = "Cliente de usuario del sistema de alquiler de bicicletas")]
struct Args {
    /// Identificador del usuario (ej: alice).
    #[arg(long)]
    id: String,
    /// Ruta al archivo de topología (estaciones.toml).
    #[arg(long)]
    config: PathBuf,
}

fn main() -> anyhow::Result<()> {
    comun::logging::init();
    let args = Args::parse();
    let config = Config::cargar(&args.config)?;

    let usuario = Usuario::new(UsuarioId(args.id));
    info!(
        usuario = ?usuario.id(),
        estaciones_conocidas = config.estaciones.len(),
        "usuario iniciado; topología cargada"
    );
    for estacion in &config.estaciones {
        info!(estacion = %estacion.id, puerto = estacion.puerto, ubicacion = ?estacion.ubicacion, "estación conocida");
    }

    info!("(REPL interactiva pendiente — Etapa 2)");
    Ok(())
}
