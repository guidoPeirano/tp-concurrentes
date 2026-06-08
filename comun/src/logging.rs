//! Inicialización del logging, compartida por los tres binarios.

use tracing_subscriber::EnvFilter;

/// Configura `tracing` con un formato simple. El nivel se toma de la variable de
/// entorno `RUST_LOG` (default `info`). Es idempotente: llamarla más de una vez
/// (por ejemplo desde tests) no falla.
pub fn init() {
    let filtro = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filtro)
        .with_target(false)
        .try_init();
}
