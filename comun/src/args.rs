//! Parsing mínimo de argumentos de línea de comandos, sin crates externas.

/// Devuelve el valor que sigue a una bandera `--nombre valor` en los argumentos
/// del proceso, o `None` si la bandera no está presente.
pub fn flag(nombre: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let pos = args.iter().position(|a| a == nombre)?;
    args.get(pos + 1).cloned()
}
