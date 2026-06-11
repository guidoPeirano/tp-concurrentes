//! Persistencia simple de estado en JSON.
//!
//! La escritura es **atómica a nivel archivo**: se escribe a un `.tmp` y se
//! renombra encima del destino (el rename es atómico en los sistemas que
//! usamos). Así un crash en mitad de la escritura nunca deja un archivo a
//! medias: o queda la versión anterior o la nueva.

use std::fs;
use std::io;
use std::path::Path;

use serde::{de::DeserializeOwned, Serialize};

/// Guarda `valor` serializado como JSON en `ruta` (atómico: tmp + rename).
pub fn guardar<T: Serialize>(ruta: &Path, valor: &T) -> io::Result<()> {
    let json = crate::serializacion::a_json(valor)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let tmp = ruta.with_extension("tmp");
    fs::write(&tmp, json)?;
    fs::rename(&tmp, ruta)
}

/// Carga el estado desde `ruta`. Devuelve `None` si el archivo no existe o no
/// se puede interpretar (p.ej. primera corrida, o un formato viejo): en ambos
/// casos el proceso arranca desde cero, que es el comportamiento que queremos.
pub fn cargar<T: DeserializeOwned>(ruta: &Path) -> Option<T> {
    let contenido = fs::read_to_string(ruta).ok()?;
    crate::serializacion::desde_json(&contenido).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn ruta_tmp(nombre: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("tp-bicis-persistencia-{nombre}.json"))
    }

    #[test]
    fn round_trip_de_un_estado() {
        let ruta = ruta_tmp("round-trip");
        let _ = fs::remove_file(&ruta);

        let estado: HashMap<String, u32> = [("a".to_string(), 1), ("b".to_string(), 2)]
            .into_iter()
            .collect();
        guardar(&ruta, &estado).expect("guarda");
        let recuperado: HashMap<String, u32> = cargar(&ruta).expect("carga");
        assert_eq!(estado, recuperado);

        let _ = fs::remove_file(&ruta);
    }

    #[test]
    fn cargar_un_archivo_inexistente_devuelve_none() {
        let ruta = ruta_tmp("no-existe");
        let _ = fs::remove_file(&ruta);
        assert_eq!(cargar::<u32>(&ruta), None);
    }

    #[test]
    fn cargar_un_archivo_corrupto_devuelve_none() {
        let ruta = ruta_tmp("corrupto");
        fs::write(&ruta, "esto no es json {{{").unwrap();
        assert_eq!(cargar::<u32>(&ruta), None);
        let _ = fs::remove_file(&ruta);
    }
}
