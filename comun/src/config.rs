//! Configuración de la topología, leída de un archivo TOML al arrancar cada
//! proceso. La comparten las tres aplicaciones: la estación busca su propia
//! entrada por id, la pasarela toma su puerto y la tarifa, y el usuario usa la
//! lista de estaciones para descubrir al líder.

use std::path::Path;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::EstacionId;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub estaciones: Vec<EstacionConfig>,
    pub pasarela: PasarelaConfig,
    pub tarifa: TarifaConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EstacionConfig {
    pub id: EstacionId,
    pub puerto: u16,
    pub ubicacion: (f64, f64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PasarelaConfig {
    pub puerto: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TarifaConfig {
    pub base: f64,
    pub por_minuto: f64,
}

impl Config {
    /// Lee y parsea el archivo de configuración.
    pub fn cargar(path: &Path) -> anyhow::Result<Config> {
        let contenido = std::fs::read_to_string(path)
            .with_context(|| format!("no pude leer la config en {}", path.display()))?;
        let config = toml::from_str(&contenido)
            .with_context(|| format!("no pude parsear el TOML de {}", path.display()))?;
        Ok(config)
    }

    /// Devuelve la entrada de configuración de una estación por su id.
    pub fn estacion(&self, id: EstacionId) -> Option<&EstacionConfig> {
        self.estaciones.iter().find(|e| e.id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EJEMPLO: &str = r#"
[[estaciones]]
id = 1
puerto = 8001
ubicacion = [-34.6037, -58.3816]

[[estaciones]]
id = 2
puerto = 8002
ubicacion = [-34.6100, -58.3850]

[[estaciones]]
id = 3
puerto = 8003
ubicacion = [-34.6200, -58.3900]

[pasarela]
puerto = 9000

[tarifa]
base = 50.0
por_minuto = 10.0
"#;

    #[test]
    fn parsea_config_de_ejemplo() {
        let config: Config = toml::from_str(EJEMPLO).expect("parsea");
        assert_eq!(config.estaciones.len(), 3);
        assert_eq!(config.estaciones[0].id, EstacionId(1));
        assert_eq!(config.estaciones[0].puerto, 8001);
        assert_eq!(config.estaciones[0].ubicacion, (-34.6037, -58.3816));
        assert_eq!(config.estaciones[2].puerto, 8003);
        assert_eq!(config.pasarela.puerto, 9000);
        assert_eq!(config.tarifa.base, 50.0);
        assert_eq!(config.tarifa.por_minuto, 10.0);
    }

    #[test]
    fn busca_estacion_por_id() {
        let config: Config = toml::from_str(EJEMPLO).unwrap();
        assert_eq!(config.estacion(EstacionId(2)).unwrap().puerto, 8002);
        assert!(config.estacion(EstacionId(99)).is_none());
    }
}
