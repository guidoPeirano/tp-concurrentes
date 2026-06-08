//! Mensajes que viajan por la red, agrupados por par de aplicaciones que los
//! intercambia. Los mensajes internos entre actores de un mismo proceso (vía
//! `Addr`) viven en cada crate de aplicación, no acá.

pub mod estacion_estacion;
pub mod estacion_pasarela;
pub mod usuario_estacion;
