//! Tipos compartidos por las cuatro aplicaciones del sistema: identificadores,
//! tipos de dominio, mensajes de red y helpers de serialización.

pub mod dominio;
pub mod ids;
pub mod mensajes;
pub mod serializacion;
pub mod tiempo;

pub use dominio::{Alquiler, EstadoAlquiler, InfoEstacion};
pub use ids::{BiciId, DatosTarjeta, EstacionId, EventId, RentalId, TransaccionId, UsuarioId};
pub use tiempo::Timestamp;
