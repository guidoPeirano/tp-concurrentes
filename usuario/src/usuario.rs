//! Cliente de usuario: su identidad, su tarjeta (mock) y su estado de alquiler.
//!
//! El estado se modela como `enum` para que no se puedan construir situaciones
//! inválidas (p. ej. devolver sin tener una bici).

use comun::mensajes::usuario_estacion::MensajeEstacionAUsuario;
use comun::{BiciId, DatosTarjeta, RentalId, UsuarioId};

pub struct Usuario {
    id: UsuarioId,
    tarjeta: DatosTarjeta,
    estado: EstadoUsuario,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EstadoUsuario {
    SinBici,
    ConBici {
        bici_id: BiciId,
        rental_id: RentalId,
    },
}

impl Usuario {
    pub fn new(id: UsuarioId) -> Self {
        Self {
            id,
            // Tarjeta de prueba; la pasarela la validará de mentira en la Etapa 3b.
            tarjeta: DatosTarjeta {
                numero: "4111111111111111".to_string(),
                titular: "Titular Demo".to_string(),
                vencimiento: "12/29".to_string(),
                cvv: "123".to_string(),
            },
            estado: EstadoUsuario::SinBici,
        }
    }

    pub fn id(&self) -> &UsuarioId {
        &self.id
    }

    pub fn tarjeta(&self) -> &DatosTarjeta {
        &self.tarjeta
    }

    pub fn estado(&self) -> &EstadoUsuario {
        &self.estado
    }

    /// Actualiza el estado según la respuesta de la estación.
    pub fn aplicar(&mut self, respuesta: &MensajeEstacionAUsuario) {
        match respuesta {
            MensajeEstacionAUsuario::AlquilerConfirmado {
                bici_id, rental_id, ..
            } => {
                self.estado = EstadoUsuario::ConBici {
                    bici_id: *bici_id,
                    rental_id: rental_id.clone(),
                };
            }
            MensajeEstacionAUsuario::DevolucionAceptada { .. }
            | MensajeEstacionAUsuario::DevolucionCompletada { .. } => {
                self.estado = EstadoUsuario::SinBici;
            }
            // Rechazos: el estado no cambia.
            MensajeEstacionAUsuario::AlquilerRechazado { .. }
            | MensajeEstacionAUsuario::DevolucionRechazada { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn confirmado() -> MensajeEstacionAUsuario {
        MensajeEstacionAUsuario::AlquilerConfirmado {
            rental_id: RentalId("R1".to_string()),
            bici_id: BiciId(7),
            preauth_id: "local".to_string(),
        }
    }

    #[test]
    fn alquiler_confirmado_pasa_a_con_bici() {
        let mut u = Usuario::new(UsuarioId("alice".to_string()));
        assert_eq!(*u.estado(), EstadoUsuario::SinBici);
        u.aplicar(&confirmado());
        assert_eq!(
            *u.estado(),
            EstadoUsuario::ConBici {
                bici_id: BiciId(7),
                rental_id: RentalId("R1".to_string())
            }
        );
    }

    #[test]
    fn devolucion_pasa_a_sin_bici() {
        let mut u = Usuario::new(UsuarioId("alice".to_string()));
        u.aplicar(&confirmado());
        u.aplicar(&MensajeEstacionAUsuario::DevolucionAceptada { bici_id: BiciId(7) });
        assert_eq!(*u.estado(), EstadoUsuario::SinBici);
    }

    #[test]
    fn alquiler_rechazado_no_cambia_el_estado() {
        let mut u = Usuario::new(UsuarioId("alice".to_string()));
        u.aplicar(&MensajeEstacionAUsuario::AlquilerRechazado {
            motivo: "sin bici".to_string(),
        });
        assert_eq!(*u.estado(), EstadoUsuario::SinBici);
    }
}
