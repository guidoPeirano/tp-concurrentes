//! Framing de mensajes para TCP: cada mensaje viaja precedido por su longitud
//! en 4 bytes big-endian. Esto permite reconstruir los límites de cada mensaje
//! aunque el stream los entregue partidos en varios `read` o varios juntos en
//! un mismo `read`.
//!
//! El payload se serializa con `serde_json`. El framing en sí es lógica pura
//! (sin IO), para poder testearlo aislado.

use std::error::Error;

use serde::{de::DeserializeOwned, Serialize};

/// Tamaño del prefijo de longitud (u32 big-endian).
const PREFIJO: usize = 4;

/// Enmarca un payload ya serializado, anteponiéndole su longitud. Lo usa la capa
/// de red, que trabaja con bytes y es agnóstica del tipo de mensaje.
pub fn enmarcar_payload(payload: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
    let len = u32::try_from(payload.len())?;
    let mut frame = Vec::with_capacity(PREFIJO + payload.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Serializa un mensaje a JSON y lo enmarca con su prefijo de longitud.
pub fn enmarcar<T: Serialize>(mensaje: &T) -> Result<Vec<u8>, Box<dyn Error>> {
    enmarcar_payload(&serde_json::to_vec(mensaje)?)
}

/// Acumula bytes recibidos del stream y extrae mensajes completos a medida que
/// los tiene enteros.
#[derive(Default)]
pub struct Desenmarcador {
    buffer: Vec<u8>,
}

impl Desenmarcador {
    pub fn new() -> Self {
        Self::default()
    }

    /// Agrega al buffer interno los bytes recién recibidos.
    pub fn alimentar(&mut self, datos: &[u8]) {
        self.buffer.extend_from_slice(datos);
    }

    /// Extrae el próximo payload completo (bytes, sin deserializar). Devuelve
    /// `None` si todavía no hay un mensaje entero (faltan bytes del prefijo o del
    /// payload). Es lo que usa la capa de red.
    pub fn siguiente_payload(&mut self) -> Option<Vec<u8>> {
        if self.buffer.len() < PREFIJO {
            return None;
        }
        let mut len_bytes = [0u8; PREFIJO];
        len_bytes.copy_from_slice(&self.buffer[..PREFIJO]);
        let len = u32::from_be_bytes(len_bytes) as usize;

        if self.buffer.len() < PREFIJO + len {
            return None;
        }

        let payload = self.buffer[PREFIJO..PREFIJO + len].to_vec();
        self.buffer.drain(..PREFIJO + len);
        Some(payload)
    }

    /// Igual que `siguiente_payload`, pero además deserializa al tipo esperado.
    /// Cómodo para tests y para quien conoce el tipo del mensaje.
    pub fn siguiente<T: DeserializeOwned>(&mut self) -> Result<Option<T>, Box<dyn Error>> {
        match self.siguiente_payload() {
            Some(payload) => Ok(Some(serde_json::from_slice(&payload)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mensajes::usuario_estacion::MensajeUsuarioAEstacion;
    use crate::{BiciId, RentalId, UsuarioId};

    fn mensaje() -> MensajeUsuarioAEstacion {
        MensajeUsuarioAEstacion::SolicitudDevolucion {
            usuario_id: UsuarioId("alice".to_string()),
            bici_id: BiciId(7),
            rental_id: RentalId("R1".to_string()),
            slot_id: 3,
        }
    }

    #[test]
    fn enmarcar_y_desenmarcar_un_mensaje() {
        let frame = enmarcar(&mensaje()).unwrap();
        let mut d = Desenmarcador::new();
        d.alimentar(&frame);

        let salida: Option<MensajeUsuarioAEstacion> = d.siguiente().unwrap();
        assert_eq!(salida, Some(mensaje()));

        let vacio: Option<MensajeUsuarioAEstacion> = d.siguiente().unwrap();
        assert_eq!(vacio, None);
    }

    #[test]
    fn mensaje_partido_en_varios_reads() {
        let frame = enmarcar(&mensaje()).unwrap();
        let mut d = Desenmarcador::new();

        // Alimentamos de a un byte: no debe entregar nada hasta tener el frame entero.
        for (i, byte) in frame.iter().enumerate() {
            d.alimentar(&[*byte]);
            let parcial: Option<MensajeUsuarioAEstacion> = d.siguiente().unwrap();
            if i + 1 < frame.len() {
                assert_eq!(
                    parcial, None,
                    "no debería haber mensaje con el frame incompleto"
                );
            } else {
                assert_eq!(parcial, Some(mensaje()));
            }
        }
    }

    #[test]
    fn varios_mensajes_en_un_mismo_buffer() {
        let mut buf = enmarcar(&mensaje()).unwrap();
        buf.extend(enmarcar(&mensaje()).unwrap());

        let mut d = Desenmarcador::new();
        d.alimentar(&buf);

        let a: Option<MensajeUsuarioAEstacion> = d.siguiente().unwrap();
        let b: Option<MensajeUsuarioAEstacion> = d.siguiente().unwrap();
        let c: Option<MensajeUsuarioAEstacion> = d.siguiente().unwrap();

        assert_eq!(a, Some(mensaje()));
        assert_eq!(b, Some(mensaje()));
        assert_eq!(c, None);
    }
}
