//! Manejo de tiempo serializable entre procesos.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Instante en milisegundos desde el epoch UNIX.
///
/// Usamos esto en lugar de `std::time::Instant` porque `Instant` es opaco y
/// local a cada proceso: no se puede serializar ni comparar entre máquinas.
/// Como el monto a cobrar depende de `t1 - t0` y esos timestamps viajan por la
/// red (estación origen → líder → pasarela), necesitamos un valor absoluto.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub u64);

impl Timestamp {
    /// Timestamp del momento actual.
    pub fn ahora() -> Self {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("el reloj del sistema está antes del epoch UNIX")
            .as_millis() as u64;
        Timestamp(millis)
    }

    /// Minutos transcurridos entre `self` y un instante posterior, redondeados
    /// hacia abajo. Si `posterior` es anterior, devuelve 0.
    pub fn minutos_hasta(self, posterior: Timestamp) -> u32 {
        (posterior.0.saturating_sub(self.0) / 60_000) as u32
    }
}
