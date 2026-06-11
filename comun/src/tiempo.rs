//! Manejo de tiempo serializable entre procesos, y el helper de timeouts que
//! usa el 2PC.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

/// Corre `futuro` con un límite de tiempo: `Some(resultado)` si terminó antes,
/// `None` si venció el plazo. Lo usa el coordinador del 2PC para tratar a un
/// participante que no contesta como un voto No implícito (Caso A).
///
/// No hay un `timeout` en `actix::clock`, así que el futuro combinado se arma a
/// mano: en cada poll prueba primero el futuro real y después el timer (sin
/// crates extra y sin `unsafe`: ambos van en `Box::pin`).
pub fn con_timeout<F: Future>(limite: Duration, futuro: F) -> ConTimeout<F> {
    ConTimeout {
        futuro: Box::pin(futuro),
        timer: Box::pin(actix::clock::sleep(limite)),
    }
}

pub struct ConTimeout<F> {
    futuro: Pin<Box<F>>,
    timer: Pin<Box<actix::clock::Sleep>>,
}

impl<F: Future> Future for ConTimeout<F> {
    type Output = Option<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `ConTimeout` es Unpin (sus campos ya están pineados en el heap).
        let this = self.get_mut();
        if let Poll::Ready(valor) = this.futuro.as_mut().poll(cx) {
            return Poll::Ready(Some(valor));
        }
        match this.timer.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
