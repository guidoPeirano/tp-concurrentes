//! Actor Slot: una posición física que puede tener o no una bicicleta.
//!
//! Es la unidad atómica del 2PC de alquiler: reserva tentativamente la bici en
//! la fase Prepare y la libera en Commit (o suelta la reserva en Abort). También
//! asegura bicis que llegan en una devolución.
//!
//! Tolerancia a fallas (Caso B): si después de votar Sí no llega ni Commit ni
//! Abort dentro del plazo, el slot aborta unilateralmente — suelta la reserva y
//! se queda con la bici. Así una caída del coordinador no deja al slot bloqueado.

use std::time::Duration;

use actix::prelude::*;
use comun::{BiciId, TransaccionId};

use crate::mensajes::{
    AbortLiberacion, AceptarBici, CommitLiberacion, ConsultarEstado, EstadoSlot, PrepareLiberacion,
    Voto,
};

/// Cuánto espera el slot la decisión (Commit/Abort) después de votar Sí, antes
/// de abortar por su cuenta (Caso B de la sección 7.1.1 del README).
const TIMEOUT_TRANSACCION: Duration = Duration::from_secs(10);

pub struct Slot {
    id: u32,
    bici: Option<BiciId>,
    reservado_para: Option<TransaccionId>,
    timeout_transaccion: Duration,
}

impl Slot {
    /// Crea un slot vacío.
    pub fn nuevo(id: u32) -> Self {
        Self {
            id,
            bici: None,
            reservado_para: None,
            timeout_transaccion: TIMEOUT_TRANSACCION,
        }
    }

    /// Crea un slot que ya tiene una bici asegurada.
    pub fn con_bici(id: u32, bici: BiciId) -> Self {
        Self {
            bici: Some(bici),
            ..Self::nuevo(id)
        }
    }

    /// Cambia el plazo del Caso B (lo usan los tests para no esperar 10s).
    /// Cuando los timeouts se levanten de la config, esto deja de ser solo de test.
    #[cfg(test)]
    pub fn con_timeout_de_transaccion(mut self, plazo: Duration) -> Self {
        self.timeout_transaccion = plazo;
        self
    }
}

impl Actor for Slot {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        println!("  slot {} iniciado", self.id);
    }
}

impl Handler<PrepareLiberacion> for Slot {
    type Result = MessageResult<PrepareLiberacion>;

    fn handle(&mut self, msg: PrepareLiberacion, ctx: &mut Self::Context) -> Self::Result {
        // Caso D: un Prepare repetido de la MISMA tx (reintento del coordinador)
        // recibe la misma respuesta, sin re-procesar.
        if self.reservado_para.as_ref() == Some(&msg.tx_id) {
            return MessageResult(Voto::Si);
        }
        // Vota Sí solo si tiene una bici y no está ya reservado para otra tx.
        let voto = if self.bici.is_some() && self.reservado_para.is_none() {
            self.reservado_para = Some(msg.tx_id.clone());
            // Caso B: si al vencer el plazo la decisión no llegó y la reserva
            // sigue siendo de ESTA tx, abortamos por nuestra cuenta. Si en el
            // medio hubo Commit (reserva en None) o llegó otra tx, no hace nada.
            let tx_id = msg.tx_id;
            ctx.run_later(self.timeout_transaccion, move |slot, _ctx| {
                if slot.reservado_para.as_ref() == Some(&tx_id) {
                    slot.reservado_para = None;
                    println!(
                        "  slot {}: timeout de la transacción {:?}, aborto unilateral (la bici queda)",
                        slot.id, tx_id
                    );
                }
            });
            Voto::Si
        } else {
            Voto::No
        };
        MessageResult(voto)
    }
}

impl Handler<CommitLiberacion> for Slot {
    type Result = Option<BiciId>;

    fn handle(&mut self, msg: CommitLiberacion, _ctx: &mut Self::Context) -> Option<BiciId> {
        if self.reservado_para.as_ref() == Some(&msg.tx_id) {
            self.reservado_para = None;
            self.bici.take()
        } else {
            None
        }
    }
}

impl Handler<AbortLiberacion> for Slot {
    type Result = ();

    fn handle(&mut self, msg: AbortLiberacion, _ctx: &mut Self::Context) {
        if self.reservado_para.as_ref() == Some(&msg.tx_id) {
            self.reservado_para = None;
        }
    }
}

impl Handler<AceptarBici> for Slot {
    type Result = bool;

    fn handle(&mut self, msg: AceptarBici, _ctx: &mut Self::Context) -> bool {
        if self.bici.is_none() {
            self.bici = Some(msg.bici_id);
            true
        } else {
            false
        }
    }
}

impl Handler<ConsultarEstado> for Slot {
    type Result = MessageResult<ConsultarEstado>;

    fn handle(&mut self, _msg: ConsultarEstado, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(EstadoSlot {
            ocupado: self.bici.is_some(),
            bici_id: self.bici,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(s: &str) -> TransaccionId {
        TransaccionId(s.to_string())
    }

    #[test]
    fn vota_no_si_esta_vacio() {
        System::new().block_on(async {
            let slot = Slot::nuevo(0).start();
            let voto = slot
                .send(PrepareLiberacion { tx_id: tx("T1") })
                .await
                .unwrap();
            assert_eq!(voto, Voto::No);
        });
    }

    #[test]
    fn vota_si_y_reserva_si_tiene_bici() {
        System::new().block_on(async {
            let slot = Slot::con_bici(0, BiciId(5)).start();
            let voto = slot
                .send(PrepareLiberacion { tx_id: tx("T1") })
                .await
                .unwrap();
            assert_eq!(voto, Voto::Si);

            // Ya reservado: una segunda tx debe votar No.
            let voto2 = slot
                .send(PrepareLiberacion { tx_id: tx("T2") })
                .await
                .unwrap();
            assert_eq!(voto2, Voto::No);
        });
    }

    #[test]
    fn commit_libera_la_bici() {
        System::new().block_on(async {
            let slot = Slot::con_bici(0, BiciId(5)).start();
            slot.send(PrepareLiberacion { tx_id: tx("T1") })
                .await
                .unwrap();

            let bici = slot
                .send(CommitLiberacion { tx_id: tx("T1") })
                .await
                .unwrap();
            assert_eq!(bici, Some(BiciId(5)));

            let estado = slot.send(ConsultarEstado).await.unwrap();
            assert_eq!(
                estado,
                EstadoSlot {
                    ocupado: false,
                    bici_id: None
                }
            );
        });
    }

    #[test]
    fn abort_limpia_la_reserva_sin_tocar_la_bici() {
        System::new().block_on(async {
            let slot = Slot::con_bici(0, BiciId(5)).start();
            slot.send(PrepareLiberacion { tx_id: tx("T1") })
                .await
                .unwrap();
            slot.send(AbortLiberacion { tx_id: tx("T1") })
                .await
                .unwrap();

            let estado = slot.send(ConsultarEstado).await.unwrap();
            assert_eq!(
                estado,
                EstadoSlot {
                    ocupado: true,
                    bici_id: Some(BiciId(5))
                }
            );

            // Liberada la reserva, puede volver a votar Sí.
            let voto = slot
                .send(PrepareLiberacion { tx_id: tx("T2") })
                .await
                .unwrap();
            assert_eq!(voto, Voto::Si);
        });
    }

    #[test]
    fn prepare_repetido_de_la_misma_tx_vota_si_de_nuevo() {
        System::new().block_on(async {
            let slot = Slot::con_bici(0, BiciId(5)).start();
            let v1 = slot
                .send(PrepareLiberacion { tx_id: tx("T1") })
                .await
                .unwrap();
            // Reintento del coordinador (mensaje duplicado): misma respuesta.
            let v2 = slot
                .send(PrepareLiberacion { tx_id: tx("T1") })
                .await
                .unwrap();
            assert_eq!((v1, v2), (Voto::Si, Voto::Si));

            // Y la reserva sigue siendo una sola: otra tx vota No.
            let v3 = slot
                .send(PrepareLiberacion { tx_id: tx("T2") })
                .await
                .unwrap();
            assert_eq!(v3, Voto::No);
        });
    }

    #[test]
    fn tras_el_timeout_la_reserva_se_libera_sola_y_la_bici_queda() {
        System::new().block_on(async {
            // Plazo corto para no esperar los 10s reales.
            let slot = Slot::con_bici(0, BiciId(5))
                .con_timeout_de_transaccion(Duration::from_millis(100))
                .start();
            let voto = slot
                .send(PrepareLiberacion { tx_id: tx("T1") })
                .await
                .unwrap();
            assert_eq!(voto, Voto::Si);

            // El coordinador "se cayó": no llega ni Commit ni Abort.
            actix::clock::sleep(Duration::from_millis(200)).await;

            // La bici sigue en el slot y la reserva se liberó: otra tx puede votar Sí.
            let estado = slot.send(ConsultarEstado).await.unwrap();
            assert_eq!(estado.bici_id, Some(BiciId(5)), "la bici no se pierde");
            let voto2 = slot
                .send(PrepareLiberacion { tx_id: tx("T2") })
                .await
                .unwrap();
            assert_eq!(voto2, Voto::Si, "la reserva vieja ya no bloquea");
        });
    }

    #[test]
    fn el_commit_a_tiempo_no_se_ve_afectado_por_el_timeout() {
        System::new().block_on(async {
            let slot = Slot::con_bici(0, BiciId(5))
                .con_timeout_de_transaccion(Duration::from_millis(100))
                .start();
            slot.send(PrepareLiberacion { tx_id: tx("T1") })
                .await
                .unwrap();
            // El Commit llega antes del plazo: la bici se libera normalmente.
            let bici = slot
                .send(CommitLiberacion { tx_id: tx("T1") })
                .await
                .unwrap();
            assert_eq!(bici, Some(BiciId(5)));

            // El timer vence después y no debe romper nada (la tx ya no existe).
            actix::clock::sleep(Duration::from_millis(200)).await;
            let estado = slot.send(ConsultarEstado).await.unwrap();
            assert_eq!(estado.bici_id, None, "el slot quedó vacío por el commit");
        });
    }

    #[test]
    fn aceptar_bici_en_vacio_y_rechazo_en_ocupado() {
        System::new().block_on(async {
            let slot = Slot::nuevo(0).start();

            let aseguro = slot.send(AceptarBici { bici_id: BiciId(9) }).await.unwrap();
            assert!(aseguro);

            let estado = slot.send(ConsultarEstado).await.unwrap();
            assert_eq!(
                estado,
                EstadoSlot {
                    ocupado: true,
                    bici_id: Some(BiciId(9))
                }
            );

            // Ya ocupado: rechaza otra bici.
            let aseguro2 = slot
                .send(AceptarBici {
                    bici_id: BiciId(10),
                })
                .await
                .unwrap();
            assert!(!aseguro2);
        });
    }
}
