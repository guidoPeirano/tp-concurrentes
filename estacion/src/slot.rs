//! Actor Slot: una posición física que puede tener o no una bicicleta.
//!
//! Es la unidad atómica del 2PC de alquiler: reserva tentativamente la bici en
//! la fase Prepare y la libera en Commit (o suelta la reserva en Abort). También
//! asegura bicis que llegan en una devolución.

use actix::prelude::*;
use comun::{BiciId, TransaccionId};

use crate::mensajes::{
    AbortLiberacion, AceptarBici, CommitLiberacion, ConsultarEstado, EstadoSlot, PrepareLiberacion,
    Voto,
};

pub struct Slot {
    id: u32,
    bici: Option<BiciId>,
    reservado_para: Option<TransaccionId>,
}

impl Slot {
    /// Crea un slot vacío.
    pub fn nuevo(id: u32) -> Self {
        Self {
            id,
            bici: None,
            reservado_para: None,
        }
    }

    /// Crea un slot que ya tiene una bici asegurada.
    pub fn con_bici(id: u32, bici: BiciId) -> Self {
        Self {
            id,
            bici: Some(bici),
            reservado_para: None,
        }
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

    fn handle(&mut self, msg: PrepareLiberacion, _ctx: &mut Self::Context) -> Self::Result {
        // Vota Sí solo si tiene una bici y no está ya reservado para otra tx.
        let voto = if self.bici.is_some() && self.reservado_para.is_none() {
            self.reservado_para = Some(msg.tx_id);
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
