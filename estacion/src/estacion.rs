//! Actor coordinador de la estación.
//!
//! Etapa 2: rutea las solicitudes de alquiler al slot indicado y coordina el 2PC
//! **local** (por ahora solo con el `Slot`, sin pasarela ni líder). Lleva el
//! registro de sus alquileres propios. La pasarela entra como segundo
//! participante del 2PC en la Etapa 3.

use std::collections::HashMap;

use actix::prelude::*;
use comun::comunicador::{PaqueteRecibido, Transporte};
use comun::mensajes::usuario_estacion::{MensajeEstacionAUsuario, MensajeUsuarioAEstacion};
use comun::{Alquiler, EstacionId, EstadoAlquiler, RentalId, Timestamp, TransaccionId};

use crate::mensajes::{AceptarBici, CommitLiberacion, PrepareLiberacion, SolicitudUsuario, Voto};
use crate::slot::Slot;

pub struct Estacion {
    id: EstacionId,
    ubicacion: (f64, f64),
    slots: Vec<Addr<Slot>>,
    alquileres_propios: HashMap<RentalId, Alquiler>,
    /// Contador para generar ids únicos de transacción y de alquiler.
    contador: u64,
}

impl Estacion {
    pub fn new(id: EstacionId, ubicacion: (f64, f64), slots: Vec<Addr<Slot>>) -> Self {
        Self {
            id,
            ubicacion,
            slots,
            alquileres_propios: HashMap::new(),
            contador: 0,
        }
    }

    fn proximo(&mut self) -> u64 {
        self.contador += 1;
        self.contador
    }
}

impl Actor for Estacion {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        println!(
            "[{}] actor Estacion iniciado (ubicacion {:?}, {} slots)",
            self.id,
            self.ubicacion,
            self.slots.len()
        );
    }
}

impl Handler<PaqueteRecibido> for Estacion {
    type Result = ();

    fn handle(&mut self, msg: PaqueteRecibido, _ctx: &mut Self::Context) {
        let via = match msg.transporte {
            Transporte::Tcp => "TCP",
            Transporte::Udp => "UDP",
        };
        println!(
            "[{}] paquete recibido por {} ({} bytes)",
            self.id,
            via,
            msg.datos.len()
        );
    }
}

impl Handler<SolicitudUsuario> for Estacion {
    type Result = ResponseActFuture<Self, MensajeEstacionAUsuario>;

    fn handle(&mut self, msg: SolicitudUsuario, _ctx: &mut Self::Context) -> Self::Result {
        match msg.0 {
            MensajeUsuarioAEstacion::SolicitudAlquiler {
                usuario_id,
                slot_id,
                ..
            } => {
                let slot = match self.slots.get(slot_id as usize) {
                    Some(s) => s.clone(),
                    None => {
                        let motivo = format!("no existe el slot {slot_id}");
                        return Box::pin(async {}.into_actor(self).map(move |_, _, _| {
                            MensajeEstacionAUsuario::AlquilerRechazado { motivo }
                        }));
                    }
                };
                let n = self.proximo();
                let tx_id = TransaccionId(format!("T-{}-{}", self.id.0, n));
                let rental_id = RentalId(format!("R-{}-{}", self.id.0, n));
                let estacion_origen = self.id;

                Box::pin(
                    async move {
                        // 2PC local: Prepare y, si vota Sí, Commit. (Un solo
                        // participante por ahora; la pasarela se suma en la Etapa 3.)
                        match slot
                            .send(PrepareLiberacion {
                                tx_id: tx_id.clone(),
                            })
                            .await
                        {
                            Ok(Voto::Si) => {
                                slot.send(CommitLiberacion { tx_id }).await.ok().flatten()
                            }
                            _ => None,
                        }
                    }
                    .into_actor(self)
                    .map(move |bici, actor, _ctx| match bici {
                        Some(bici_id) => {
                            actor.alquileres_propios.insert(
                                rental_id.clone(),
                                Alquiler {
                                    rental_id: rental_id.clone(),
                                    bici_id,
                                    usuario_id,
                                    estacion_origen,
                                    inicio: Timestamp::ahora(),
                                    fin: None,
                                    preauth_id: String::from("local"),
                                    estado: EstadoAlquiler::Activo,
                                },
                            );
                            MensajeEstacionAUsuario::AlquilerConfirmado {
                                rental_id,
                                bici_id,
                                preauth_id: String::from("local"),
                            }
                        }
                        None => MensajeEstacionAUsuario::AlquilerRechazado {
                            motivo: "el slot no tenía una bici disponible".to_string(),
                        },
                    }),
                )
            }

            MensajeUsuarioAEstacion::SolicitudDevolucion { bici_id, .. } => {
                let slots = self.slots.clone();
                Box::pin(
                    async move {
                        // Aceptamos la bici en el primer slot libre.
                        for slot in &slots {
                            if let Ok(true) = slot.send(AceptarBici { bici_id }).await {
                                return true;
                            }
                        }
                        false
                    }
                    .into_actor(self)
                    .map(move |aceptada, _actor, _ctx| {
                        if aceptada {
                            MensajeEstacionAUsuario::DevolucionAceptada { bici_id }
                        } else {
                            MensajeEstacionAUsuario::AlquilerRechazado {
                                motivo: "no hay slot libre para la devolución".to_string(),
                            }
                        }
                    }),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mensajes::ConsultarEstado;
    use comun::{BiciId, DatosTarjeta, UsuarioId};

    fn tarjeta() -> DatosTarjeta {
        DatosTarjeta {
            numero: "4111111111111111".to_string(),
            titular: "Alice".to_string(),
            vencimiento: "12/29".to_string(),
            cvv: "123".to_string(),
        }
    }

    fn alquilar(slot_id: u32) -> SolicitudUsuario {
        SolicitudUsuario(MensajeUsuarioAEstacion::SolicitudAlquiler {
            usuario_id: UsuarioId("alice".to_string()),
            slot_id,
            tarjeta: tarjeta(),
        })
    }

    #[test]
    fn alquiler_se_rutea_al_slot_indicado() {
        System::new().block_on(async {
            let s0 = Slot::con_bici(0, BiciId(10)).start();
            let s1 = Slot::con_bici(1, BiciId(11)).start();
            let estacion =
                Estacion::new(EstacionId(1), (0.0, 0.0), vec![s0.clone(), s1.clone()]).start();

            let resp = estacion.send(alquilar(1)).await.unwrap();
            assert!(matches!(
                resp,
                MensajeEstacionAUsuario::AlquilerConfirmado {
                    bici_id: BiciId(11),
                    ..
                }
            ));

            // Solo el slot 1 quedó vacío; el slot 0 sigue con su bici.
            assert!(s0.send(ConsultarEstado).await.unwrap().ocupado);
            assert!(!s1.send(ConsultarEstado).await.unwrap().ocupado);
        });
    }

    #[test]
    fn alquiler_y_devolucion_local_end_to_end() {
        System::new().block_on(async {
            let con_bici = Slot::con_bici(0, BiciId(42)).start();
            let vacio = Slot::nuevo(1).start();
            let estacion = Estacion::new(
                EstacionId(1),
                (0.0, 0.0),
                vec![con_bici.clone(), vacio.clone()],
            )
            .start();

            // Alquiler: el slot de origen queda vacío.
            let resp = estacion.send(alquilar(0)).await.unwrap();
            let (rental_id, bici_id) = match resp {
                MensajeEstacionAUsuario::AlquilerConfirmado {
                    rental_id, bici_id, ..
                } => (rental_id, bici_id),
                otro => panic!("esperaba AlquilerConfirmado, fue {otro:?}"),
            };
            assert_eq!(bici_id, BiciId(42));
            assert!(!con_bici.send(ConsultarEstado).await.unwrap().ocupado);

            // Devolución: el slot destino queda ocupado.
            let resp2 = estacion
                .send(SolicitudUsuario(
                    MensajeUsuarioAEstacion::SolicitudDevolucion {
                        usuario_id: UsuarioId("alice".to_string()),
                        bici_id,
                        rental_id,
                    },
                ))
                .await
                .unwrap();
            assert!(matches!(
                resp2,
                MensajeEstacionAUsuario::DevolucionAceptada { .. }
            ));
            // La bici vuelve al primer slot libre (el 0, que quedó vacío tras alquilar).
            let estado = con_bici.send(ConsultarEstado).await.unwrap();
            assert_eq!(
                estado.bici_id,
                Some(BiciId(42)),
                "la devolución dejó un slot ocupado"
            );
        });
    }
}
