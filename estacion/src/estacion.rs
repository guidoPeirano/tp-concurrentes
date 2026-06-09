//! Actor coordinador de la estación.
//!
//! Rutea las solicitudes de alquiler al slot indicado y coordina el 2PC **local**
//! (por ahora solo con el `Slot`, sin pasarela ni líder). Atiende pedidos tanto
//! en proceso (`SolicitudUsuario`) como por red (`PaqueteRecibido` con framing,
//! respondiendo por la misma conexión TCP). Lleva el registro de sus alquileres
//! propios. La pasarela entra como segundo participante del 2PC en la Etapa 3b.

use std::collections::HashMap;

use actix::prelude::*;
use comun::comunicador::PaqueteRecibido;
use comun::mensajes::usuario_estacion::{
    MensajeEstacionAUsuario, MensajeUsuario, MensajeUsuarioAEstacion,
};
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

    /// Datos que necesita `procesar_operacion`, capturados antes del trabajo async.
    fn contexto_operacion(&mut self) -> (TransaccionId, RentalId, Vec<Addr<Slot>>, EstacionId) {
        let n = self.proximo();
        (
            TransaccionId(format!("T-{}-{}", self.id.0, n)),
            RentalId(format!("R-{}-{}", self.id.0, n)),
            self.slots.clone(),
            self.id,
        )
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

/// Lógica del alquiler/devolución local, sin estado de la estación (se le pasa lo
/// necesario). Devuelve la respuesta para el usuario y, si corresponde, el alquiler
/// a registrar. Compartida entre el camino en proceso y el camino por red.
async fn procesar_operacion(
    operacion: MensajeUsuarioAEstacion,
    slots: Vec<Addr<Slot>>,
    tx_id: TransaccionId,
    rental_id: RentalId,
    estacion_origen: EstacionId,
) -> (MensajeEstacionAUsuario, Option<Alquiler>) {
    match operacion {
        MensajeUsuarioAEstacion::SolicitudAlquiler {
            usuario_id,
            slot_id,
            ..
        } => {
            let Some(slot) = slots.get(slot_id as usize).cloned() else {
                return (
                    MensajeEstacionAUsuario::AlquilerRechazado {
                        motivo: format!("no existe el slot {slot_id}"),
                    },
                    None,
                );
            };
            // 2PC local: Prepare y, si vota Sí, Commit.
            let bici = match slot
                .send(PrepareLiberacion {
                    tx_id: tx_id.clone(),
                })
                .await
            {
                Ok(Voto::Si) => slot.send(CommitLiberacion { tx_id }).await.ok().flatten(),
                _ => None,
            };
            match bici {
                Some(bici_id) => {
                    let alquiler = Alquiler {
                        rental_id: rental_id.clone(),
                        bici_id,
                        usuario_id,
                        estacion_origen,
                        inicio: Timestamp::ahora(),
                        fin: None,
                        preauth_id: String::from("local"),
                        estado: EstadoAlquiler::Activo,
                    };
                    (
                        MensajeEstacionAUsuario::AlquilerConfirmado {
                            rental_id,
                            bici_id,
                            preauth_id: String::from("local"),
                        },
                        Some(alquiler),
                    )
                }
                None => (
                    MensajeEstacionAUsuario::AlquilerRechazado {
                        motivo: "el slot no tenía una bici disponible".to_string(),
                    },
                    None,
                ),
            }
        }

        MensajeUsuarioAEstacion::SolicitudDevolucion {
            bici_id, slot_id, ..
        } => {
            let Some(slot) = slots.get(slot_id as usize).cloned() else {
                return (
                    MensajeEstacionAUsuario::DevolucionRechazada {
                        motivo: format!("no existe el slot {slot_id}"),
                    },
                    None,
                );
            };
            // El slot acepta la bici solo si está vacío.
            match slot.send(AceptarBici { bici_id }).await {
                Ok(true) => (
                    MensajeEstacionAUsuario::DevolucionAceptada { bici_id },
                    None,
                ),
                _ => (
                    MensajeEstacionAUsuario::DevolucionRechazada {
                        motivo: format!("el slot {slot_id} está ocupado"),
                    },
                    None,
                ),
            }
        }
    }
}

impl Handler<SolicitudUsuario> for Estacion {
    type Result = ResponseActFuture<Self, MensajeEstacionAUsuario>;

    fn handle(&mut self, msg: SolicitudUsuario, _ctx: &mut Self::Context) -> Self::Result {
        let (tx_id, rental_id, slots, origen) = self.contexto_operacion();
        Box::pin(
            async move { procesar_operacion(msg.0, slots, tx_id, rental_id, origen).await }
                .into_actor(self)
                .map(|(respuesta, alquiler), actor, _ctx| {
                    if let Some(a) = alquiler {
                        actor.alquileres_propios.insert(a.rental_id.clone(), a);
                    }
                    respuesta
                }),
        )
    }
}

impl Handler<PaqueteRecibido> for Estacion {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, msg: PaqueteRecibido, _ctx: &mut Self::Context) -> Self::Result {
        let pedido: Option<MensajeUsuario> = comun::serializacion::desde_bytes(&msg.datos).ok();

        match (pedido, msg.responder) {
            (Some(MensajeUsuario::Operacion(operacion)), Some(responder)) => {
                let (tx_id, rental_id, slots, origen) = self.contexto_operacion();
                Box::pin(
                    async move {
                        let (respuesta, alquiler) =
                            procesar_operacion(operacion, slots, tx_id, rental_id, origen).await;
                        (respuesta, alquiler, responder)
                    }
                    .into_actor(self)
                    .map(|(respuesta, alquiler, responder), actor, _ctx| {
                        if let Some(a) = alquiler {
                            actor.alquileres_propios.insert(a.rental_id.clone(), a);
                        }
                        if let Ok(bytes) = comun::serializacion::a_bytes(&respuesta) {
                            responder.responder(bytes);
                        }
                    }),
                )
            }
            // Consultas, UDP o payloads no reconocidos: por ahora no hacemos nada.
            _ => Box::pin(async {}.into_actor(self).map(|_, _, _| ())),
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

            // Devolución en el slot 1 (vacío): queda ocupado con la bici.
            let resp2 = estacion
                .send(SolicitudUsuario(
                    MensajeUsuarioAEstacion::SolicitudDevolucion {
                        usuario_id: UsuarioId("alice".to_string()),
                        bici_id,
                        rental_id,
                        slot_id: 1,
                    },
                ))
                .await
                .unwrap();
            assert!(matches!(
                resp2,
                MensajeEstacionAUsuario::DevolucionAceptada { .. }
            ));
            let estado = vacio.send(ConsultarEstado).await.unwrap();
            assert_eq!(
                estado.bici_id,
                Some(BiciId(42)),
                "la devolución dejó el slot 1 ocupado"
            );
        });
    }

    #[test]
    fn devolucion_a_slot_ocupado_se_rechaza() {
        System::new().block_on(async {
            let ocupado = Slot::con_bici(0, BiciId(10)).start();
            let estacion = Estacion::new(EstacionId(1), (0.0, 0.0), vec![ocupado.clone()]).start();

            // Intentar devolver en el slot 0, que ya tiene una bici.
            let resp = estacion
                .send(SolicitudUsuario(
                    MensajeUsuarioAEstacion::SolicitudDevolucion {
                        usuario_id: UsuarioId("alice".to_string()),
                        bici_id: BiciId(99),
                        rental_id: RentalId("R1".to_string()),
                        slot_id: 0,
                    },
                ))
                .await
                .unwrap();
            assert!(matches!(
                resp,
                MensajeEstacionAUsuario::DevolucionRechazada { .. }
            ));
            // El slot sigue con su bici original.
            assert_eq!(
                ocupado.send(ConsultarEstado).await.unwrap().bici_id,
                Some(BiciId(10))
            );
        });
    }
}
