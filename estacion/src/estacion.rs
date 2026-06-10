//! Actor coordinador de la estación.
//!
//! Es el **coordinador del 2PC** del alquiler: en la fase Prepare le pregunta al
//! `Slot` (local) y a la `Pasarela` (remota); si los dos votan que sí, commitea
//! ambos y registra el alquiler con la pre-autorización real; si alguno vota no
//! (o la pasarela no responde), aborta ambos. La devolución sigue siendo local.
//!
//! Toda la red pasa por el `Comunicador`: para hablarle a la pasarela, la estación
//! le manda `ConsultarTcp` a su Comunicador (no toca sockets ella misma).

use std::collections::HashMap;
use std::net::SocketAddr;

use actix::prelude::*;
use comun::comunicador::{Comunicador, ConsultarTcp, EnviarTcp, PaqueteRecibido, Responder};
use comun::mensajes::estacion_estacion::MensajeEntreEstacionesTCP;
use comun::mensajes::estacion_pasarela::{
    MensajeEstacionAPasarela, MensajePasarelaAEstacion, VotoResultado,
};
use comun::mensajes::usuario_estacion::{
    MensajeEstacionAUsuario, MensajeUsuario, MensajeUsuarioAEstacion,
};
use comun::{Alquiler, EstacionId, EstadoAlquiler, EventId, RentalId, Timestamp, TransaccionId};

use crate::mensajes::{
    AbortLiberacion, AceptarBici, CommitLiberacion, ConsultarRegistro, PrepareLiberacion,
    ProcesarDevolucion, RegistrarComunicador, SolicitudUsuario, Voto,
};
use crate::registro::Registro;
use crate::slot::Slot;

/// Datos que el destino necesita para cobrar y cerrar una devolución (los obtiene
/// del registro del líder).
struct DatosCobro {
    rental_id: RentalId,
    preauth_id: String,
    t0: Timestamp,
    estacion_origen: EstacionId,
}

/// Rol de la estación: una es el líder (mantiene el registro autoritativo), el
/// resto son followers.
enum RolEstacion {
    Lider(Registro),
    Follower,
}

/// Monto que se pre-autoriza (reserva) al iniciar un alquiler. Provisorio fijo.
const MONTO_RESERVA: f64 = 1000.0;

pub struct Estacion {
    id: EstacionId,
    ubicacion: (f64, f64),
    slots: Vec<Addr<Slot>>,
    pasarela: SocketAddr,
    /// Dirección del líder (a quién reportar los alquileres si soy follower).
    lider: SocketAddr,
    /// Direcciones de todas las estaciones, para alcanzar al origen en la devolución.
    estaciones: HashMap<EstacionId, SocketAddr>,
    rol: RolEstacion,
    /// `Addr` del Comunicador propio (se cablea al arrancar con `RegistrarComunicador`).
    comunicador: Option<Addr<Comunicador>>,
    alquileres_propios: HashMap<RentalId, Alquiler>,
    /// Contador para generar ids únicos de transacción, alquiler y evento.
    contador: u64,
}

impl Estacion {
    pub fn new(
        id: EstacionId,
        ubicacion: (f64, f64),
        slots: Vec<Addr<Slot>>,
        pasarela: SocketAddr,
        lider: SocketAddr,
        es_lider: bool,
        estaciones: HashMap<EstacionId, SocketAddr>,
    ) -> Self {
        Self {
            id,
            ubicacion,
            slots,
            pasarela,
            lider,
            estaciones,
            rol: if es_lider {
                RolEstacion::Lider(Registro::new())
            } else {
                RolEstacion::Follower
            },
            comunicador: None,
            alquileres_propios: HashMap::new(),
            contador: 0,
        }
    }

    fn proximo(&mut self) -> u64 {
        self.contador += 1;
        self.contador
    }

    /// Reporta un alquiler recién abierto al líder. Si esta estación ES el líder,
    /// lo registra directo; si es follower, se lo manda por el Comunicador.
    fn reportar_alquiler(&mut self, alquiler: &Alquiler) {
        let n = self.proximo();
        let abierto = MensajeEntreEstacionesTCP::AlquilerAbierto {
            event_id: EventId(format!("E-{}-{}", self.id.0, n)),
            rental_id: alquiler.rental_id.clone(),
            bici_id: alquiler.bici_id,
            usuario_id: alquiler.usuario_id.clone(),
            estacion_origen: alquiler.estacion_origen,
            t0: alquiler.inicio,
            preauth_id: alquiler.preauth_id.clone(),
        };
        match &mut self.rol {
            RolEstacion::Lider(registro) => registro.agregar(alquiler.clone()),
            RolEstacion::Follower => {
                if let (Some(comunicador), Ok(bytes)) =
                    (&self.comunicador, comun::serializacion::a_bytes(&abierto))
                {
                    comunicador.do_send(EnviarTcp {
                        destino: self.lider,
                        datos: bytes,
                    });
                }
            }
        }
    }

    /// Maneja un mensaje que llegó de otra estación. Son sincrónicos (consultas al
    /// registro local). El líder responde `NotificarDevolucion` con los datos de
    /// cobro y cierra el alquiler con `DevolucionProcesada`.
    fn manejar_entre_estaciones(
        &mut self,
        msg: MensajeEntreEstacionesTCP,
        responder: Option<Responder>,
    ) {
        match msg {
            MensajeEntreEstacionesTCP::AlquilerAbierto {
                rental_id,
                bici_id,
                usuario_id,
                estacion_origen,
                t0,
                preauth_id,
                ..
            } => {
                if let RolEstacion::Lider(registro) = &mut self.rol {
                    registro.agregar(Alquiler {
                        rental_id,
                        bici_id,
                        usuario_id,
                        estacion_origen,
                        inicio: t0,
                        fin: None,
                        preauth_id,
                        estado: EstadoAlquiler::Activo,
                    });
                }
            }
            MensajeEntreEstacionesTCP::NotificarDevolucion {
                event_id, bici_id, ..
            } => {
                let respuesta = match &self.rol {
                    RolEstacion::Lider(registro) => match registro.buscar_por_bici(bici_id) {
                        Some(a) => MensajeEntreEstacionesTCP::DatosParaCobro {
                            event_id,
                            rental_id: a.rental_id.clone(),
                            preauth_id: a.preauth_id.clone(),
                            t0: a.inicio,
                            estacion_origen: a.estacion_origen,
                        },
                        None => MensajeEntreEstacionesTCP::NoRegistradoAun { event_id },
                    },
                    RolEstacion::Follower => {
                        MensajeEntreEstacionesTCP::NoRegistradoAun { event_id }
                    }
                };
                if let (Some(r), Ok(bytes)) = (responder, comun::serializacion::a_bytes(&respuesta))
                {
                    r.responder(bytes);
                }
            }
            MensajeEntreEstacionesTCP::DevolucionProcesada { rental_id, .. } => {
                if let RolEstacion::Lider(registro) = &mut self.rol {
                    registro.cerrar(&rental_id);
                }
            }
            MensajeEntreEstacionesTCP::CierreAlquiler { rental_id, .. } => {
                // Soy la estación de origen: marco mi alquiler como cerrado.
                if let Some(alquiler) = self.alquileres_propios.get_mut(&rental_id) {
                    alquiler.estado = EstadoAlquiler::Cerrado;
                }
            }
            // Ring (Etapa 5) y manejo de huérfanas (Etapa 6) más adelante.
            _ => {}
        }
    }

    /// Datos que necesita `procesar_operacion`, capturados antes del trabajo async.
    fn contexto(&mut self) -> ContextoOperacion {
        let n = self.proximo();
        ContextoOperacion {
            tx_id: TransaccionId(format!("T-{}-{}", self.id.0, n)),
            rental_id: RentalId(format!("R-{}-{}", self.id.0, n)),
            slots: self.slots.clone(),
            estacion_origen: self.id,
            pasarela: self.pasarela,
            comunicador: self.comunicador.clone(),
        }
    }
}

/// Contexto inmutable de una operación (sin `&self`), para el trabajo async.
struct ContextoOperacion {
    tx_id: TransaccionId,
    rental_id: RentalId,
    slots: Vec<Addr<Slot>>,
    estacion_origen: EstacionId,
    pasarela: SocketAddr,
    comunicador: Option<Addr<Comunicador>>,
}

impl Actor for Estacion {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        println!(
            "[{}] actor Estacion iniciado (ubicacion {:?}, {} slots, pasarela {})",
            self.id,
            self.ubicacion,
            self.slots.len(),
            self.pasarela
        );
    }
}

impl Handler<RegistrarComunicador> for Estacion {
    type Result = ();

    fn handle(&mut self, msg: RegistrarComunicador, _ctx: &mut Self::Context) {
        self.comunicador = Some(msg.0);
    }
}

impl Handler<ConsultarRegistro> for Estacion {
    type Result = usize;

    fn handle(&mut self, _msg: ConsultarRegistro, _ctx: &mut Self::Context) -> usize {
        match &self.rol {
            RolEstacion::Lider(registro) => registro.activos(),
            RolEstacion::Follower => 0,
        }
    }
}

impl Handler<ProcesarDevolucion> for Estacion {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, msg: ProcesarDevolucion, _ctx: &mut Self::Context) -> Self::Result {
        // Datos de cobro: si soy el líder los busco en mi registro (en proceso);
        // si soy follower los consulto por red (más abajo).
        let es_lider = matches!(self.rol, RolEstacion::Lider(_));
        let datos_si_lider = if let RolEstacion::Lider(registro) = &self.rol {
            registro.buscar_por_bici(msg.bici_id).map(|a| DatosCobro {
                rental_id: a.rental_id.clone(),
                preauth_id: a.preauth_id.clone(),
                t0: a.inicio,
                estacion_origen: a.estacion_origen,
            })
        } else {
            None
        };
        let comunicador = self.comunicador.clone();
        let lider = self.lider;
        let pasarela = self.pasarela;
        let mi_id = self.id;
        let bici_id = msg.bici_id;
        let t1 = msg.t1;
        let n = self.proximo();
        let event_id = EventId(format!("E-{}-{}", self.id.0, n));

        Box::pin(
            async move {
                let datos = if es_lider {
                    datos_si_lider
                } else {
                    consultar_lider_devolucion(
                        &comunicador,
                        lider,
                        event_id.clone(),
                        bici_id,
                        mi_id,
                        t1,
                    )
                    .await
                };
                let datos = datos?;
                // Cobro a la pasarela (proporcional a t1 - t0).
                let cobro = MensajeEstacionAPasarela::ProcesarCobro {
                    preauth_id: datos.preauth_id.clone(),
                    t0: datos.t0,
                    t1,
                };
                let monto = match consultar_pasarela(&comunicador, pasarela, &cobro).await {
                    Some(MensajePasarelaAEstacion::CobroConfirmado { monto, .. }) => monto,
                    _ => 0.0,
                };
                Some((datos, monto, event_id))
            }
            .into_actor(self)
            .map(move |resultado, actor, _ctx| {
                let Some((datos, monto, event_id)) = resultado else {
                    return; // NoRegistradoAun / huérfana: se maneja en la Etapa 6.
                };
                let tiempo = datos.t0.minutos_hasta(t1);

                // Cerrar en el líder (en proceso si soy líder; por red si no).
                if let RolEstacion::Lider(registro) = &mut actor.rol {
                    registro.cerrar(&datos.rental_id);
                } else if let (Some(comunicador), Ok(bytes)) = (
                    &actor.comunicador,
                    comun::serializacion::a_bytes(
                        &MensajeEntreEstacionesTCP::DevolucionProcesada {
                            event_id,
                            rental_id: datos.rental_id.clone(),
                            monto_cobrado: monto,
                            tiempo_uso_minutos: tiempo,
                        },
                    ),
                ) {
                    comunicador.do_send(EnviarTcp {
                        destino: actor.lider,
                        datos: bytes,
                    });
                }

                // Cerrar en el origen (en proceso si el origen soy yo; por red si no).
                if datos.estacion_origen == actor.id {
                    if let Some(alquiler) = actor.alquileres_propios.get_mut(&datos.rental_id) {
                        alquiler.estado = EstadoAlquiler::Cerrado;
                    }
                } else if let Some(destino) = actor.estaciones.get(&datos.estacion_origen).copied()
                {
                    if let (Some(comunicador), Ok(bytes)) = (
                        &actor.comunicador,
                        comun::serializacion::a_bytes(&MensajeEntreEstacionesTCP::CierreAlquiler {
                            rental_id: datos.rental_id.clone(),
                            t1,
                            monto_cobrado: monto,
                        }),
                    ) {
                        comunicador.do_send(EnviarTcp {
                            destino,
                            datos: bytes,
                        });
                    }
                }
            }),
        )
    }
}

/// Le pide al Comunicador que haga el request-response con la pasarela y devuelve
/// la respuesta deserializada (o `None` si no hay Comunicador o no se pudo).
async fn consultar_pasarela(
    comunicador: &Option<Addr<Comunicador>>,
    pasarela: SocketAddr,
    pedido: &MensajeEstacionAPasarela,
) -> Option<MensajePasarelaAEstacion> {
    let comunicador = comunicador.as_ref()?;
    let bytes = comun::serializacion::a_bytes(pedido).ok()?;
    let respuesta = comunicador
        .send(ConsultarTcp {
            destino: pasarela,
            datos: bytes,
        })
        .await
        .ok()
        .flatten()?;
    comun::serializacion::desde_bytes(&respuesta).ok()
}

/// Consulta al líder (por red) los datos de cobro de una bici que se está
/// devolviendo. Devuelve `None` si el líder responde `NoRegistradoAun` (o falla).
async fn consultar_lider_devolucion(
    comunicador: &Option<Addr<Comunicador>>,
    lider: SocketAddr,
    event_id: EventId,
    bici_id: comun::BiciId,
    estacion_destino: EstacionId,
    t1: Timestamp,
) -> Option<DatosCobro> {
    let comunicador = comunicador.as_ref()?;
    let notif = MensajeEntreEstacionesTCP::NotificarDevolucion {
        event_id,
        bici_id,
        estacion_destino,
        t1,
    };
    let bytes = comun::serializacion::a_bytes(&notif).ok()?;
    let respuesta = comunicador
        .send(ConsultarTcp {
            destino: lider,
            datos: bytes,
        })
        .await
        .ok()
        .flatten()?;
    match comun::serializacion::desde_bytes::<MensajeEntreEstacionesTCP>(&respuesta).ok()? {
        MensajeEntreEstacionesTCP::DatosParaCobro {
            rental_id,
            preauth_id,
            t0,
            estacion_origen,
            ..
        } => Some(DatosCobro {
            rental_id,
            preauth_id,
            t0,
            estacion_origen,
        }),
        _ => None,
    }
}

/// Lógica del alquiler/devolución, sin estado de la estación. Devuelve la
/// respuesta para el usuario y, si corresponde, el alquiler a registrar.
async fn procesar_operacion(
    operacion: MensajeUsuarioAEstacion,
    ctx: ContextoOperacion,
) -> (MensajeEstacionAUsuario, Option<Alquiler>) {
    let ContextoOperacion {
        tx_id,
        rental_id,
        slots,
        estacion_origen,
        pasarela,
        comunicador,
    } = ctx;

    match operacion {
        MensajeUsuarioAEstacion::SolicitudAlquiler {
            usuario_id,
            slot_id,
            tarjeta,
        } => {
            let Some(slot) = slots.get(slot_id as usize).cloned() else {
                return (
                    MensajeEstacionAUsuario::AlquilerRechazado {
                        motivo: format!("no existe el slot {slot_id}"),
                    },
                    None,
                );
            };

            // --- FASE PREPARE ---
            let voto_slot = slot
                .send(PrepareLiberacion {
                    tx_id: tx_id.clone(),
                })
                .await
                .unwrap_or(Voto::No);
            let prepare = MensajeEstacionAPasarela::PreparePreauth {
                tx_id: tx_id.clone(),
                usuario_id: usuario_id.clone(),
                tarjeta,
                monto_propuesto: MONTO_RESERVA,
            };
            let (pasarela_ok, preauth_id) =
                match consultar_pasarela(&comunicador, pasarela, &prepare).await {
                    Some(MensajePasarelaAEstacion::Voto {
                        resultado: VotoResultado::Yes,
                        preauth_id: Some(id),
                        ..
                    }) => (true, Some(id)),
                    _ => (false, None),
                };

            // --- FASE DECISIÓN ---
            if voto_slot == Voto::Si && pasarela_ok {
                let preauth_id = preauth_id.expect("un voto Yes siempre trae preauth_id");
                let bici = slot
                    .send(CommitLiberacion {
                        tx_id: tx_id.clone(),
                    })
                    .await
                    .ok()
                    .flatten();
                let commit = MensajeEstacionAPasarela::CommitPreauth {
                    tx_id,
                    preauth_id: preauth_id.clone(),
                };
                let _ = consultar_pasarela(&comunicador, pasarela, &commit).await;

                match bici {
                    Some(bici_id) => {
                        let alquiler = Alquiler {
                            rental_id: rental_id.clone(),
                            bici_id,
                            usuario_id,
                            estacion_origen,
                            inicio: Timestamp::ahora(),
                            fin: None,
                            preauth_id: preauth_id.clone(),
                            estado: EstadoAlquiler::Activo,
                        };
                        (
                            MensajeEstacionAUsuario::AlquilerConfirmado {
                                rental_id,
                                bici_id,
                                preauth_id,
                            },
                            Some(alquiler),
                        )
                    }
                    None => (
                        MensajeEstacionAUsuario::AlquilerRechazado {
                            motivo: "el slot no liberó la bici".to_string(),
                        },
                        None,
                    ),
                }
            } else {
                // Abort a quien haya votado/preparado que sí.
                if voto_slot == Voto::Si {
                    let _ = slot
                        .send(AbortLiberacion {
                            tx_id: tx_id.clone(),
                        })
                        .await;
                }
                if let Some(id) = preauth_id {
                    let abort = MensajeEstacionAPasarela::AbortPreauth {
                        tx_id,
                        preauth_id: id,
                    };
                    let _ = consultar_pasarela(&comunicador, pasarela, &abort).await;
                }
                let motivo = if voto_slot != Voto::Si {
                    "el slot no tenía una bici disponible".to_string()
                } else {
                    "la pasarela rechazó el pago o no respondió".to_string()
                };
                (MensajeEstacionAUsuario::AlquilerRechazado { motivo }, None)
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
        let ctx = self.contexto();
        Box::pin(
            async move { procesar_operacion(msg.0, ctx).await }
                .into_actor(self)
                .map(|(respuesta, alquiler), actor, ctx| {
                    if let Some(a) = alquiler {
                        actor.reportar_alquiler(&a);
                        actor.alquileres_propios.insert(a.rental_id.clone(), a);
                    }
                    if let MensajeEstacionAUsuario::DevolucionAceptada { bici_id } = &respuesta {
                        ctx.address().do_send(ProcesarDevolucion {
                            bici_id: *bici_id,
                            t1: Timestamp::ahora(),
                        });
                    }
                    respuesta
                }),
        )
    }
}

impl Handler<PaqueteRecibido> for Estacion {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, msg: PaqueteRecibido, _ctx: &mut Self::Context) -> Self::Result {
        // ¿Es un mensaje entre estaciones (reporte/devolución al líder, etc.)?
        if let Ok(entre_estaciones) =
            comun::serializacion::desde_bytes::<MensajeEntreEstacionesTCP>(&msg.datos)
        {
            self.manejar_entre_estaciones(entre_estaciones, msg.responder);
            return Box::pin(async {}.into_actor(self).map(|_, _, _| ()));
        }

        let pedido: Option<MensajeUsuario> = comun::serializacion::desde_bytes(&msg.datos).ok();

        match (pedido, msg.responder) {
            (Some(MensajeUsuario::Operacion(operacion)), Some(responder)) => {
                let ctx = self.contexto();
                Box::pin(
                    async move {
                        let (respuesta, alquiler) = procesar_operacion(operacion, ctx).await;
                        (respuesta, alquiler, responder)
                    }
                    .into_actor(self)
                    .map(|(respuesta, alquiler, responder), actor, ctx| {
                        if let Some(a) = alquiler {
                            actor.reportar_alquiler(&a);
                            actor.alquileres_propios.insert(a.rental_id.clone(), a);
                        }
                        if let MensajeEstacionAUsuario::DevolucionAceptada { bici_id } = &respuesta
                        {
                            ctx.address().do_send(ProcesarDevolucion {
                                bici_id: *bici_id,
                                t1: Timestamp::ahora(),
                            });
                        }
                        if let Ok(bytes) = comun::serializacion::a_bytes(&respuesta) {
                            responder.responder(bytes);
                        }
                    }),
                )
            }
            _ => Box::pin(async {}.into_actor(self).map(|_, _, _| ())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mensajes::ConsultarEstado;
    use comun::comunicador::Transporte;
    use comun::framing::{enmarcar, Desenmarcador};
    use comun::{BiciId, DatosTarjeta, UsuarioId};

    fn paquete_tcp(
        msg: &MensajeEntreEstacionesTCP,
        responder: Option<Responder>,
    ) -> PaqueteRecibido {
        PaqueteRecibido {
            transporte: Transporte::Tcp,
            datos: comun::serializacion::a_bytes(msg).unwrap(),
            responder,
        }
    }
    use std::io::{Read, Write};
    use std::net::TcpListener;

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

    /// Crea la estación (como líder) con su Comunicador cableado (igual que en `main`).
    async fn arrancar(slots: Vec<Addr<Slot>>, pasarela: SocketAddr) -> Addr<Estacion> {
        // Como es líder, registra sus propios alquileres; la dirección de líder no se usa.
        let lider = "127.0.0.1:9".parse().unwrap();
        let estacion = Estacion::new(
            EstacionId(1),
            (0.0, 0.0),
            slots,
            pasarela,
            lider,
            true,
            HashMap::new(),
        )
        .start();
        let comunicador = Comunicador::new(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
            estacion.clone().recipient(),
        )
        .start();
        estacion
            .send(RegistrarComunicador(comunicador))
            .await
            .unwrap();
        estacion
    }

    /// Pasarela de mentira: escucha en un puerto efímero y responde a cada pedido
    /// con un voto fijo (Yes/No) y las confirmaciones de commit/abort.
    fn pasarela_mock(vota_si: bool) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for conexion in listener.incoming() {
                let Ok(mut stream) = conexion else { continue };
                let mut desen = Desenmarcador::new();
                let mut buf = [0u8; 4096];
                loop {
                    let n = match stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    desen.alimentar(&buf[..n]);
                    if let Some(payload) = desen.siguiente_payload() {
                        let pedido: MensajeEstacionAPasarela =
                            comun::serializacion::desde_bytes(&payload).unwrap();
                        let resp = responder_mock(pedido, vota_si);
                        let _ = stream.write_all(&enmarcar(&resp).unwrap());
                        break;
                    }
                }
            }
        });
        addr
    }

    fn responder_mock(pedido: MensajeEstacionAPasarela, vota_si: bool) -> MensajePasarelaAEstacion {
        match pedido {
            MensajeEstacionAPasarela::PreparePreauth { tx_id, .. } => {
                if vota_si {
                    MensajePasarelaAEstacion::Voto {
                        tx_id,
                        resultado: VotoResultado::Yes,
                        preauth_id: Some("P-mock".to_string()),
                    }
                } else {
                    MensajePasarelaAEstacion::Voto {
                        tx_id,
                        resultado: VotoResultado::No {
                            motivo: "tarjeta rechazada".to_string(),
                        },
                        preauth_id: None,
                    }
                }
            }
            MensajeEstacionAPasarela::CommitPreauth { preauth_id, .. } => {
                MensajePasarelaAEstacion::PreauthConfirmada { preauth_id }
            }
            MensajeEstacionAPasarela::AbortPreauth { preauth_id, .. } => {
                MensajePasarelaAEstacion::PreauthAnulada { preauth_id }
            }
            MensajeEstacionAPasarela::ProcesarCobro { preauth_id, .. } => {
                MensajePasarelaAEstacion::CobroConfirmado {
                    preauth_id,
                    monto: 0.0,
                }
            }
        }
    }

    #[test]
    fn alquiler_con_ambos_votos_si_confirma_con_preauth_real() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(true);
            let s0 = Slot::con_bici(0, BiciId(42)).start();
            let estacion = arrancar(vec![s0.clone()], pasarela).await;

            let resp = estacion.send(alquilar(0)).await.unwrap();
            match resp {
                MensajeEstacionAUsuario::AlquilerConfirmado { preauth_id, .. } => {
                    assert_eq!(preauth_id, "P-mock", "la preauth la da la pasarela");
                }
                otro => panic!("esperaba AlquilerConfirmado, fue {otro:?}"),
            }
            assert!(
                !s0.send(ConsultarEstado).await.unwrap().ocupado,
                "el slot quedó vacío"
            );
        });
    }

    #[test]
    fn el_lider_registra_el_alquiler() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(true);
            let s0 = Slot::con_bici(0, BiciId(42)).start();
            let estacion = arrancar(vec![s0], pasarela).await;

            assert_eq!(estacion.send(ConsultarRegistro).await.unwrap(), 0);
            let _ = estacion.send(alquilar(0)).await.unwrap();
            // El alquiler exitoso se reportó al registro (esta estación es el líder).
            assert_eq!(estacion.send(ConsultarRegistro).await.unwrap(), 1);
        });
    }

    #[test]
    fn devolucion_completa_cobra_y_cierra_el_registro() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(true);
            let s0 = Slot::con_bici(0, BiciId(42)).start();
            let estacion = arrancar(vec![s0], pasarela).await; // líder y origen

            // Alquilar: registro queda en 1, slot 0 vacío.
            let _ = estacion.send(alquilar(0)).await.unwrap();
            assert_eq!(estacion.send(ConsultarRegistro).await.unwrap(), 1);

            // Devolver la bici en el slot 0 (ahora vacío) → DevolucionAceptada + background.
            let resp = estacion
                .send(SolicitudUsuario(
                    MensajeUsuarioAEstacion::SolicitudDevolucion {
                        usuario_id: UsuarioId("alice".to_string()),
                        bici_id: BiciId(42),
                        rental_id: RentalId("ignorado".to_string()),
                        slot_id: 0,
                    },
                ))
                .await
                .unwrap();
            assert!(matches!(
                resp,
                MensajeEstacionAUsuario::DevolucionAceptada { .. }
            ));

            // El background (ProcesarDevolucion) corre antes que esta consulta y
            // cierra el alquiler en el registro.
            assert_eq!(estacion.send(ConsultarRegistro).await.unwrap(), 0);
        });
    }

    #[test]
    fn el_lider_responde_datos_para_cobro_y_cierra_con_devolucion_procesada() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(true);
            let s0 = Slot::con_bici(0, BiciId(42)).start();
            let estacion = arrancar(vec![s0], pasarela).await; // es líder
            let _ = estacion.send(alquilar(0)).await.unwrap(); // registra la bici 42
            assert_eq!(estacion.send(ConsultarRegistro).await.unwrap(), 1);

            // La destino notifica la devolución → el líder responde DatosParaCobro.
            let (responder, rx) = Responder::canal();
            let notif = MensajeEntreEstacionesTCP::NotificarDevolucion {
                event_id: EventId("E1".to_string()),
                bici_id: BiciId(42),
                estacion_destino: EstacionId(2),
                t1: Timestamp(60_000),
            };
            estacion
                .send(paquete_tcp(&notif, Some(responder)))
                .await
                .unwrap();
            let reply: MensajeEntreEstacionesTCP =
                comun::serializacion::desde_bytes(&rx.recv().unwrap()).unwrap();
            let rental = match reply {
                MensajeEntreEstacionesTCP::DatosParaCobro {
                    rental_id,
                    preauth_id,
                    ..
                } => {
                    assert_eq!(preauth_id, "P-mock");
                    rental_id
                }
                otro => panic!("esperaba DatosParaCobro, fue {otro:?}"),
            };

            // La destino confirma el cobro → el líder cierra el alquiler.
            let procesada = MensajeEntreEstacionesTCP::DevolucionProcesada {
                event_id: EventId("E1".to_string()),
                rental_id: rental,
                monto_cobrado: 70.0,
                tiempo_uso_minutos: 1,
            };
            estacion.send(paquete_tcp(&procesada, None)).await.unwrap();
            assert_eq!(estacion.send(ConsultarRegistro).await.unwrap(), 0);
        });
    }

    #[test]
    fn notificar_devolucion_de_bici_desconocida_responde_no_registrado() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(true);
            let estacion = arrancar(vec![], pasarela).await; // líder sin alquileres
            let (responder, rx) = Responder::canal();
            let notif = MensajeEntreEstacionesTCP::NotificarDevolucion {
                event_id: EventId("E1".to_string()),
                bici_id: BiciId(999),
                estacion_destino: EstacionId(2),
                t1: Timestamp(0),
            };
            estacion
                .send(paquete_tcp(&notif, Some(responder)))
                .await
                .unwrap();
            let reply: MensajeEntreEstacionesTCP =
                comun::serializacion::desde_bytes(&rx.recv().unwrap()).unwrap();
            assert!(matches!(
                reply,
                MensajeEntreEstacionesTCP::NoRegistradoAun { .. }
            ));
        });
    }

    #[test]
    fn pasarela_vota_no_aborta_y_el_slot_no_pierde_la_bici() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(false);
            let s0 = Slot::con_bici(0, BiciId(42)).start();
            let estacion = arrancar(vec![s0.clone()], pasarela).await;

            let resp = estacion.send(alquilar(0)).await.unwrap();
            assert!(matches!(
                resp,
                MensajeEstacionAUsuario::AlquilerRechazado { .. }
            ));
            // Tras el abort, el slot sigue con su bici (la reserva se liberó).
            let estado = s0.send(ConsultarEstado).await.unwrap();
            assert_eq!(estado.bici_id, Some(BiciId(42)), "la bici sigue en el slot");
        });
    }

    #[test]
    fn devolucion_a_slot_ocupado_se_rechaza() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(true);
            let ocupado = Slot::con_bici(0, BiciId(10)).start();
            let estacion = arrancar(vec![ocupado.clone()], pasarela).await;

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
            assert_eq!(
                ocupado.send(ConsultarEstado).await.unwrap().bici_id,
                Some(BiciId(10))
            );
        });
    }
}
