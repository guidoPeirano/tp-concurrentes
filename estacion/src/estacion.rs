//! Actor coordinador de la estación.
//!
//! Es el **coordinador del 2PC** del alquiler: en la fase Prepare le pregunta al
//! `Slot` (local) y a la `Pasarela` (remota); si los dos votan que sí, commitea
//! ambos y registra el alquiler con la pre-autorización real; si alguno vota no
//! (o la pasarela no responde), aborta ambos. La devolución sigue siendo local.
//!
//! Toda la red pasa por el `Comunicador`: para hablarle a la pasarela, la estación
//! le manda `ConsultarTcp` a su Comunicador (no toca sockets ella misma).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;

use actix::prelude::*;
use comun::comunicador::{
    Comunicador, ConsultarTcp, EnviarTcp, EnviarTcpConfirmado, EnviarUdp, PaqueteRecibido,
    Responder, Transporte,
};
use comun::mensajes::estacion_estacion::{MensajeEntreEstacionesTCP, MensajeEntreEstacionesUDP};
use comun::mensajes::estacion_pasarela::{
    MensajeEstacionAPasarela, MensajePasarelaAEstacion, VotoResultado,
};
use comun::mensajes::usuario_estacion::{
    MensajeEstacionAUsuario, MensajeEstacionAUsuarioConsulta, MensajeUsuario,
    MensajeUsuarioAEstacion, MensajeUsuarioAEstacionConsulta,
};
use comun::{
    Alquiler, EstacionId, EstadoAlquiler, EventId, InfoEstacion, RentalId, Timestamp, TransaccionId,
};
use serde::{Deserialize, Serialize};

/// Cada cuánto cada estación le manda su estado al líder por UDP.
const INTERVALO_GOSSIP: std::time::Duration = std::time::Duration::from_secs(3);

/// Cada cuánto un follower sondea al líder para verificar que siga vivo.
const INTERVALO_VIGILANCIA: std::time::Duration = std::time::Duration::from_secs(2);

/// Sondeos fallidos consecutivos para dar al líder por caído e iniciar elección.
const UMBRAL_FALLOS_LIDER: u8 = 2;

/// Intervalos de vigilancia con una elección sin resolver antes de reiniciarla
/// (cubre el caso de un mensaje del Ring perdido por una caída en cadena).
const UMBRAL_ELECCION_TRABADA: u8 = 5;

/// Cuánto espera el coordinador del 2PC el voto de la pasarela antes de tomarlo
/// como No implícito (Caso A de la sección 7.1.1 del README).
const TIMEOUT_PREPARE: std::time::Duration = std::time::Duration::from_secs(3);

/// Intentos de `NotificarDevolucion` al líder antes de dar a la bici por
/// huérfana (el líder pudo responder `NoRegistradoAun` si el reporte del origen
/// todavía no le llegó).
const REINTENTOS_NOTIFICACION: u32 = 4;

/// Espera base entre reintentos de `NotificarDevolucion` (crece linealmente).
const ESPERA_REINTENTO: std::time::Duration = std::time::Duration::from_millis(500);

/// Cada cuánto se reintentan los commits decididos que la pasarela todavía no
/// confirmó (Caso C: el Commit se perdió o la pasarela estaba caída).
const INTERVALO_REINTENTO_COMMITS: std::time::Duration = std::time::Duration::from_secs(5);

use crate::eleccion::{AccionRing, Eleccion, EstadoLider};
use crate::mensajes::{
    AbortLiberacion, AceptarBici, CommitConfirmado, CommitLiberacion, ConsultarCache,
    ConsultarCommitsPendientes, ConsultarEstado, ConsultarLider, ConsultarPendientes,
    ConsultarRegistro, InfoLider, PrepareLiberacion, ProcesarDevolucion, RegistrarCommitPendiente,
    RegistrarComunicador, SolicitudUsuario, Voto,
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

/// Rol de la estación: una es el líder (mantiene el registro autoritativo y la
/// cache de estados para las consultas), el resto son followers.
enum RolEstacion {
    Lider {
        registro: Registro,
        /// Estado agregado de cada estación, alimentado por el gossip UDP
        /// (`EstadoEstacion`). Sirve para responder consultas de disponibilidad.
        cache: HashMap<EstacionId, InfoEstacion>,
        /// Eventos ya procesados (Caso D): un reintento con el mismo `event_id`
        /// no se vuelve a aplicar. Importa con la cola de diferidos: un evento
        /// puede llegar dos veces si el ACK de TCP se perdió y se reenvió.
        eventos: HashSet<EventId>,
    },
    Follower,
}

impl RolEstacion {
    /// Rol líder recién asumido: registro, cache y dedup arrancan vacíos (el
    /// registro se puebla con la reconstrucción).
    fn lider_nuevo() -> Self {
        RolEstacion::Lider {
            registro: Registro::new(),
            cache: HashMap::new(),
            eventos: HashSet::new(),
        }
    }
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
    /// Id del líder, para responder el discovery (`PreguntarLider`).
    lider_id: EstacionId,
    /// Direcciones de todas las estaciones, para alcanzar al origen en la devolución.
    estaciones: HashMap<EstacionId, SocketAddr>,
    rol: RolEstacion,
    /// Estado del algoritmo de elección de líder (Ring): anillo, `term` y a quién
    /// reconoce como líder.
    eleccion: Eleccion,
    /// `Addr` del Comunicador propio (se cablea al arrancar con `RegistrarComunicador`).
    comunicador: Option<Addr<Comunicador>>,
    alquileres_propios: HashMap<RentalId, Alquiler>,
    /// Contador para generar ids únicos de transacción, alquiler y evento.
    contador: u64,
    /// Sondeos al líder fallidos consecutivos (la vigilancia los cuenta).
    fallos_lider: u8,
    /// Intervalos de vigilancia transcurridos con la elección sin resolver.
    intervalos_en_eleccion: u8,
    /// Cola de diferidos: eventos al líder que no se pudieron entregar. Se
    /// reintentan cuando el líder vuelve a responder (o tras una elección).
    eventos_pendientes: Vec<MensajeEntreEstacionesTCP>,
    /// Commits del 2PC decididos pero todavía sin confirmación de la pasarela
    /// (Caso C). Se reintentan periódicamente y sobreviven reinicios.
    commits_pendientes: HashMap<TransaccionId, String>,
    /// Archivo de persistencia (si está, el estado sobrevive un reinicio).
    archivo: Option<PathBuf>,
    /// Cada cuánto reintentar los commits pendientes (acortable en tests).
    intervalo_reintento_commits: std::time::Duration,
}

/// Lo que la estación guarda en disco: sus alquileres, los commits decididos
/// sin confirmar y la cola de eventos diferidos al líder. El rol y la elección
/// NO se persisten: al reiniciar se rearma por config + discovery/elección.
#[derive(Serialize, Deserialize)]
struct EstadoEnDisco {
    alquileres_propios: HashMap<RentalId, Alquiler>,
    commits_pendientes: Vec<(TransaccionId, String)>,
    eventos_pendientes: Vec<MensajeEntreEstacionesTCP>,
}

impl Estacion {
    pub fn new(
        id: EstacionId,
        ubicacion: (f64, f64),
        slots: Vec<Addr<Slot>>,
        pasarela: SocketAddr,
        lider: (EstacionId, SocketAddr),
        es_lider: bool,
        estaciones: HashMap<EstacionId, SocketAddr>,
    ) -> Self {
        let (lider_id, lider) = lider;
        let eleccion = Eleccion::new(id, estaciones.keys().copied()).con_lider_inicial(lider_id);
        Self {
            id,
            ubicacion,
            slots,
            pasarela,
            lider,
            lider_id,
            estaciones,
            rol: if es_lider {
                RolEstacion::lider_nuevo()
            } else {
                RolEstacion::Follower
            },
            eleccion,
            comunicador: None,
            alquileres_propios: HashMap::new(),
            contador: 0,
            fallos_lider: 0,
            intervalos_en_eleccion: 0,
            eventos_pendientes: Vec::new(),
            commits_pendientes: HashMap::new(),
            archivo: None,
            intervalo_reintento_commits: INTERVALO_REINTENTO_COMMITS,
        }
    }

    /// Activa la persistencia en `ruta`: si ya hay un estado guardado lo carga
    /// (recuperación tras reinicio) y, si esta estación arranca como líder por
    /// config, repuebla su registro con sus propios alquileres activos.
    pub fn con_persistencia(mut self, ruta: PathBuf) -> Self {
        if let Some(estado) = comun::persistencia::cargar::<EstadoEnDisco>(&ruta) {
            println!(
                "[{}] estado recuperado de {:?}: {} alquileres, {} commits pendientes, {} eventos diferidos",
                self.id,
                ruta,
                estado.alquileres_propios.len(),
                estado.commits_pendientes.len(),
                estado.eventos_pendientes.len()
            );
            self.alquileres_propios = estado.alquileres_propios;
            self.commits_pendientes = estado.commits_pendientes.into_iter().collect();
            self.eventos_pendientes = estado.eventos_pendientes;
            if let RolEstacion::Lider { registro, .. } = &mut self.rol {
                for alquiler in self
                    .alquileres_propios
                    .values()
                    .filter(|a| a.estado == EstadoAlquiler::Activo)
                {
                    registro.agregar(alquiler.clone());
                }
            }
        }
        self.archivo = Some(ruta);
        self
    }

    /// Acorta el intervalo de reintento de commits (para tests).
    #[cfg(test)]
    pub fn con_intervalo_de_reintento(mut self, intervalo: std::time::Duration) -> Self {
        self.intervalo_reintento_commits = intervalo;
        self
    }

    /// Vuelca el estado a disco (si la persistencia está activa).
    fn persistir(&self) {
        let Some(ruta) = &self.archivo else { return };
        let estado = EstadoEnDisco {
            alquileres_propios: self.alquileres_propios.clone(),
            commits_pendientes: self
                .commits_pendientes
                .iter()
                .map(|(t, p)| (t.clone(), p.clone()))
                .collect(),
            eventos_pendientes: self.eventos_pendientes.clone(),
        };
        if let Err(e) = comun::persistencia::guardar(ruta, &estado) {
            eprintln!("[{}] no pude persistir el estado en {ruta:?}: {e}", self.id);
        }
    }

    fn proximo(&mut self) -> u64 {
        self.contador += 1;
        self.contador
    }

    /// Arma el snapshot de estado (contando bicis disponibles en los slots) y se
    /// lo manda al líder por UDP. El conteo es asincrónico (consulta a cada slot),
    /// así que se resuelve con un futuro spawneado en el contexto del actor.
    fn enviar_estado(&mut self, ctx: &mut Context<Self>) {
        let slots = self.slots.clone();
        let id = self.id;
        let ubicacion = self.ubicacion;
        let lider = self.lider;
        let total = self.slots.len() as u32;
        let fut = async move {
            let mut disponibles = 0u32;
            for slot in &slots {
                if let Ok(estado) = slot.send(ConsultarEstado).await {
                    if estado.ocupado {
                        disponibles += 1;
                    }
                }
            }
            disponibles
        }
        .into_actor(self)
        .map(move |disponibles, act, _ctx| {
            let estado = MensajeEntreEstacionesUDP::EstadoEstacion {
                estacion_id: id,
                ubicacion,
                bicis_disponibles: disponibles,
                slots_libres: total - disponibles,
                timestamp: Timestamp::ahora(),
            };
            if let (Some(comunicador), Ok(bytes)) =
                (&act.comunicador, comun::serializacion::a_bytes(&estado))
            {
                comunicador.do_send(EnviarUdp {
                    destino: lider,
                    datos: bytes,
                });
            }
        });
        ctx.spawn(fut);
    }

    /// Reporta un alquiler recién abierto al líder. Si esta estación ES el líder,
    /// lo registra directo; si es follower, se lo manda por el Comunicador (con
    /// cola de diferidos si el líder no está disponible).
    fn reportar_alquiler(&mut self, alquiler: &Alquiler, ctx: &mut Context<Self>) {
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
            RolEstacion::Lider { registro, .. } => registro.agregar(alquiler.clone()),
            RolEstacion::Follower => self.enviar_evento_al_lider(abierto, ctx),
        }
    }

    /// Manda un evento al líder con confirmación de entrega: si no se pudo (el
    /// líder está caído o en elección), el evento queda en la cola de diferidos
    /// y se reintenta cuando el líder vuelva a estar disponible. El `event_id`
    /// del evento hace que un doble envío sea inofensivo (el líder deduplica).
    fn enviar_evento_al_lider(
        &mut self,
        evento: MensajeEntreEstacionesTCP,
        ctx: &mut Context<Self>,
    ) {
        let (Some(comunicador), Ok(bytes)) = (
            self.comunicador.clone(),
            comun::serializacion::a_bytes(&evento),
        ) else {
            self.eventos_pendientes.push(evento);
            self.persistir();
            return;
        };
        let destino = self.lider;
        let fut = async move {
            comunicador
                .send(EnviarTcpConfirmado {
                    destino,
                    datos: bytes,
                })
                .await
                .unwrap_or(false)
        }
        .into_actor(self)
        .map(move |entregado, actor, _ctx| {
            if !entregado {
                println!(
                    "[{}] el líder no recibe eventos: lo dejo en la cola de diferidos",
                    actor.id
                );
                actor.eventos_pendientes.push(evento);
                actor.persistir();
            }
        });
        ctx.spawn(fut);
    }

    /// Reintenta los eventos diferidos. Si mientras tanto esta estación pasó a
    /// ser el líder, se los aplica a sí misma (directo al registro).
    fn descargar_pendientes(&mut self, ctx: &mut Context<Self>) {
        if self.eventos_pendientes.is_empty() {
            return;
        }
        let pendientes = std::mem::take(&mut self.eventos_pendientes);
        self.persistir();
        println!(
            "[{}] reintento {} eventos diferidos al líder",
            self.id,
            pendientes.len()
        );
        for evento in pendientes {
            if matches!(self.rol, RolEstacion::Lider { .. }) {
                self.manejar_entre_estaciones(evento, None, ctx);
            } else {
                self.enviar_evento_al_lider(evento, ctx);
            }
        }
    }

    /// Responde una consulta del usuario (CU3): discovery del líder o
    /// disponibilidad. La disponibilidad solo la puede contestar el líder (es el
    /// único con la cache); un follower devuelve la lista vacía.
    fn manejar_consulta(
        &self,
        consulta: MensajeUsuarioAEstacionConsulta,
    ) -> MensajeEstacionAUsuarioConsulta {
        match consulta {
            MensajeUsuarioAEstacionConsulta::PreguntarLider => {
                match self.eleccion.lider_conocido() {
                    EstadoLider::Conocido(_) => MensajeEstacionAUsuarioConsulta::RespuestaLider {
                        lider_id: self.lider_id,
                        lider_addr: self.lider,
                        term: self.eleccion.term(),
                    },
                    EstadoLider::EnEleccion => MensajeEstacionAUsuarioConsulta::EnEleccion,
                    EstadoLider::Desconocido => MensajeEstacionAUsuarioConsulta::LiderDesconocido,
                }
            }
            MensajeUsuarioAEstacionConsulta::ConsultaDisponibilidad {
                ubicacion,
                radio_max_km,
                ..
            } => {
                let estaciones = match &self.rol {
                    RolEstacion::Lider { cache, .. } => cache
                        .values()
                        .filter(|info| info.bicis_disponibles > 0)
                        .filter(|info| distancia_km(ubicacion, info.ubicacion) <= radio_max_km)
                        .cloned()
                        .collect(),
                    RolEstacion::Follower => Vec::new(),
                };
                MensajeEstacionAUsuarioConsulta::RespuestaDisponibilidad { estaciones }
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
        ctx: &mut Context<Self>,
    ) {
        match msg {
            MensajeEntreEstacionesTCP::AlquilerAbierto {
                event_id,
                rental_id,
                bici_id,
                usuario_id,
                estacion_origen,
                t0,
                preauth_id,
            } => {
                if let RolEstacion::Lider {
                    registro, eventos, ..
                } = &mut self.rol
                {
                    // Caso D: un evento repetido (reintento) no se vuelve a aplicar.
                    if eventos.insert(event_id) {
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
            }
            MensajeEntreEstacionesTCP::NotificarDevolucion {
                event_id, bici_id, ..
            } => {
                let respuesta = match &self.rol {
                    RolEstacion::Lider { registro, .. } => {
                        match registro.buscar_por_bici(bici_id) {
                            Some(a) => MensajeEntreEstacionesTCP::DatosParaCobro {
                                event_id,
                                rental_id: a.rental_id.clone(),
                                preauth_id: a.preauth_id.clone(),
                                t0: a.inicio,
                                estacion_origen: a.estacion_origen,
                            },
                            None => MensajeEntreEstacionesTCP::NoRegistradoAun { event_id },
                        }
                    }
                    RolEstacion::Follower => {
                        MensajeEntreEstacionesTCP::NoRegistradoAun { event_id }
                    }
                };
                if let (Some(r), Ok(bytes)) = (responder, comun::serializacion::a_bytes(&respuesta))
                {
                    r.responder(bytes);
                }
            }
            MensajeEntreEstacionesTCP::DevolucionProcesada {
                event_id,
                rental_id,
                ..
            } => {
                if let RolEstacion::Lider {
                    registro, eventos, ..
                } = &mut self.rol
                {
                    // Caso D: dedup por event_id, igual que AlquilerAbierto.
                    if eventos.insert(event_id) {
                        registro.cerrar(&rental_id);
                    }
                }
            }
            MensajeEntreEstacionesTCP::CierreAlquiler { rental_id, .. } => {
                // Soy la estación de origen: marco mi alquiler como cerrado.
                if let Some(alquiler) = self.alquileres_propios.get_mut(&rental_id) {
                    alquiler.estado = EstadoAlquiler::Cerrado;
                    self.persistir();
                }
            }
            // --- CU4: reconstrucción del registro tras una elección ---
            MensajeEntreEstacionesTCP::SolicitarAlquileresAbiertos { .. } => {
                // El nuevo líder está reconstruyendo su registro: le mando mis
                // alquileres propios que siguen activos.
                let alquileres: Vec<Alquiler> = self
                    .alquileres_propios
                    .values()
                    .filter(|a| a.estado == EstadoAlquiler::Activo)
                    .cloned()
                    .collect();
                let respuesta = MensajeEntreEstacionesTCP::RespuestaAlquileres { alquileres };
                if let (Some(r), Ok(bytes)) = (responder, comun::serializacion::a_bytes(&respuesta))
                {
                    r.responder(bytes);
                }
            }

            // --- Ring de elección (CU4) ---
            MensajeEntreEstacionesTCP::Election { ids, iniciador } => {
                let accion = self.eleccion.recibir_election(ids, iniciador);
                self.procesar_accion_ring(accion, ctx);
            }
            MensajeEntreEstacionesTCP::Coordinator { lider, term } => {
                let accion = self.eleccion.recibir_coordinator(lider, term);
                self.procesar_accion_ring(accion, ctx);
            }
            // Manejo de bicis huérfanas: Etapa 6.
            _ => {}
        }
    }

    /// Aplica el resultado de un paso del Ring: primero el cambio de rol/líder que
    /// dictaminó la elección, después el reenvío del mensaje por el anillo.
    fn procesar_accion_ring(&mut self, accion: AccionRing, ctx: &mut Context<Self>) {
        self.aplicar_liderazgo(ctx);
        self.ejecutar_accion_ring(accion, ctx);
    }

    /// Reenvía un mensaje del Ring probando los `destinos` en orden de anillo
    /// hasta que uno acepte la conexión (así se saltean los nodos caídos). Si
    /// ninguno es alcanzable, el anillo se reduce a esta estación: el mensaje
    /// "da la vuelta" entregándose a sí misma, lo que cierra la elección si era
    /// propia (y se descarta si no lo era).
    fn ejecutar_accion_ring(&mut self, accion: AccionRing, ctx: &mut Context<Self>) {
        let (destinos, mensaje) = match accion {
            AccionRing::Ignorar => return,
            AccionRing::Reenviar { destinos, mensaje }
            | AccionRing::AsumirYReenviar { destinos, mensaje } => (destinos, mensaje),
        };
        if destinos.is_empty() {
            return;
        }
        let (Some(comunicador), Ok(bytes)) = (
            self.comunicador.clone(),
            comun::serializacion::a_bytes(&mensaje),
        ) else {
            return;
        };
        let direcciones: Vec<(EstacionId, SocketAddr)> = destinos
            .iter()
            .filter_map(|id| self.estaciones.get(id).map(|addr| (*id, *addr)))
            .collect();
        let yo = ctx.address();
        let mi_id = self.id;
        let fut = async move {
            for (id, addr) in direcciones {
                let entregado = comunicador
                    .send(EnviarTcpConfirmado {
                        destino: addr,
                        datos: bytes.clone(),
                    })
                    .await
                    .unwrap_or(false);
                if entregado {
                    return;
                }
                println!("[{mi_id}] {id} no responde, salteo al siguiente del anillo");
            }
            // Nadie alcanzable: me lo entrego a mí misma (vuelta completa).
            yo.do_send(PaqueteRecibido {
                transporte: Transporte::Tcp,
                datos: bytes,
                responder: None,
            });
        };
        ctx.spawn(fut.into_actor(self).map(|_, _, _| ()));
    }

    /// Sincroniza el rol y el puntero al líder con lo que dictaminó el Ring. Si
    /// esta estación pasa a ser líder, reconstruye el registro pidiéndole a cada
    /// estación sus alquileres abiertos; si deja de serlo, vuelve a follower.
    fn aplicar_liderazgo(&mut self, ctx: &mut Context<Self>) {
        let EstadoLider::Conocido(lider_id) = self.eleccion.lider_conocido() else {
            return;
        };
        self.fallos_lider = 0;
        self.intervalos_en_eleccion = 0;
        self.lider_id = lider_id;
        if let Some(addr) = self.estaciones.get(&lider_id).copied() {
            self.lider = addr;
        }
        let soy_lider = lider_id == self.id;
        let era_lider = matches!(self.rol, RolEstacion::Lider { .. });
        if soy_lider && !era_lider {
            println!(
                "[{}] asumo como líder (term {})",
                self.id,
                self.eleccion.term()
            );
            self.rol = RolEstacion::lider_nuevo();
            self.reconstruir_registro(ctx);
        } else if !soy_lider && era_lider {
            println!("[{}] dejo de ser líder: ahora lidera {}", self.id, lider_id);
            self.rol = RolEstacion::Follower;
        }
        // Con líder conocido (sea quien sea), los eventos diferidos se reintentan.
        self.descargar_pendientes(ctx);
    }

    /// Reconstruye el registro del líder recién electo: arranca con los alquileres
    /// propios y le pide los suyos a cada una de las otras estaciones
    /// (`SolicitarAlquileresAbiertos`). Si alguna no responde, se continúa con
    /// datos parciales (como pide el enunciado); sus alquileres se recuperan por
    /// la vía de las bicis huérfanas (Etapa 6) o cuando se reincorpore.
    fn reconstruir_registro(&mut self, ctx: &mut Context<Self>) {
        let propios: Vec<Alquiler> = self
            .alquileres_propios
            .values()
            .filter(|a| a.estado == EstadoAlquiler::Activo)
            .cloned()
            .collect();
        if let RolEstacion::Lider { registro, .. } = &mut self.rol {
            for alquiler in propios {
                registro.agregar(alquiler);
            }
        }
        let term = self.eleccion.term();
        let comunicador = self.comunicador.clone();
        let destinos: Vec<SocketAddr> = self
            .estaciones
            .iter()
            .filter(|(id, _)| **id != self.id)
            .map(|(_, addr)| *addr)
            .collect();
        let fut = async move {
            let mut recuperados = Vec::new();
            let (Some(comunicador), Ok(bytes)) = (
                comunicador,
                comun::serializacion::a_bytes(
                    &MensajeEntreEstacionesTCP::SolicitarAlquileresAbiertos { term },
                ),
            ) else {
                return recuperados;
            };
            for destino in destinos {
                let respuesta = comunicador
                    .send(ConsultarTcp {
                        destino,
                        datos: bytes.clone(),
                    })
                    .await;
                if let Ok(Some(datos)) = respuesta {
                    if let Ok(MensajeEntreEstacionesTCP::RespuestaAlquileres { alquileres }) =
                        comun::serializacion::desde_bytes(&datos)
                    {
                        recuperados.extend(alquileres);
                    }
                }
            }
            recuperados
        }
        .into_actor(self)
        .map(|alquileres, actor, _ctx| {
            if let RolEstacion::Lider { registro, .. } = &mut actor.rol {
                for alquiler in alquileres {
                    registro.agregar(alquiler);
                }
                println!(
                    "[{}] registro reconstruido: {} alquileres activos",
                    actor.id,
                    registro.activos()
                );
            }
        });
        ctx.spawn(fut);
    }

    /// Vigilancia del líder (corre cada `INTERVALO_VIGILANCIA`): un follower
    /// sondea al líder con un request-response; tras `UMBRAL_FALLOS_LIDER` fallos
    /// consecutivos lo da por caído e inicia una elección. Cubre tanto al líder
    /// caído (conexión rechazada, falla al instante) como al colgado (el sondeo
    /// vence por el timeout de lectura del Comunicador).
    fn vigilar_lider(&mut self, ctx: &mut Context<Self>) {
        match self.eleccion.lider_conocido() {
            EstadoLider::Conocido(lider_id) if lider_id == self.id => return, // el líder soy yo
            EstadoLider::Conocido(_) => {}
            EstadoLider::EnEleccion => {
                // Una elección puede quedar trabada si el que tenía que reenviar
                // se cayó con el mensaje encima: tras unos intervalos sin
                // resolverse, la reiniciamos.
                self.intervalos_en_eleccion += 1;
                if self.intervalos_en_eleccion >= UMBRAL_ELECCION_TRABADA {
                    self.intervalos_en_eleccion = 0;
                    let accion = self.eleccion.iniciar();
                    self.procesar_accion_ring(accion, ctx);
                }
                return;
            }
            EstadoLider::Desconocido => {
                // No hay líder que vigilar: directamente elegimos uno.
                let accion = self.eleccion.iniciar();
                self.procesar_accion_ring(accion, ctx);
                return;
            }
        }
        let comunicador = self.comunicador.clone();
        let lider = self.lider;
        let fut = async move { sondear_lider(&comunicador, lider).await }
            .into_actor(self)
            .map(|vivo, actor, ctx| {
                if vivo {
                    actor.fallos_lider = 0;
                    // El líder responde: si quedaron eventos diferidos (p.ej. de
                    // un corte transitorio), este es el momento de reenviarlos.
                    actor.descargar_pendientes(ctx);
                    return;
                }
                actor.fallos_lider += 1;
                if actor.fallos_lider >= UMBRAL_FALLOS_LIDER {
                    actor.fallos_lider = 0;
                    println!(
                        "[{}] el líder {} no responde: inicio elección",
                        actor.id, actor.lider_id
                    );
                    let accion = actor.eleccion.iniciar();
                    actor.procesar_accion_ring(accion, ctx);
                }
            });
        ctx.spawn(fut);
    }

    /// Datos que necesita `procesar_operacion`, capturados antes del trabajo async.
    fn contexto(&mut self, ctx: &mut Context<Self>) -> ContextoOperacion {
        let n = self.proximo();
        ContextoOperacion {
            tx_id: TransaccionId(format!("T-{}-{}", self.id.0, n)),
            rental_id: RentalId(format!("R-{}-{}", self.id.0, n)),
            slots: self.slots.clone(),
            estacion_origen: self.id,
            pasarela: self.pasarela,
            comunicador: self.comunicador.clone(),
            yo: ctx.address(),
        }
    }

    /// Reintenta los commits decididos que la pasarela todavía no confirmó
    /// (Caso C). El re-Commit es seguro: la pasarela es idempotente. Si la
    /// pasarela responde `PreauthAnulada` (la anuló por su propio timeout), no
    /// queda nada que completar y la constancia se descarta.
    fn reintentar_commits(&mut self, ctx: &mut Context<Self>) {
        if self.commits_pendientes.is_empty() {
            return;
        }
        let pendientes: Vec<(TransaccionId, String)> = self
            .commits_pendientes
            .iter()
            .map(|(t, p)| (t.clone(), p.clone()))
            .collect();
        println!(
            "[{}] reintento {} commits pendientes contra la pasarela",
            self.id,
            pendientes.len()
        );
        let comunicador = self.comunicador.clone();
        let pasarela = self.pasarela;
        let fut = async move {
            let mut resueltos = Vec::new();
            for (tx_id, preauth_id) in pendientes {
                let commit = MensajeEstacionAPasarela::CommitPreauth {
                    tx_id: tx_id.clone(),
                    preauth_id,
                };
                match comun::tiempo::con_timeout(
                    TIMEOUT_PREPARE,
                    consultar_pasarela(&comunicador, pasarela, &commit),
                )
                .await
                .flatten()
                {
                    Some(MensajePasarelaAEstacion::PreauthConfirmada { .. })
                    | Some(MensajePasarelaAEstacion::PreauthAnulada { .. }) => {
                        resueltos.push(tx_id);
                    }
                    _ => {} // sigue pendiente, se reintenta en el próximo intervalo
                }
            }
            resueltos
        }
        .into_actor(self)
        .map(|resueltos, actor, _ctx| {
            if !resueltos.is_empty() {
                for tx_id in resueltos {
                    actor.commits_pendientes.remove(&tx_id);
                }
                actor.persistir();
            }
        });
        ctx.spawn(fut);
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
    /// `Addr` de la propia estación: el 2PC le avisa el commit decidido (para
    /// que lo persista ANTES de mandarlo) y la confirmación de la pasarela.
    yo: Addr<Estacion>,
}

/// Distancia aproximada en km entre dos puntos `(lat, lon)` en grados, con la
/// fórmula equirectangular (suficiente para distancias urbanas y mucho más barata
/// que la de Haversine). No usamos crates externas: solo `f64` de la std.
fn distancia_km(a: (f64, f64), b: (f64, f64)) -> f64 {
    const R_TIERRA_KM: f64 = 6371.0;
    let (lat1, lon1) = (a.0.to_radians(), a.1.to_radians());
    let (lat2, lon2) = (b.0.to_radians(), b.1.to_radians());
    let x = (lon2 - lon1) * ((lat1 + lat2) / 2.0).cos();
    let y = lat2 - lat1;
    R_TIERRA_KM * (x * x + y * y).sqrt()
}

impl Actor for Estacion {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!(
            "[{}] actor Estacion iniciado (ubicacion {:?}, {} slots, pasarela {})",
            self.id,
            self.ubicacion,
            self.slots.len(),
            self.pasarela
        );
        // Gossip: cada estación (incluido el líder, a sí mismo) le manda su estado
        // al líder por UDP. Así el líder arma su cache para responder consultas.
        ctx.run_interval(INTERVALO_GOSSIP, |act, ctx| act.enviar_estado(ctx));
        // Vigilancia del líder: si deja de responder, se inicia una elección.
        ctx.run_interval(INTERVALO_VIGILANCIA, |act, ctx| act.vigilar_lider(ctx));
        // Caso C: commits decididos sin confirmar se reintentan (también cubre
        // a los recuperados de disco tras un reinicio).
        ctx.run_interval(self.intervalo_reintento_commits, |act, ctx| {
            act.reintentar_commits(ctx)
        });
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
            RolEstacion::Lider { registro, .. } => registro.activos(),
            RolEstacion::Follower => 0,
        }
    }
}

impl Handler<ConsultarCache> for Estacion {
    type Result = usize;

    fn handle(&mut self, _msg: ConsultarCache, _ctx: &mut Self::Context) -> usize {
        match &self.rol {
            RolEstacion::Lider { cache, .. } => cache.len(),
            RolEstacion::Follower => 0,
        }
    }
}

impl Handler<ConsultarPendientes> for Estacion {
    type Result = usize;

    fn handle(&mut self, _msg: ConsultarPendientes, _ctx: &mut Self::Context) -> usize {
        self.eventos_pendientes.len()
    }
}

impl Handler<RegistrarCommitPendiente> for Estacion {
    type Result = ();

    fn handle(&mut self, msg: RegistrarCommitPendiente, _ctx: &mut Self::Context) {
        self.commits_pendientes.insert(msg.tx_id, msg.preauth_id);
        // La respuesta de este handler ES la garantía: el 2PC espera este send
        // antes de mandar el Commit, así que al persistir acá la constancia
        // queda en disco antes de que el Commit salga a la red.
        self.persistir();
    }
}

impl Handler<CommitConfirmado> for Estacion {
    type Result = ();

    fn handle(&mut self, msg: CommitConfirmado, _ctx: &mut Self::Context) {
        if self.commits_pendientes.remove(&msg.tx_id).is_some() {
            self.persistir();
        }
    }
}

impl Handler<ConsultarCommitsPendientes> for Estacion {
    type Result = usize;

    fn handle(&mut self, _msg: ConsultarCommitsPendientes, _ctx: &mut Self::Context) -> usize {
        self.commits_pendientes.len()
    }
}

impl Handler<ConsultarLider> for Estacion {
    type Result = MessageResult<ConsultarLider>;

    fn handle(&mut self, _msg: ConsultarLider, _ctx: &mut Self::Context) -> Self::Result {
        let lider_id = match self.eleccion.lider_conocido() {
            EstadoLider::Conocido(id) => Some(id),
            _ => None,
        };
        MessageResult(InfoLider {
            lider_id,
            term: self.eleccion.term(),
            soy_lider: matches!(self.rol, RolEstacion::Lider { .. }),
        })
    }
}

impl Handler<ProcesarDevolucion> for Estacion {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, msg: ProcesarDevolucion, _ctx: &mut Self::Context) -> Self::Result {
        // Datos de cobro: si soy el líder los busco en mi registro (en proceso);
        // si soy follower los consulto por red (más abajo).
        let es_lider = matches!(self.rol, RolEstacion::Lider { .. });
        let datos_si_lider = if let RolEstacion::Lider { registro, .. } = &self.rol {
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
                    // El líder puede no tener el alquiler todavía (el reporte del
                    // origen viaja en paralelo): ante NoRegistradoAun (o un fallo
                    // de red) se reintenta con una espera creciente.
                    let mut datos = None;
                    for intento in 0..REINTENTOS_NOTIFICACION {
                        if intento > 0 {
                            actix::clock::sleep(ESPERA_REINTENTO * intento).await;
                        }
                        datos = consultar_lider_devolucion(
                            &comunicador,
                            lider,
                            event_id.clone(),
                            bici_id,
                            mi_id,
                            t1,
                        )
                        .await;
                        if datos.is_some() {
                            break;
                        }
                    }
                    datos
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
            .map(move |resultado, actor, ctx| {
                let Some((datos, monto, event_id)) = resultado else {
                    return; // Agotados los reintentos: bici huérfana (PR de la 8.2.1).
                };
                let tiempo = datos.t0.minutos_hasta(t1);

                // Cerrar en el líder (en proceso si soy líder; por red si no, con
                // cola de diferidos si no está disponible).
                if let RolEstacion::Lider { registro, .. } = &mut actor.rol {
                    registro.cerrar(&datos.rental_id);
                } else {
                    actor.enviar_evento_al_lider(
                        MensajeEntreEstacionesTCP::DevolucionProcesada {
                            event_id,
                            rental_id: datos.rental_id.clone(),
                            monto_cobrado: monto,
                            tiempo_uso_minutos: tiempo,
                        },
                        ctx,
                    );
                }

                // Cerrar en el origen (en proceso si el origen soy yo; por red si no).
                if datos.estacion_origen == actor.id {
                    if let Some(alquiler) = actor.alquileres_propios.get_mut(&datos.rental_id) {
                        alquiler.estado = EstadoAlquiler::Cerrado;
                        actor.persistir();
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

/// Sondea al líder con un `PreguntarLider` (request-response): `true` si contestó
/// algo, `false` si no se pudo conectar o no respondió. Reusa el mensaje de
/// discovery: no hace falta un "ping" propio en el protocolo.
async fn sondear_lider(comunicador: &Option<Addr<Comunicador>>, lider: SocketAddr) -> bool {
    let Some(comunicador) = comunicador else {
        // Sin red cableada (tests unitarios) no se vigila a nadie.
        return true;
    };
    let consulta = MensajeUsuario::Consulta(MensajeUsuarioAEstacionConsulta::PreguntarLider);
    let Ok(bytes) = comun::serializacion::a_bytes(&consulta) else {
        return true;
    };
    matches!(
        comunicador
            .send(ConsultarTcp {
                destino: lider,
                datos: bytes,
            })
            .await,
        Ok(Some(_))
    )
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
        yo,
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
            // Caso A: si la pasarela no vota dentro del plazo, es un No implícito
            // (el alquiler se aborta y el slot conserva su bici).
            let respuesta_pasarela = comun::tiempo::con_timeout(
                TIMEOUT_PREPARE,
                consultar_pasarela(&comunicador, pasarela, &prepare),
            )
            .await;
            let hubo_timeout = respuesta_pasarela.is_none();
            let (pasarela_ok, preauth_id) = match respuesta_pasarela.flatten() {
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
                // Caso C: constancia (persistida) de la decisión ANTES de
                // mandar el Commit. Si el proceso se cae acá en el medio, el
                // reintento periódico lo completa al volver (la pasarela es
                // idempotente al re-Commit).
                let _ = yo
                    .send(RegistrarCommitPendiente {
                        tx_id: tx_id.clone(),
                        preauth_id: preauth_id.clone(),
                    })
                    .await;
                let commit = MensajeEstacionAPasarela::CommitPreauth {
                    tx_id: tx_id.clone(),
                    preauth_id: preauth_id.clone(),
                };
                // También con plazo: una pasarela colgada acá no debe dejar al
                // usuario esperando (el reintento se encarga después).
                let respuesta_commit = comun::tiempo::con_timeout(
                    TIMEOUT_PREPARE,
                    consultar_pasarela(&comunicador, pasarela, &commit),
                )
                .await
                .flatten();
                if matches!(
                    respuesta_commit,
                    Some(MensajePasarelaAEstacion::PreauthConfirmada { .. })
                ) {
                    yo.do_send(CommitConfirmado { tx_id });
                }

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
                    let _ = comun::tiempo::con_timeout(
                        TIMEOUT_PREPARE,
                        consultar_pasarela(&comunicador, pasarela, &abort),
                    )
                    .await;
                }
                let motivo = if voto_slot != Voto::Si {
                    "el slot no tenía una bici disponible".to_string()
                } else if hubo_timeout {
                    "timeout en preparación".to_string()
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
        let ctx = self.contexto(_ctx);
        Box::pin(
            async move { procesar_operacion(msg.0, ctx).await }
                .into_actor(self)
                .map(|(respuesta, alquiler), actor, ctx| {
                    if let Some(a) = alquiler {
                        actor.reportar_alquiler(&a, ctx);
                        actor.alquileres_propios.insert(a.rental_id.clone(), a);
                        actor.persistir();
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

    fn handle(&mut self, msg: PaqueteRecibido, ctx: &mut Self::Context) -> Self::Result {
        // ¿Es un mensaje entre estaciones (reporte/devolución al líder, Ring, etc.)?
        if let Ok(entre_estaciones) =
            comun::serializacion::desde_bytes::<MensajeEntreEstacionesTCP>(&msg.datos)
        {
            self.manejar_entre_estaciones(entre_estaciones, msg.responder, ctx);
            return Box::pin(async {}.into_actor(self).map(|_, _, _| ()));
        }

        // ¿Es gossip de estado (UDP)? Solo le interesa al líder, que actualiza su cache.
        if let Ok(MensajeEntreEstacionesUDP::EstadoEstacion {
            estacion_id,
            ubicacion,
            bicis_disponibles,
            slots_libres,
            timestamp,
        }) = comun::serializacion::desde_bytes::<MensajeEntreEstacionesUDP>(&msg.datos)
        {
            if let RolEstacion::Lider { cache, .. } = &mut self.rol {
                cache.insert(
                    estacion_id,
                    InfoEstacion {
                        estacion_id,
                        ubicacion,
                        bicis_disponibles,
                        slots_libres,
                        last_seen: timestamp,
                    },
                );
            }
            return Box::pin(async {}.into_actor(self).map(|_, _, _| ()));
        }

        let pedido: Option<MensajeUsuario> = comun::serializacion::desde_bytes(&msg.datos).ok();

        match (pedido, msg.responder) {
            (Some(MensajeUsuario::Consulta(consulta)), Some(responder)) => {
                let respuesta = self.manejar_consulta(consulta);
                if let Ok(bytes) = comun::serializacion::a_bytes(&respuesta) {
                    responder.responder(bytes);
                }
                Box::pin(async {}.into_actor(self).map(|_, _, _| ()))
            }
            (Some(MensajeUsuario::Operacion(operacion)), Some(responder)) => {
                let ctx = self.contexto(ctx);
                Box::pin(
                    async move {
                        let (respuesta, alquiler) = procesar_operacion(operacion, ctx).await;
                        (respuesta, alquiler, responder)
                    }
                    .into_actor(self)
                    .map(|(respuesta, alquiler, responder), actor, ctx| {
                        if let Some(a) = alquiler {
                            actor.reportar_alquiler(&a, ctx);
                            actor.alquileres_propios.insert(a.rental_id.clone(), a);
                            actor.persistir();
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

    fn paquete_udp(msg: &MensajeEntreEstacionesUDP) -> PaqueteRecibido {
        PaqueteRecibido {
            transporte: Transporte::Udp,
            datos: comun::serializacion::a_bytes(msg).unwrap(),
            responder: None,
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
            (EstacionId(1), lider),
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

            // El cierre (ProcesarDevolucion) corre en background: el cobro pasa
            // por el Comunicador (asincrónico), así que esperamos a que el
            // registro refleje el cierre en vez de asumir el orden.
            let mut activos = usize::MAX;
            for _ in 0..50 {
                activos = estacion.send(ConsultarRegistro).await.unwrap();
                if activos == 0 {
                    break;
                }
                actix::clock::sleep(std::time::Duration::from_millis(100)).await;
            }
            assert_eq!(activos, 0, "la devolución debería cerrar el alquiler");
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

    /// Pasarela "colgada": acepta la conexión, lee el pedido y nunca contesta.
    /// Mantiene el stream vivo para que el timeout sea de verdad por espera (no
    /// por conexión cerrada).
    fn pasarela_muda() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let mut conexiones = Vec::new();
            for conexion in listener.incoming() {
                let Ok(mut stream) = conexion else { continue };
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                conexiones.push(stream); // la dejamos abierta, sin responder
            }
        });
        addr
    }

    #[test]
    fn timeout_de_prepare_es_no_implicito_y_el_slot_conserva_la_bici() {
        System::new().block_on(async {
            // Caso A: la pasarela no vota dentro del plazo (TIMEOUT_PREPARE = 3s).
            let pasarela = pasarela_muda();
            let s0 = Slot::con_bici(0, BiciId(42)).start();
            let estacion = arrancar(vec![s0.clone()], pasarela).await;

            let resp = estacion.send(alquilar(0)).await.unwrap();
            match resp {
                MensajeEstacionAUsuario::AlquilerRechazado { motivo } => {
                    assert_eq!(motivo, "timeout en preparación");
                }
                otro => panic!("esperaba AlquilerRechazado por timeout, fue {otro:?}"),
            }
            // El abort liberó la reserva del slot: la bici sigue ahí y se puede
            // volver a intentar.
            let estado = s0.send(ConsultarEstado).await.unwrap();
            assert_eq!(estado.bici_id, Some(BiciId(42)), "la bici no se pierde");
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

    #[test]
    fn el_lider_guarda_el_gossip_en_su_cache() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(true);
            let estacion = arrancar(vec![Slot::nuevo(0).start()], pasarela).await;

            // Arranca con la cache vacía.
            assert_eq!(estacion.send(ConsultarCache).await.unwrap(), 0);

            // Llega gossip de otra estación → el líder lo guarda.
            let gossip = MensajeEntreEstacionesUDP::EstadoEstacion {
                estacion_id: EstacionId(2),
                ubicacion: (-34.61, -58.41),
                bicis_disponibles: 4,
                slots_libres: 6,
                timestamp: Timestamp(123),
            };
            estacion.send(paquete_udp(&gossip)).await.unwrap();
            assert_eq!(estacion.send(ConsultarCache).await.unwrap(), 1);

            // Otro snapshot de la MISMA estación no agrega una entrada (es un upsert).
            estacion.send(paquete_udp(&gossip)).await.unwrap();
            assert_eq!(estacion.send(ConsultarCache).await.unwrap(), 1);
        });
    }

    /// Gossip de una estación con `bicis` bicis en `ubicacion`.
    fn gossip(id: u32, ubicacion: (f64, f64), bicis: u32) -> PaqueteRecibido {
        paquete_udp(&MensajeEntreEstacionesUDP::EstadoEstacion {
            estacion_id: EstacionId(id),
            ubicacion,
            bicis_disponibles: bicis,
            slots_libres: 10 - bicis,
            timestamp: Timestamp(0),
        })
    }

    /// Manda una consulta a la estación (vía `PaqueteRecibido` con responder) y
    /// devuelve la respuesta tipada.
    async fn consultar(
        estacion: &Addr<Estacion>,
        consulta: MensajeUsuarioAEstacionConsulta,
    ) -> MensajeEstacionAUsuarioConsulta {
        let (resp, rx) = Responder::canal();
        let datos = comun::serializacion::a_bytes(&MensajeUsuario::Consulta(consulta)).unwrap();
        estacion
            .send(PaqueteRecibido {
                transporte: Transporte::Tcp,
                datos,
                responder: Some(resp),
            })
            .await
            .unwrap();
        comun::serializacion::desde_bytes(&rx.recv().unwrap()).unwrap()
    }

    #[test]
    fn discovery_devuelve_al_lider() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(true);
            let estacion = arrancar(vec![Slot::nuevo(0).start()], pasarela).await;

            let resp = consultar(&estacion, MensajeUsuarioAEstacionConsulta::PreguntarLider).await;
            match resp {
                MensajeEstacionAUsuarioConsulta::RespuestaLider { lider_id, .. } => {
                    assert_eq!(lider_id, EstacionId(1));
                }
                otra => panic!("esperaba RespuestaLider, fue {otra:?}"),
            }
        });
    }

    #[test]
    fn consulta_de_disponibilidad_filtra_por_proximidad_y_por_bicis() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(true);
            let estacion = arrancar(vec![Slot::nuevo(0).start()], pasarela).await;

            // Cargamos la cache con tres estaciones cerca de Buenos Aires:
            //  - 2: cerca, con bicis  → debe aparecer
            //  - 3: cerca, sin bicis  → se filtra (no sirve para alquilar)
            //  - 4: lejos (La Plata), con bicis → se filtra por distancia
            estacion.send(gossip(2, (-34.60, -58.40), 5)).await.unwrap();
            estacion.send(gossip(3, (-34.61, -58.41), 0)).await.unwrap();
            estacion.send(gossip(4, (-34.92, -57.95), 5)).await.unwrap();

            let resp = consultar(
                &estacion,
                MensajeUsuarioAEstacionConsulta::ConsultaDisponibilidad {
                    usuario_id: UsuarioId("alice".to_string()),
                    ubicacion: (-34.60, -58.40),
                    radio_max_km: 5.0,
                },
            )
            .await;
            match resp {
                MensajeEstacionAUsuarioConsulta::RespuestaDisponibilidad { estaciones } => {
                    let ids: Vec<u32> = estaciones.iter().map(|e| e.estacion_id.0).collect();
                    assert_eq!(ids, vec![2], "solo la 2 está cerca y con bicis");
                }
                otra => panic!("esperaba RespuestaDisponibilidad, fue {otra:?}"),
            }
        });
    }

    fn alquiler_abierto(event: &str, rental: &str, bici: u32) -> MensajeEntreEstacionesTCP {
        MensajeEntreEstacionesTCP::AlquilerAbierto {
            event_id: EventId(event.to_string()),
            rental_id: RentalId(rental.to_string()),
            bici_id: BiciId(bici),
            usuario_id: UsuarioId("alice".to_string()),
            estacion_origen: EstacionId(2),
            t0: Timestamp(0),
            preauth_id: "P-1".to_string(),
        }
    }

    #[test]
    fn el_lider_ignora_un_evento_duplicado() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(true);
            let estacion = arrancar(vec![], pasarela).await; // líder

            // Primer reporte: se registra.
            estacion
                .send(paquete_tcp(&alquiler_abierto("E1", "R1", 1), None))
                .await
                .unwrap();
            assert_eq!(estacion.send(ConsultarRegistro).await.unwrap(), 1);

            // El MISMO event_id otra vez (reintento de la cola de diferidos):
            // no se aplica, aunque el contenido difiera.
            estacion
                .send(paquete_tcp(&alquiler_abierto("E1", "R2", 2), None))
                .await
                .unwrap();
            assert_eq!(estacion.send(ConsultarRegistro).await.unwrap(), 1);

            // Un evento nuevo sí entra.
            estacion
                .send(paquete_tcp(&alquiler_abierto("E2", "R2", 2), None))
                .await
                .unwrap();
            assert_eq!(estacion.send(ConsultarRegistro).await.unwrap(), 2);
        });
    }

    #[test]
    fn los_eventos_al_lider_caido_se_difieren_y_se_aplican_al_asumir() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(true);
            // Follower con el líder (9) muerto desde el arranque.
            let muerto: SocketAddr = "127.0.0.1:19039".parse().unwrap();
            let estaciones: HashMap<EstacionId, SocketAddr> =
                [(EstacionId(9), muerto)].into_iter().collect();
            let s0 = Slot::con_bici(0, BiciId(42)).start();
            let estacion = Estacion::new(
                EstacionId(1),
                (0.0, 0.0),
                vec![s0],
                pasarela,
                (EstacionId(9), muerto),
                false,
                estaciones,
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

            // El alquiler sale bien (la pasarela está viva), pero el reporte al
            // líder muerto falla y queda diferido.
            let resp = estacion.send(alquilar(0)).await.unwrap();
            assert!(matches!(
                resp,
                MensajeEstacionAUsuario::AlquilerConfirmado { .. }
            ));
            actix::clock::sleep(std::time::Duration::from_millis(600)).await;
            assert_eq!(
                estacion.send(ConsultarPendientes).await.unwrap(),
                1,
                "el AlquilerAbierto quedó en la cola de diferidos"
            );

            // Llega un Coordinator que me nombra líder: descargo la cola sobre
            // mi propio registro.
            estacion
                .send(paquete_tcp(
                    &MensajeEntreEstacionesTCP::Coordinator {
                        lider: EstacionId(1),
                        term: 1,
                    },
                    None,
                ))
                .await
                .unwrap();
            actix::clock::sleep(std::time::Duration::from_millis(600)).await;
            assert_eq!(estacion.send(ConsultarPendientes).await.unwrap(), 0);
            assert_eq!(estacion.send(ConsultarRegistro).await.unwrap(), 1);
        });
    }

    /// Líder de mentira con guion: al primer `NotificarDevolucion` responde
    /// `NoRegistradoAun` (el reporte del origen "todavía no llegó"), al segundo
    /// entrega los datos de cobro. Todo evento sin respuesta (p.ej.
    /// `DevolucionProcesada`) se vuelca al canal para que el test lo verifique.
    /// También contesta el sondeo de la vigilancia para no disparar elecciones.
    fn lider_mock() -> (
        SocketAddr,
        std::sync::mpsc::Receiver<MensajeEntreEstacionesTCP>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut notificaciones = 0u32;
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
                    while let Some(payload) = desen.siguiente_payload() {
                        if let Ok(msg) =
                            comun::serializacion::desde_bytes::<MensajeEntreEstacionesTCP>(&payload)
                        {
                            match msg {
                                MensajeEntreEstacionesTCP::NotificarDevolucion {
                                    event_id, ..
                                } => {
                                    notificaciones += 1;
                                    let resp = if notificaciones == 1 {
                                        MensajeEntreEstacionesTCP::NoRegistradoAun { event_id }
                                    } else {
                                        MensajeEntreEstacionesTCP::DatosParaCobro {
                                            event_id,
                                            rental_id: RentalId("R-lider".to_string()),
                                            preauth_id: "P-mock".to_string(),
                                            t0: Timestamp(0),
                                            estacion_origen: EstacionId(2),
                                        }
                                    };
                                    let _ = stream.write_all(&enmarcar(&resp).unwrap());
                                }
                                otro => {
                                    let _ = tx.send(otro);
                                }
                            }
                        } else if comun::serializacion::desde_bytes::<MensajeUsuario>(&payload)
                            .is_ok()
                        {
                            let resp = MensajeEstacionAUsuarioConsulta::RespuestaLider {
                                lider_id: EstacionId(9),
                                lider_addr: addr,
                                term: 0,
                            };
                            let _ = stream.write_all(&enmarcar(&resp).unwrap());
                        }
                    }
                }
            }
        });
        (addr, rx)
    }

    #[test]
    fn la_devolucion_reintenta_cuando_el_lider_responde_no_registrado_aun() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(true);
            let (lider_addr, eventos) = lider_mock();
            let estaciones: HashMap<EstacionId, SocketAddr> =
                [(EstacionId(9), lider_addr)].into_iter().collect();
            let s0 = Slot::nuevo(0).start(); // vacío: puede aceptar la bici
            let estacion = Estacion::new(
                EstacionId(2),
                (0.0, 0.0),
                vec![s0],
                pasarela,
                (EstacionId(9), lider_addr),
                false,
                estaciones,
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

            // El usuario devuelve la bici: aceptada al toque, cierre en background.
            let resp = estacion
                .send(SolicitudUsuario(
                    MensajeUsuarioAEstacion::SolicitudDevolucion {
                        usuario_id: UsuarioId("alice".to_string()),
                        bici_id: BiciId(42),
                        rental_id: RentalId("R-lider".to_string()),
                        slot_id: 0,
                    },
                ))
                .await
                .unwrap();
            assert!(matches!(
                resp,
                MensajeEstacionAUsuario::DevolucionAceptada { .. }
            ));

            // El primer Notificar recibe NoRegistradoAun; el reintento obtiene
            // los datos, cobra y cierra con DevolucionProcesada hacia el líder.
            let mut recibido = None;
            for _ in 0..100 {
                if let Ok(msg) = eventos.try_recv() {
                    recibido = Some(msg);
                    break;
                }
                actix::clock::sleep(std::time::Duration::from_millis(100)).await;
            }
            match recibido {
                Some(MensajeEntreEstacionesTCP::DevolucionProcesada { rental_id, .. }) => {
                    assert_eq!(rental_id, RentalId("R-lider".to_string()));
                }
                otro => panic!("esperaba DevolucionProcesada tras el reintento, fue {otro:?}"),
            }
        });
    }

    #[test]
    fn los_alquileres_propios_sobreviven_un_reinicio() {
        System::new().block_on(async {
            let ruta = std::env::temp_dir().join("tp-bicis-test-estacion-reinicio.json");
            let _ = std::fs::remove_file(&ruta);
            let pasarela = pasarela_mock(true);
            let lider = "127.0.0.1:9".parse().unwrap();

            // "Primera corrida": la estación (líder por config) alquila y persiste.
            let s0 = Slot::con_bici(0, BiciId(42)).start();
            let e1 = Estacion::new(
                EstacionId(1),
                (0.0, 0.0),
                vec![s0],
                pasarela,
                (EstacionId(1), lider),
                true,
                HashMap::new(),
            )
            .con_persistencia(ruta.clone())
            .start();
            let comunicador = Comunicador::new(
                "127.0.0.1:0".parse().unwrap(),
                "127.0.0.1:0".parse().unwrap(),
                e1.clone().recipient(),
            )
            .start();
            e1.send(RegistrarComunicador(comunicador)).await.unwrap();
            let resp = e1.send(alquilar(0)).await.unwrap();
            assert!(matches!(
                resp,
                MensajeEstacionAUsuario::AlquilerConfirmado { .. }
            ));
            assert_eq!(e1.send(ConsultarRegistro).await.unwrap(), 1);

            // "Reinicio": otra instancia con el mismo archivo recupera sus
            // alquileres y, como arranca de líder, repuebla su registro.
            let e2 = Estacion::new(
                EstacionId(1),
                (0.0, 0.0),
                vec![],
                pasarela,
                (EstacionId(1), lider),
                true,
                HashMap::new(),
            )
            .con_persistencia(ruta.clone())
            .start();
            assert_eq!(
                e2.send(ConsultarRegistro).await.unwrap(),
                1,
                "el alquiler activo sobrevive el reinicio"
            );

            let _ = std::fs::remove_file(&ruta);
        });
    }

    /// Pasarela con guion para el Caso C: vota Sí al Prepare, "pierde" el
    /// primer Commit (cierra la conexión sin responder) y confirma el segundo.
    fn pasarela_mock_commit_perdido() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let mut commits = 0u32;
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
                        if matches!(pedido, MensajeEstacionAPasarela::CommitPreauth { .. }) {
                            commits += 1;
                            if commits == 1 {
                                break; // primer Commit: se "pierde" (sin respuesta)
                            }
                        }
                        let resp = responder_mock(pedido, true);
                        let _ = stream.write_all(&enmarcar(&resp).unwrap());
                        break;
                    }
                }
            }
        });
        addr
    }

    #[test]
    fn un_commit_perdido_se_completa_con_el_reintento() {
        System::new().block_on(async {
            let pasarela = pasarela_mock_commit_perdido();
            let lider = "127.0.0.1:9".parse().unwrap();
            let s0 = Slot::con_bici(0, BiciId(42)).start();
            let estacion = Estacion::new(
                EstacionId(1),
                (0.0, 0.0),
                vec![s0],
                pasarela,
                (EstacionId(1), lider),
                true,
                HashMap::new(),
            )
            .con_intervalo_de_reintento(std::time::Duration::from_millis(300))
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

            // El alquiler se confirma igual (la decisión ya era COMMIT), pero
            // la constancia queda pendiente porque la pasarela no respondió.
            let resp = estacion.send(alquilar(0)).await.unwrap();
            assert!(matches!(
                resp,
                MensajeEstacionAUsuario::AlquilerConfirmado { .. }
            ));
            assert_eq!(
                estacion.send(ConsultarCommitsPendientes).await.unwrap(),
                1,
                "el commit sin confirmar queda registrado"
            );

            // El reintento periódico completa el Commit (la pasarela responde
            // al segundo intento) y la constancia se borra.
            let mut pendientes = usize::MAX;
            for _ in 0..30 {
                actix::clock::sleep(std::time::Duration::from_millis(200)).await;
                pendientes = estacion.send(ConsultarCommitsPendientes).await.unwrap();
                if pendientes == 0 {
                    break;
                }
            }
            assert_eq!(pendientes, 0, "el reintento debe completar el commit");
        });
    }

    /// Escenario completo de la Etapa 5: el líder (9) está caído desde el
    /// arranque. Las estaciones vivas (1, 2, 3) lo detectan por el sondeo,
    /// corren el Ring salteando al muerto (el sucesor de la 3 es la 9), gana la
    /// 3 (mayor id vivo), y el nuevo líder reconstruye su registro con el
    /// alquiler que la 1 tenía abierto.
    #[test]
    fn caida_del_lider_dispara_eleccion_y_reconstruye_el_registro() {
        System::new().block_on(async {
            let pasarela = pasarela_mock(true);
            // Puertos fijos del rango de tests: las estaciones se tienen que
            // conocer entre sí ANTES de arrancar (igual que con la config real).
            let puerto = |id: u32| 19020 + id as u16;
            let direccion = |id: u32| SocketAddr::from(([127, 0, 0, 1], puerto(id)));
            let estaciones: HashMap<EstacionId, SocketAddr> = [1, 2, 3, 9]
                .into_iter()
                .map(|id| (EstacionId(id), direccion(id)))
                .collect();

            let mut actores = Vec::new();
            for id in [1u32, 2, 3] {
                let slots = vec![Slot::con_bici(0, BiciId(40 + id)).start()];
                let estacion = Estacion::new(
                    EstacionId(id),
                    (0.0, 0.0),
                    slots,
                    pasarela,
                    (EstacionId(9), direccion(9)), // líder por config: muerto
                    false,
                    estaciones.clone(),
                )
                .start();
                let comunicador =
                    Comunicador::new(direccion(id), direccion(id), estacion.clone().recipient())
                        .start();
                estacion
                    .send(RegistrarComunicador(comunicador))
                    .await
                    .unwrap();
                actores.push(estacion);
            }

            // La 1 alquila: le queda un alquiler propio activo (el reporte al
            // líder muerto se pierde; lo recupera la reconstrucción).
            let resp = actores[0].send(alquilar(0)).await.unwrap();
            assert!(matches!(
                resp,
                MensajeEstacionAUsuario::AlquilerConfirmado { .. }
            ));

            // Vigilancia (2s) × umbral (2 fallos) ≈ 4s hasta la elección; después
            // circula el Ring y se reconstruye el registro. Esperamos hasta 15s.
            let mut convergio = false;
            for _ in 0..30 {
                actix::clock::sleep(std::time::Duration::from_millis(500)).await;
                let info = actores[2].send(ConsultarLider).await.unwrap();
                if info.soy_lider
                    && info.lider_id == Some(EstacionId(3))
                    && actores[2].send(ConsultarRegistro).await.unwrap() == 1
                {
                    convergio = true;
                    break;
                }
            }
            assert!(
                convergio,
                "la 3 debería asumir como líder con el registro reconstruido (1 alquiler)"
            );

            // Las demás reconocen a la 3 como líder, con term post-elección.
            for (i, actor) in actores.iter().enumerate().take(2) {
                let info = actor.send(ConsultarLider).await.unwrap();
                assert_eq!(
                    info.lider_id,
                    Some(EstacionId(3)),
                    "la estación {} debería reconocer a la 3",
                    i + 1
                );
                assert!(info.term >= 1, "el term debe haber avanzado");
                assert!(!info.soy_lider);
            }
        });
    }
}
