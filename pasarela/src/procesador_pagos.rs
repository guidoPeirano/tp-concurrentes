//! Actor que simula la pasarela bancaria.
//!
//! Maneja pre-autorizaciones (reserva tentativa de un monto) y cobros
//! definitivos, participando del 2PC de alquiler como participante remoto.
//! Garantiza idempotencia: reintentos del mismo `tx_id`/`preauth_id` no
//! reprocesan. La persistencia en disco llega en la Etapa 6.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use actix::prelude::*;
use comun::comunicador::PaqueteRecibido;
use comun::config::TarifaConfig;
use comun::mensajes::estacion_pasarela::{
    MensajeEstacionAPasarela, MensajePasarelaAEstacion, VotoResultado,
};
use comun::{DatosTarjeta, TransaccionId};
use serde::{Deserialize, Serialize};

use crate::mensajes::PedidoPasarela;
use crate::tarifa::calcular_monto;

/// Cuánto espera la pasarela la decisión (Commit/Abort) después de votar Sí,
/// antes de anular la pre-autorización por su cuenta y liberar los fondos
/// (Caso B de la sección 7.1.1 del README).
const TIMEOUT_TRANSACCION: Duration = Duration::from_secs(10);

pub struct ProcesadorPagos {
    /// Pre-autorizaciones por `preauth_id`.
    pre_autorizaciones: HashMap<String, PreAutorizacion>,
    tarifa: TarifaConfig,
    contador: u64,
    timeout_transaccion: Duration,
    /// Archivo de persistencia (si está, el estado sobrevive un reinicio).
    archivo: Option<PathBuf>,
}

#[derive(Clone, Serialize, Deserialize)]
struct PreAutorizacion {
    tx_id: Option<TransaccionId>,
    monto_reservado: f64,
    estado: EstadoPreAuth,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum EstadoPreAuth {
    Preparada,
    Activa,
    Cobrada { monto_final: f64 },
    Anulada,
}

/// Lo que la pasarela guarda en disco: las preauths y el contador (para no
/// repetir ids después de un reinicio).
#[derive(Serialize, Deserialize)]
struct EstadoEnDisco {
    pre_autorizaciones: HashMap<String, PreAutorizacion>,
    contador: u64,
}

impl ProcesadorPagos {
    pub fn new(tarifa: TarifaConfig) -> Self {
        Self {
            pre_autorizaciones: HashMap::new(),
            tarifa,
            contador: 0,
            timeout_transaccion: TIMEOUT_TRANSACCION,
            archivo: None,
        }
    }

    /// Activa la persistencia en `ruta`: si ya hay un estado guardado lo carga
    /// (recuperación tras reinicio), y de acá en más cada cambio se guarda.
    pub fn con_persistencia(mut self, ruta: PathBuf) -> Self {
        if let Some(estado) = comun::persistencia::cargar::<EstadoEnDisco>(&ruta) {
            println!(
                "[pasarela] estado recuperado de {:?}: {} pre-autorizaciones",
                ruta,
                estado.pre_autorizaciones.len()
            );
            self.pre_autorizaciones = estado.pre_autorizaciones;
            self.contador = estado.contador;
        }
        self.archivo = Some(ruta);
        self
    }

    /// Vuelca el estado a disco (si la persistencia está activa).
    fn persistir(&self) {
        let Some(ruta) = &self.archivo else { return };
        let estado = EstadoEnDisco {
            pre_autorizaciones: self.pre_autorizaciones.clone(),
            contador: self.contador,
        };
        if let Err(e) = comun::persistencia::guardar(ruta, &estado) {
            eprintln!("[pasarela] no pude persistir el estado en {ruta:?}: {e}");
        }
    }

    /// Cambia el plazo del Caso B (lo usan los tests para no esperar 10s).
    /// Cuando los timeouts se levanten de la config, esto deja de ser solo de test.
    #[cfg(test)]
    pub fn con_timeout_de_transaccion(mut self, plazo: Duration) -> Self {
        self.timeout_transaccion = plazo;
        self
    }

    /// Si la respuesta fue un voto Sí (quedó una preauth `Preparada`), programa
    /// el aborto unilateral del Caso B: al vencer el plazo, si la decisión
    /// todavía no llegó, la preauth se anula y los fondos se liberan. Si en el
    /// medio hubo Commit (`Activa`) o Abort (`Anulada`), el timer no hace nada.
    fn programar_timeout(&self, respuesta: &MensajePasarelaAEstacion, ctx: &mut Context<Self>) {
        let MensajePasarelaAEstacion::Voto {
            resultado: VotoResultado::Yes,
            preauth_id: Some(preauth_id),
            ..
        } = respuesta
        else {
            return;
        };
        let preauth_id = preauth_id.clone();
        ctx.run_later(self.timeout_transaccion, move |pagos, _ctx| {
            if let Some(pa) = pagos.pre_autorizaciones.get_mut(&preauth_id) {
                if pa.estado == EstadoPreAuth::Preparada {
                    pa.estado = EstadoPreAuth::Anulada;
                    println!(
                        "[pasarela] timeout de la preauth {preauth_id}: anulada, fondos liberados"
                    );
                    pagos.persistir();
                }
            }
        });
    }

    fn nuevo_preauth_id(&mut self) -> String {
        self.contador += 1;
        format!("P-{}", self.contador)
    }

    /// Busca el `preauth_id` ya asociado a una transacción (para idempotencia del Prepare).
    fn preauth_de_tx(&self, tx_id: &TransaccionId) -> Option<String> {
        self.pre_autorizaciones
            .iter()
            .find(|(_, pa)| pa.tx_id.as_ref() == Some(tx_id))
            .map(|(id, _)| id.clone())
    }

    /// Resuelve un pedido de la estación. Es síncrono: la pasarela no depende de
    /// otros actores. Tanto el camino en proceso como el de red lo usan.
    fn procesar(&mut self, pedido: MensajeEstacionAPasarela) -> MensajePasarelaAEstacion {
        match pedido {
            MensajeEstacionAPasarela::PreparePreauth {
                tx_id,
                tarjeta,
                monto_propuesto,
                ..
            } => {
                // Idempotencia: si ya preparamos esta tx, devolvemos el mismo voto.
                if let Some(preauth_id) = self.preauth_de_tx(&tx_id) {
                    return MensajePasarelaAEstacion::Voto {
                        tx_id,
                        resultado: VotoResultado::Yes,
                        preauth_id: Some(preauth_id),
                    };
                }
                if !tarjeta_valida(&tarjeta) {
                    return MensajePasarelaAEstacion::Voto {
                        tx_id,
                        resultado: VotoResultado::No {
                            motivo: "tarjeta inválida".to_string(),
                        },
                        preauth_id: None,
                    };
                }
                let preauth_id = self.nuevo_preauth_id();
                self.pre_autorizaciones.insert(
                    preauth_id.clone(),
                    PreAutorizacion {
                        tx_id: Some(tx_id.clone()),
                        monto_reservado: monto_propuesto,
                        estado: EstadoPreAuth::Preparada,
                    },
                );
                MensajePasarelaAEstacion::Voto {
                    tx_id,
                    resultado: VotoResultado::Yes,
                    preauth_id: Some(preauth_id),
                }
            }

            MensajeEstacionAPasarela::CommitPreauth { preauth_id, .. } => {
                match self.pre_autorizaciones.get_mut(&preauth_id) {
                    // Caso B: la preauth se anuló por timeout y el Commit llegó
                    // tarde. No se puede confirmar: se informa que quedó anulada
                    // (el coordinador reintentará con un 2PC nuevo).
                    Some(pa) if pa.estado == EstadoPreAuth::Anulada => {
                        MensajePasarelaAEstacion::PreauthAnulada { preauth_id }
                    }
                    Some(pa) => {
                        // Idempotente: si ya estaba Activa, queda igual.
                        if pa.estado == EstadoPreAuth::Preparada {
                            pa.estado = EstadoPreAuth::Activa;
                        }
                        MensajePasarelaAEstacion::PreauthConfirmada { preauth_id }
                    }
                    // Confirmar una preauth desconocida no tiene sentido.
                    None => MensajePasarelaAEstacion::PreauthAnulada { preauth_id },
                }
            }

            MensajeEstacionAPasarela::AbortPreauth { preauth_id, .. } => {
                if let Some(pa) = self.pre_autorizaciones.get_mut(&preauth_id) {
                    pa.estado = EstadoPreAuth::Anulada;
                }
                MensajePasarelaAEstacion::PreauthAnulada { preauth_id }
            }

            MensajeEstacionAPasarela::ProcesarCobro { preauth_id, t0, t1 } => {
                match self.pre_autorizaciones.get_mut(&preauth_id) {
                    // Una preauth anulada (por Abort o por timeout) no se cobra.
                    Some(pa) if pa.estado == EstadoPreAuth::Anulada => {
                        MensajePasarelaAEstacion::CobroRechazado {
                            preauth_id,
                            motivo: "la pre-autorización está anulada".to_string(),
                        }
                    }
                    Some(pa) => {
                        // Idempotente: si ya se cobró, devolvemos el mismo monto.
                        if let EstadoPreAuth::Cobrada { monto_final } = pa.estado {
                            return MensajePasarelaAEstacion::CobroConfirmado {
                                preauth_id,
                                monto: monto_final,
                            };
                        }
                        // No se cobra más de lo pre-autorizado.
                        let monto = calcular_monto(&self.tarifa, t0, t1).min(pa.monto_reservado);
                        pa.estado = EstadoPreAuth::Cobrada { monto_final: monto };
                        MensajePasarelaAEstacion::CobroConfirmado { preauth_id, monto }
                    }
                    None => MensajePasarelaAEstacion::CobroRechazado {
                        preauth_id,
                        motivo: "no existe la pre-autorización".to_string(),
                    },
                }
            }
        }
    }
}

/// Validación de tarjeta (mock): exige número no vacío y CVV de 3 dígitos.
fn tarjeta_valida(tarjeta: &DatosTarjeta) -> bool {
    !tarjeta.numero.is_empty() && tarjeta.cvv.len() == 3
}

impl Actor for ProcesadorPagos {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        println!(
            "[pasarela] ProcesadorPagos iniciado (tarifa base={}, por_minuto={})",
            self.tarifa.base, self.tarifa.por_minuto
        );
    }
}

impl Handler<PedidoPasarela> for ProcesadorPagos {
    type Result = MessageResult<PedidoPasarela>;

    fn handle(&mut self, msg: PedidoPasarela, ctx: &mut Self::Context) -> Self::Result {
        let respuesta = self.procesar(msg.0);
        self.programar_timeout(&respuesta, ctx);
        self.persistir();
        MessageResult(respuesta)
    }
}

impl Handler<PaqueteRecibido> for ProcesadorPagos {
    type Result = ();

    fn handle(&mut self, msg: PaqueteRecibido, ctx: &mut Self::Context) {
        let pedido: Option<MensajeEstacionAPasarela> =
            comun::serializacion::desde_bytes(&msg.datos).ok();
        if let (Some(pedido), Some(responder)) = (pedido, msg.responder) {
            let respuesta = self.procesar(pedido);
            self.programar_timeout(&respuesta, ctx);
            // Persistir ANTES de responder: si nos caemos justo acá, el estado
            // guardado ya refleja lo que la estación va a dar por confirmado.
            self.persistir();
            if let Ok(bytes) = comun::serializacion::a_bytes(&respuesta) {
                responder.responder(bytes);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use comun::mensajes::estacion_pasarela::MensajeEstacionAPasarela as Pedido;
    use comun::{Timestamp, UsuarioId};

    fn tarifa() -> TarifaConfig {
        TarifaConfig {
            base: 50.0,
            por_minuto: 10.0,
        }
    }

    fn tarjeta(cvv: &str) -> DatosTarjeta {
        DatosTarjeta {
            numero: "4111111111111111".to_string(),
            titular: "Alice".to_string(),
            vencimiento: "12/29".to_string(),
            cvv: cvv.to_string(),
        }
    }

    fn prepare(tx: &str, cvv: &str) -> Pedido {
        Pedido::PreparePreauth {
            tx_id: TransaccionId(tx.to_string()),
            usuario_id: UsuarioId("alice".to_string()),
            tarjeta: tarjeta(cvv),
            monto_propuesto: 1000.0,
        }
    }

    /// Helper que crea el actor, le manda un pedido y devuelve la respuesta.
    async fn enviar(p: &Addr<ProcesadorPagos>, pedido: Pedido) -> MensajePasarelaAEstacion {
        p.send(PedidoPasarela(pedido)).await.unwrap()
    }

    #[test]
    fn prepare_valida_vota_si_y_crea_preauth() {
        System::new().block_on(async {
            let p = ProcesadorPagos::new(tarifa()).start();
            let resp = enviar(&p, prepare("T1", "123")).await;
            match resp {
                MensajePasarelaAEstacion::Voto {
                    resultado,
                    preauth_id,
                    ..
                } => {
                    assert_eq!(resultado, VotoResultado::Yes);
                    assert!(preauth_id.is_some());
                }
                otro => panic!("esperaba Voto, fue {otro:?}"),
            }
        });
    }

    #[test]
    fn prepare_tarjeta_invalida_vota_no() {
        System::new().block_on(async {
            let p = ProcesadorPagos::new(tarifa()).start();
            let resp = enviar(&p, prepare("T1", "1")).await; // cvv inválido
            assert!(matches!(
                resp,
                MensajePasarelaAEstacion::Voto {
                    resultado: VotoResultado::No { .. },
                    preauth_id: None,
                    ..
                }
            ));
        });
    }

    #[test]
    fn prepare_es_idempotente_por_tx() {
        System::new().block_on(async {
            let p = ProcesadorPagos::new(tarifa()).start();
            let id1 = preauth_id(enviar(&p, prepare("T1", "123")).await);
            let id2 = preauth_id(enviar(&p, prepare("T1", "123")).await);
            assert_eq!(id1, id2, "el mismo tx_id devuelve el mismo preauth_id");
        });
    }

    #[test]
    fn cobro_calcula_monto_y_es_idempotente() {
        System::new().block_on(async {
            let p = ProcesadorPagos::new(tarifa()).start();
            let id = preauth_id(enviar(&p, prepare("T1", "123")).await);
            enviar(
                &p,
                Pedido::CommitPreauth {
                    tx_id: TransaccionId("T1".to_string()),
                    preauth_id: id.clone(),
                },
            )
            .await;

            // 2 minutos → 50 + 10*2 = 70.
            let cobro = Pedido::ProcesarCobro {
                preauth_id: id.clone(),
                t0: Timestamp(0),
                t1: Timestamp(120_000),
            };
            let r1 = enviar(&p, cobro.clone()).await;
            let r2 = enviar(&p, cobro).await; // reintento
            assert_eq!(r1, r2, "el cobro es idempotente");
            assert!(matches!(
                r1,
                MensajePasarelaAEstacion::CobroConfirmado { monto, .. } if monto == 70.0
            ));
        });
    }

    fn preauth_id(resp: MensajePasarelaAEstacion) -> String {
        match resp {
            MensajePasarelaAEstacion::Voto {
                preauth_id: Some(id),
                ..
            } => id,
            otro => panic!("esperaba Voto con preauth_id, fue {otro:?}"),
        }
    }

    #[test]
    fn el_estado_sobrevive_un_reinicio() {
        System::new().block_on(async {
            let ruta = std::env::temp_dir().join("tp-bicis-test-pasarela-reinicio.json");
            let _ = std::fs::remove_file(&ruta);

            // "Primera corrida": prepare + commit, persistidos.
            let p1 = ProcesadorPagos::new(tarifa())
                .con_persistencia(ruta.clone())
                .start();
            let id = preauth_id(enviar(&p1, prepare("T1", "123")).await);
            enviar(
                &p1,
                Pedido::CommitPreauth {
                    tx_id: TransaccionId("T1".to_string()),
                    preauth_id: id.clone(),
                },
            )
            .await;

            // "Reinicio": una instancia nueva carga el estado desde el archivo
            // y puede cobrar la preauth que quedó activa.
            let p2 = ProcesadorPagos::new(tarifa())
                .con_persistencia(ruta.clone())
                .start();
            let cobro = enviar(
                &p2,
                Pedido::ProcesarCobro {
                    preauth_id: id,
                    t0: Timestamp(0),
                    t1: Timestamp(120_000),
                },
            )
            .await;
            assert!(
                matches!(cobro, MensajePasarelaAEstacion::CobroConfirmado { monto, .. } if monto == 70.0),
                "esperaba CobroConfirmado de 70 tras el reinicio, fue {cobro:?}"
            );

            // El contador también se recupera: una preauth nueva no repite id.
            let id2 = preauth_id(enviar(&p2, prepare("T2", "123")).await);
            assert_ne!(id2, "P-1", "el contador no debe arrancar de cero");

            let _ = std::fs::remove_file(&ruta);
        });
    }

    #[test]
    fn preauth_preparada_se_anula_sola_por_timeout() {
        System::new().block_on(async {
            // Plazo corto para no esperar los 10s reales.
            let p = ProcesadorPagos::new(tarifa())
                .con_timeout_de_transaccion(Duration::from_millis(100))
                .start();
            let id = preauth_id(enviar(&p, prepare("T1", "123")).await);

            // El coordinador "se cayó": no llega ni Commit ni Abort.
            actix::clock::sleep(Duration::from_millis(200)).await;

            // El Commit tardío no confirma: la preauth quedó anulada.
            let commit = enviar(
                &p,
                Pedido::CommitPreauth {
                    tx_id: TransaccionId("T1".to_string()),
                    preauth_id: id.clone(),
                },
            )
            .await;
            assert!(
                matches!(commit, MensajePasarelaAEstacion::PreauthAnulada { .. }),
                "esperaba PreauthAnulada, fue {commit:?}"
            );

            // Y por supuesto no se puede cobrar.
            let cobro = enviar(
                &p,
                Pedido::ProcesarCobro {
                    preauth_id: id,
                    t0: Timestamp(0),
                    t1: Timestamp(120_000),
                },
            )
            .await;
            assert!(
                matches!(cobro, MensajePasarelaAEstacion::CobroRechazado { .. }),
                "esperaba CobroRechazado, fue {cobro:?}"
            );
        });
    }

    #[test]
    fn el_commit_a_tiempo_evita_la_anulacion() {
        System::new().block_on(async {
            let p = ProcesadorPagos::new(tarifa())
                .con_timeout_de_transaccion(Duration::from_millis(100))
                .start();
            let id = preauth_id(enviar(&p, prepare("T1", "123")).await);

            // El Commit llega antes del plazo: la preauth queda Activa.
            let commit = enviar(
                &p,
                Pedido::CommitPreauth {
                    tx_id: TransaccionId("T1".to_string()),
                    preauth_id: id.clone(),
                },
            )
            .await;
            assert!(matches!(
                commit,
                MensajePasarelaAEstacion::PreauthConfirmada { .. }
            ));

            // El timer vence después y no la toca: el cobro sale normal.
            actix::clock::sleep(Duration::from_millis(200)).await;
            let cobro = enviar(
                &p,
                Pedido::ProcesarCobro {
                    preauth_id: id,
                    t0: Timestamp(0),
                    t1: Timestamp(120_000),
                },
            )
            .await;
            assert!(
                matches!(cobro, MensajePasarelaAEstacion::CobroConfirmado { monto, .. } if monto == 70.0),
                "esperaba CobroConfirmado de 70, fue {cobro:?}"
            );
        });
    }
}
