//! Actor de red, compartido por las aplicaciones que hablan por socket
//! (estación y pasarela). Aísla la lógica de red del actor de negocio:
//!
//! - Escucha TCP y UDP en threads dedicados (`std::net`), desenmarca los frames
//!   TCP y reenvía cada payload recibido al actor de negocio vía un `Recipient`.
//! - Envía por TCP (con framing) y por UDP (datagrama suelto) a pedido.
//!
//! Es agnóstico del tipo de mensaje: trabaja con bytes. Quien recibe el
//! `PaqueteRecibido` lo deserializa al tipo que corresponda.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use actix::prelude::*;

use crate::framing::{enmarcar_payload, Desenmarcador};

/// Transporte por el que llegó un paquete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transporte {
    Tcp,
    Udp,
}

/// Permite responder por la misma conexión TCP por la que llegó un pedido
/// (patrón request-response). Para UDP y para mensajes sin respuesta es `None`.
pub struct Responder {
    tx: Sender<Vec<u8>>,
}

impl Responder {
    /// Envía la respuesta (bytes ya serializados) de vuelta por la conexión.
    pub fn responder(self, datos: Vec<u8>) {
        let _ = self.tx.send(datos);
    }

    /// Crea un `Responder` suelto junto con el extremo receptor. Útil para tests:
    /// permite simular un pedido con respuesta y leer lo que el actor contesta.
    pub fn canal() -> (Responder, std::sync::mpsc::Receiver<Vec<u8>>) {
        let (tx, rx) = mpsc::channel();
        (Responder { tx }, rx)
    }
}

/// Un payload (bytes ya desenmarcados) recibido de la red. El Comunicador se lo
/// reenvía al actor de negocio, que lo deserializa al tipo que espera. Si vino
/// por TCP, trae un `Responder` para contestar por la misma conexión.
#[derive(Message)]
#[rtype(result = "()")]
pub struct PaqueteRecibido {
    pub transporte: Transporte,
    pub datos: Vec<u8>,
    pub responder: Option<Responder>,
}

/// Pide enviar un payload por TCP (con framing) a un destino.
#[derive(Message)]
#[rtype(result = "()")]
pub struct EnviarTcp {
    pub destino: SocketAddr,
    pub datos: Vec<u8>,
}

/// Pide enviar un payload por UDP (un datagrama, sin framing) a un destino.
#[derive(Message)]
#[rtype(result = "()")]
pub struct EnviarUdp {
    pub destino: SocketAddr,
    pub datos: Vec<u8>,
}

/// Pide un request-response por TCP: conecta a `destino`, manda el payload
/// (enmarcado) y devuelve la respuesta, o `None` si no se pudo. Lo usa la estación
/// para hablarle a la pasarela en el 2PC, sin tocar sockets ella misma.
#[derive(Message)]
#[rtype(result = "Option<Vec<u8>>")]
pub struct ConsultarTcp {
    pub destino: SocketAddr,
    pub datos: Vec<u8>,
}

/// Como `EnviarTcp`, pero responde si la conexión y el envío salieron bien. Lo
/// usa el Ring de elección para cerrar el anillo ante caídas: si el siguiente
/// nodo no acepta la conexión, el que reenvía prueba con el que le sigue.
#[derive(Message)]
#[rtype(result = "bool")]
pub struct EnviarTcpConfirmado {
    pub destino: SocketAddr,
    pub datos: Vec<u8>,
}

/// ¿La última operación TCP hacia `destino` anduvo? Una dirección que nunca se
/// intentó se asume alcanzable. Lo usa la estación para decidir el modo
/// desconectado (Caso E) sin pagar un timeout en el momento del alquiler.
#[derive(Message)]
#[rtype(result = "bool")]
pub struct ConsultarAlcanzable {
    pub destino: SocketAddr,
}

/// Actualiza la marca de alcanzabilidad de una dirección. Lo usan los propios
/// handlers del Comunicador (tras cada operación saliente) y el re-sondeo.
#[derive(Message)]
#[rtype(result = "()")]
pub struct MarcarAlcanzable {
    pub destino: SocketAddr,
    pub alcanzable: bool,
}

/// Simula un corte de red (lo dispara la consola del proceso): mientras está
/// desconectado, todo lo que entra y sale por este Comunicador se descarta.
/// Al reconectar, el re-sondeo va desmarcando lo que volvió.
#[derive(Message)]
#[rtype(result = "()")]
pub struct SimularConectividad {
    pub conectado: bool,
}

/// Cada cuánto el Comunicador re-sondea (conexión corta) las direcciones que
/// tiene marcadas como inalcanzables, para detectar que volvieron.
const INTERVALO_RESONDEO: Duration = Duration::from_secs(3);

pub struct Comunicador {
    addr_tcp: SocketAddr,
    addr_udp: SocketAddr,
    destino: Recipient<PaqueteRecibido>,
    /// Socket UDP para enviar (clon del que usa el thread de recepción). Se
    /// inicializa en `started`.
    socket_udp: Option<UdpSocket>,
    /// Servicios alcanzables, por dirección: el resultado de la última
    /// operación TCP saliente hacia cada destino.
    alcanzables: HashMap<SocketAddr, bool>,
    /// Corte de red simulado (compartido con los threads de escucha).
    desconectado: Arc<AtomicBool>,
}

impl Comunicador {
    pub fn new(
        addr_tcp: SocketAddr,
        addr_udp: SocketAddr,
        destino: Recipient<PaqueteRecibido>,
    ) -> Self {
        Self {
            addr_tcp,
            addr_udp,
            destino,
            socket_udp: None,
            alcanzables: HashMap::new(),
            desconectado: Arc::new(AtomicBool::new(false)),
        }
    }

    fn esta_desconectado(&self) -> bool {
        self.desconectado.load(Ordering::Relaxed)
    }

    /// Re-sondea (conexión corta, en un thread) las direcciones marcadas como
    /// inalcanzables; si alguna volvió, se desmarca. Así el Caso E termina solo
    /// cuando la pasarela reaparece.
    fn resondear(&self, ctx: &mut Context<Self>) {
        // Con el corte simulado activo no hay nada que re-sondear.
        if self.esta_desconectado() {
            return;
        }
        let caidas: Vec<SocketAddr> = self
            .alcanzables
            .iter()
            .filter(|(_, ok)| !**ok)
            .map(|(addr, _)| *addr)
            .collect();
        if caidas.is_empty() {
            return;
        }
        let yo = ctx.address();
        thread::spawn(move || {
            for destino in caidas {
                if TcpStream::connect_timeout(&destino, Duration::from_millis(500)).is_ok() {
                    yo.do_send(MarcarAlcanzable {
                        destino,
                        alcanzable: true,
                    });
                }
            }
        });
    }
}

impl Actor for Comunicador {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        escuchar_tcp(
            self.addr_tcp,
            self.destino.clone(),
            Arc::clone(&self.desconectado),
        );
        self.socket_udp = Some(escuchar_udp(
            self.addr_udp,
            self.destino.clone(),
            Arc::clone(&self.desconectado),
        ));
        ctx.run_interval(INTERVALO_RESONDEO, |act, ctx| act.resondear(ctx));
    }
}

impl Handler<EnviarTcp> for Comunicador {
    type Result = ();

    fn handle(&mut self, msg: EnviarTcp, _ctx: &mut Self::Context) {
        if self.esta_desconectado() {
            return; // corte simulado: el envío se pierde
        }
        // El envío es bloqueante; lo hacemos en un thread para no frenar al actor.
        thread::spawn(move || enviar_tcp(msg.destino, &msg.datos));
    }
}

impl Handler<EnviarUdp> for Comunicador {
    type Result = ();

    fn handle(&mut self, msg: EnviarUdp, _ctx: &mut Self::Context) {
        if self.esta_desconectado() {
            return; // corte simulado: el datagrama se pierde
        }
        if let Some(socket) = &self.socket_udp {
            let _ = socket.send_to(&msg.datos, msg.destino);
        }
    }
}

impl Handler<ConsultarTcp> for Comunicador {
    type Result = ResponseFuture<Option<Vec<u8>>>;

    fn handle(&mut self, msg: ConsultarTcp, ctx: &mut Self::Context) -> Self::Result {
        // El round-trip bloqueante se hace en un thread y acá solo se espera el
        // resultado, sin frenar al actor. Importa de verdad: si este handler
        // bloqueara el arbiter, dos procesos que se consultan mutuamente (p.ej.
        // el líder reconstruyendo el registro mientras un follower lo sondea)
        // quedarían esperándose para siempre.
        if self.esta_desconectado() {
            // Corte simulado: la consulta falla y el destino queda marcado.
            self.alcanzables.insert(msg.destino, false);
            return Box::pin(async { None });
        }
        let yo = ctx.address();
        Box::pin(async move {
            let destino = msg.destino;
            let resultado = en_thread(move || solicitar_tcp(msg.destino, &msg.datos).ok())
                .await
                .flatten();
            yo.do_send(MarcarAlcanzable {
                destino,
                alcanzable: resultado.is_some(),
            });
            resultado
        })
    }
}

impl Handler<EnviarTcpConfirmado> for Comunicador {
    type Result = ResponseFuture<bool>;

    fn handle(&mut self, msg: EnviarTcpConfirmado, ctx: &mut Self::Context) -> Self::Result {
        // Mismo esquema que `ConsultarTcp`: el envío (con sus reintentos de
        // conexión) corre en un thread para no frenar al actor.
        if self.esta_desconectado() {
            self.alcanzables.insert(msg.destino, false);
            return Box::pin(async { false });
        }
        let yo = ctx.address();
        Box::pin(async move {
            let destino = msg.destino;
            let entregado = en_thread(move || enviar_tcp_confirmado(msg.destino, &msg.datos))
                .await
                .unwrap_or(false);
            yo.do_send(MarcarAlcanzable {
                destino,
                alcanzable: entregado,
            });
            entregado
        })
    }
}

impl Handler<SimularConectividad> for Comunicador {
    type Result = ();

    fn handle(&mut self, msg: SimularConectividad, _ctx: &mut Self::Context) {
        self.desconectado.store(!msg.conectado, Ordering::Relaxed);
        if msg.conectado {
            println!("[comunicador] red RESTABLECIDA (simulación)");
        } else {
            println!(
                "[comunicador] red CORTADA (simulación): se descarta todo lo que entra y sale"
            );
        }
    }
}

impl Handler<ConsultarAlcanzable> for Comunicador {
    type Result = bool;

    fn handle(&mut self, msg: ConsultarAlcanzable, _ctx: &mut Self::Context) -> bool {
        *self.alcanzables.get(&msg.destino).unwrap_or(&true)
    }
}

impl Handler<MarcarAlcanzable> for Comunicador {
    type Result = ();

    fn handle(&mut self, msg: MarcarAlcanzable, _ctx: &mut Self::Context) {
        let anterior = self.alcanzables.insert(msg.destino, msg.alcanzable);
        if anterior == Some(!msg.alcanzable) {
            println!(
                "[comunicador] {} pasó a {}",
                msg.destino,
                if msg.alcanzable {
                    "alcanzable"
                } else {
                    "INALCANZABLE"
                }
            );
        }
    }
}

/// Corre `trabajo` (bloqueante) en un thread y devuelve un futuro await-able con
/// su resultado, sin bloquear el thread del actor. El puente es un canal de la
/// std consultado con `try_recv` + sleeps cortos (sin crates async extra).
/// Devuelve `None` si el thread murió sin responder.
fn en_thread<T, F>(trabajo: F) -> impl std::future::Future<Output = Option<T>>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(trabajo());
    });
    async move {
        loop {
            match rx.try_recv() {
                Ok(resultado) => return Some(resultado),
                Err(mpsc::TryRecvError::Disconnected) => return None,
                Err(mpsc::TryRecvError::Empty) => {
                    actix::clock::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }
}

/// Levanta el listener TCP en un thread; por cada conexión, otro thread lee y
/// desenmarca, reenviando cada payload al destino.
fn escuchar_tcp(addr: SocketAddr, destino: Recipient<PaqueteRecibido>, corte: Arc<AtomicBool>) {
    let listener =
        TcpListener::bind(addr).unwrap_or_else(|e| panic!("no pude bindear TCP en {addr}: {e}"));
    thread::spawn(move || {
        for conexion in listener.incoming() {
            match conexion {
                Ok(stream) => {
                    let destino = destino.clone();
                    let corte = Arc::clone(&corte);
                    thread::spawn(move || leer_stream_tcp(stream, destino, corte));
                }
                Err(_) => continue,
            }
        }
    });
}

fn leer_stream_tcp(
    mut stream: TcpStream,
    destino: Recipient<PaqueteRecibido>,
    corte: Arc<AtomicBool>,
) {
    let mut desenmarcador = Desenmarcador::new();
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break, // el otro lado cerró
            Ok(n) => {
                desenmarcador.alimentar(&buf[..n]);
                while let Some(payload) = desenmarcador.siguiente_payload() {
                    // Corte simulado: lo que llega se descarta sin responder
                    // (para el que mandó, es igual que un proceso colgado).
                    if corte.load(Ordering::Relaxed) {
                        continue;
                    }
                    // Por cada pedido, le damos al actor un canal para responder.
                    let (tx, rx) = mpsc::channel();
                    destino.do_send(PaqueteRecibido {
                        transporte: Transporte::Tcp,
                        datos: payload,
                        responder: Some(Responder { tx }),
                    });
                    // Esperamos la respuesta y la devolvemos por la misma conexión.
                    // Si el actor no responde (mensaje fire-and-forget), suelta el
                    // `Responder` y `recv` corta sin escribir nada.
                    if let Ok(respuesta) = rx.recv() {
                        if let Ok(frame) = enmarcar_payload(&respuesta) {
                            let _ = stream.write_all(&frame);
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }
}

/// Bindea el socket UDP, deja un thread escuchando datagramas y devuelve un clon
/// del socket para poder enviar.
fn escuchar_udp(
    addr: SocketAddr,
    destino: Recipient<PaqueteRecibido>,
    corte: Arc<AtomicBool>,
) -> UdpSocket {
    let socket =
        UdpSocket::bind(addr).unwrap_or_else(|e| panic!("no pude bindear UDP en {addr}: {e}"));
    let socket_recv = socket.try_clone().expect("clonar socket UDP");
    thread::spawn(move || {
        let mut buf = [0u8; 65_535];
        while let Ok((n, _)) = socket_recv.recv_from(&mut buf) {
            if corte.load(Ordering::Relaxed) {
                continue; // corte simulado: el datagrama se descarta
            }
            destino.do_send(PaqueteRecibido {
                transporte: Transporte::Udp,
                datos: buf[..n].to_vec(),
                responder: None,
            });
        }
    });
    socket
}

/// Conecta y envía un payload enmarcado por TCP. Reintenta unos instantes para
/// tolerar que el destino todavía no haya terminado de levantar su listener.
fn enviar_tcp(destino: SocketAddr, datos: &[u8]) {
    let frame = match enmarcar_payload(datos) {
        Ok(f) => f,
        Err(_) => return,
    };
    for _ in 0..10 {
        if let Ok(mut stream) = TcpStream::connect(destino) {
            let _ = stream.write_all(&frame);
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    eprintln!("[comunicador] no pude conectar a {destino} para enviar por TCP");
}

/// Conecta y envía un payload enmarcado, informando si se pudo entregar. Menos
/// reintentos que `enviar_tcp`: acá el que llama quiere un veredicto rápido para
/// probar con otro destino (el par de reintentos tolera un listener que recién
/// está levantando).
fn enviar_tcp_confirmado(destino: SocketAddr, datos: &[u8]) -> bool {
    let Ok(frame) = enmarcar_payload(datos) else {
        return false;
    };
    for intento in 0..3 {
        if intento > 0 {
            thread::sleep(Duration::from_millis(50));
        }
        if let Ok(mut stream) = TcpStream::connect(destino) {
            return stream.write_all(&frame).is_ok();
        }
    }
    false
}

/// Cuánto espera `solicitar_tcp` la respuesta antes de dar la consulta por
/// fallida. Sin esto, un proceso colgado (vivo pero que no responde) dejaría la
/// consulta esperando para siempre; con esto, el que consulta recibe `None` y
/// reacciona (p.ej. la vigilancia del líder lo da por caído).
const TIMEOUT_LECTURA_TCP: Duration = Duration::from_secs(5);

/// Cliente TCP request-response: conecta, manda un payload enmarcado y devuelve
/// la respuesta (también enmarcada) por la misma conexión. Es **bloqueante**
/// (corre en un thread, ver `en_thread`) pero acotado: la lectura vence a los
/// `TIMEOUT_LECTURA_TCP`. Interno del Comunicador: lo usa `ConsultarTcp`.
fn solicitar_tcp(destino: SocketAddr, datos: &[u8]) -> std::io::Result<Vec<u8>> {
    let frame = enmarcar_payload(datos)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let mut stream = TcpStream::connect(destino)?;
    stream.set_read_timeout(Some(TIMEOUT_LECTURA_TCP))?;
    stream.write_all(&frame)?;

    let mut desenmarcador = Desenmarcador::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "la conexión se cerró sin respuesta",
            ));
        }
        desenmarcador.alimentar(&buf[..n]);
        if let Some(payload) = desenmarcador.siguiente_payload() {
            return Ok(payload);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{self, Sender};

    /// Actor de prueba que vuelca a un canal todo lo que recibe.
    struct Sonda {
        tx: Sender<(Transporte, Vec<u8>)>,
    }

    impl Actor for Sonda {
        type Context = Context<Self>;
    }

    impl Handler<PaqueteRecibido> for Sonda {
        type Result = ();

        fn handle(&mut self, msg: PaqueteRecibido, _ctx: &mut Self::Context) {
            let _ = self.tx.send((msg.transporte, msg.datos));
        }
    }

    #[test]
    fn marca_inalcanzable_tras_fallar_y_se_recupera_con_el_resondeo() {
        System::new().block_on(async {
            let (tx, _rx) = mpsc::channel();
            let comunicador = Comunicador::new(
                "127.0.0.1:0".parse().unwrap(),
                "127.0.0.1:0".parse().unwrap(),
                Sonda { tx }.start().recipient(),
            )
            .start();
            let muerto: SocketAddr = "127.0.0.1:18900".parse().unwrap();

            // Una dirección que nunca se intentó se asume alcanzable.
            assert!(comunicador
                .send(ConsultarAlcanzable { destino: muerto })
                .await
                .unwrap());

            // Un envío fallido la marca inalcanzable.
            let entregado = comunicador
                .send(EnviarTcpConfirmado {
                    destino: muerto,
                    datos: b"hola".to_vec(),
                })
                .await
                .unwrap();
            assert!(!entregado);
            actix::clock::sleep(Duration::from_millis(200)).await;
            assert!(
                !comunicador
                    .send(ConsultarAlcanzable { destino: muerto })
                    .await
                    .unwrap(),
                "tras el fallo queda marcada inalcanzable"
            );

            // El servicio "vuelve": el re-sondeo periódico la desmarca solo.
            let _listener = TcpListener::bind(muerto).unwrap();
            let mut alcanzable = false;
            for _ in 0..30 {
                actix::clock::sleep(Duration::from_millis(300)).await;
                alcanzable = comunicador
                    .send(ConsultarAlcanzable { destino: muerto })
                    .await
                    .unwrap();
                if alcanzable {
                    break;
                }
            }
            assert!(alcanzable, "el re-sondeo debería detectar que volvió");
        });
    }

    #[test]
    fn el_corte_simulado_descarta_lo_que_entra_y_lo_que_sale() {
        System::new().block_on(async {
            let (tx_a, rx_a) = mpsc::channel::<(Transporte, Vec<u8>)>();
            let (tx_b, _rx_b) = mpsc::channel::<(Transporte, Vec<u8>)>();
            let addr_a: SocketAddr = "127.0.0.1:18902".parse().unwrap();
            let addr_b: SocketAddr = "127.0.0.1:18903".parse().unwrap();
            let a =
                Comunicador::new(addr_a, addr_a, Sonda { tx: tx_a }.start().recipient()).start();
            let b =
                Comunicador::new(addr_b, addr_b, Sonda { tx: tx_b }.start().recipient()).start();
            actix::clock::sleep(Duration::from_millis(300)).await;

            // A se queda sin red: lo que intenta mandar falla al instante...
            a.send(SimularConectividad { conectado: false })
                .await
                .unwrap();
            let entregado = a
                .send(EnviarTcpConfirmado {
                    destino: addr_b,
                    datos: b"no-deberia-salir".to_vec(),
                })
                .await
                .unwrap();
            assert!(!entregado, "sin red no se entrega nada");

            // ...y lo que le mandan, se descarta (B sí tiene red).
            let entregado = b
                .send(EnviarTcpConfirmado {
                    destino: addr_a,
                    datos: b"a-un-cortado".to_vec(),
                })
                .await
                .unwrap();
            assert!(entregado, "el kernel de A acepta los bytes igual");
            actix::clock::sleep(Duration::from_millis(300)).await;
            assert!(
                rx_a.try_recv().is_err(),
                "el payload no debe llegar al actor de A durante el corte"
            );

            // Vuelve la red: el tráfico fluye de nuevo.
            a.send(SimularConectividad { conectado: true })
                .await
                .unwrap();
            let entregado = b
                .send(EnviarTcpConfirmado {
                    destino: addr_a,
                    datos: b"ahora-si".to_vec(),
                })
                .await
                .unwrap();
            assert!(entregado);
            let mut recibido = None;
            for _ in 0..20 {
                if let Ok(p) = rx_a.try_recv() {
                    recibido = Some(p);
                    break;
                }
                actix::clock::sleep(Duration::from_millis(100)).await;
            }
            let (transporte, datos) = recibido.expect("con la red de vuelta, el payload llega");
            assert_eq!(transporte, Transporte::Tcp);
            assert_eq!(datos, b"ahora-si");
        });
    }

    #[test]
    fn intercambio_tcp_y_udp_entre_dos_comunicadores() {
        let (tx, rx) = mpsc::channel::<(Transporte, Vec<u8>)>();

        let b: SocketAddr = "127.0.0.1:18841".parse().unwrap();
        let a: SocketAddr = "127.0.0.1:18843".parse().unwrap();

        // Corremos el sistema de actores en un thread aparte para que el thread
        // del test quede libre para esperar en el canal.
        thread::spawn(move || {
            let system = System::new();
            system.block_on(async move {
                let sonda = Sonda { tx }.start();
                let _comun_b = Comunicador::new(b, b, sonda.clone().recipient()).start();
                let comun_a = Comunicador::new(a, a, sonda.recipient()).start();

                // Esperamos (sin bloquear el arbiter) a que ambos bindeen.
                actix::clock::sleep(Duration::from_millis(300)).await;

                comun_a.do_send(EnviarTcp {
                    destino: b,
                    datos: b"hola-por-tcp".to_vec(),
                });
                comun_a.do_send(EnviarUdp {
                    destino: b,
                    datos: b"hola-por-udp".to_vec(),
                });
            });
            let _ = system.run();
        });

        let mut recibidos = Vec::new();
        for _ in 0..2 {
            let paquete = rx
                .recv_timeout(Duration::from_secs(5))
                .expect("debería llegar un paquete");
            recibidos.push(paquete);
        }

        assert!(
            recibidos
                .iter()
                .any(|(t, d)| *t == Transporte::Tcp && d == b"hola-por-tcp"),
            "B debería recibir el mensaje por TCP idéntico"
        );
        assert!(
            recibidos
                .iter()
                .any(|(t, d)| *t == Transporte::Udp && d == b"hola-por-udp"),
            "B debería recibir el datagrama UDP"
        );
    }
}
