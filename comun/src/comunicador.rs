//! Actor de red, compartido por las aplicaciones que hablan por socket
//! (estación y pasarela). Aísla la lógica de red del actor de negocio:
//!
//! - Escucha TCP y UDP en threads dedicados (`std::net`), desenmarca los frames
//!   TCP y reenvía cada payload recibido al actor de negocio vía un `Recipient`.
//! - Envía por TCP (con framing) y por UDP (datagrama suelto) a pedido.
//!
//! Es agnóstico del tipo de mensaje: trabaja con bytes. Quien recibe el
//! `PaqueteRecibido` lo deserializa al tipo que corresponda.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::mpsc::{self, Sender};
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

pub struct Comunicador {
    addr_tcp: SocketAddr,
    addr_udp: SocketAddr,
    destino: Recipient<PaqueteRecibido>,
    /// Socket UDP para enviar (clon del que usa el thread de recepción). Se
    /// inicializa en `started`.
    socket_udp: Option<UdpSocket>,
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
        }
    }
}

impl Actor for Comunicador {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        escuchar_tcp(self.addr_tcp, self.destino.clone());
        self.socket_udp = Some(escuchar_udp(self.addr_udp, self.destino.clone()));
    }
}

impl Handler<EnviarTcp> for Comunicador {
    type Result = ();

    fn handle(&mut self, msg: EnviarTcp, _ctx: &mut Self::Context) {
        // El envío es bloqueante; lo hacemos en un thread para no frenar al actor.
        thread::spawn(move || enviar_tcp(msg.destino, &msg.datos));
    }
}

impl Handler<EnviarUdp> for Comunicador {
    type Result = ();

    fn handle(&mut self, msg: EnviarUdp, _ctx: &mut Self::Context) {
        if let Some(socket) = &self.socket_udp {
            let _ = socket.send_to(&msg.datos, msg.destino);
        }
    }
}

impl Handler<ConsultarTcp> for Comunicador {
    type Result = MessageResult<ConsultarTcp>;

    fn handle(&mut self, msg: ConsultarTcp, _ctx: &mut Self::Context) -> Self::Result {
        // Round-trip bloqueante; el destino es otro proceso (no hay deadlock).
        MessageResult(solicitar_tcp(msg.destino, &msg.datos).ok())
    }
}

/// Levanta el listener TCP en un thread; por cada conexión, otro thread lee y
/// desenmarca, reenviando cada payload al destino.
fn escuchar_tcp(addr: SocketAddr, destino: Recipient<PaqueteRecibido>) {
    let listener =
        TcpListener::bind(addr).unwrap_or_else(|e| panic!("no pude bindear TCP en {addr}: {e}"));
    thread::spawn(move || {
        for conexion in listener.incoming() {
            match conexion {
                Ok(stream) => {
                    let destino = destino.clone();
                    thread::spawn(move || leer_stream_tcp(stream, destino));
                }
                Err(_) => continue,
            }
        }
    });
}

fn leer_stream_tcp(mut stream: TcpStream, destino: Recipient<PaqueteRecibido>) {
    let mut desenmarcador = Desenmarcador::new();
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break, // el otro lado cerró
            Ok(n) => {
                desenmarcador.alimentar(&buf[..n]);
                while let Some(payload) = desenmarcador.siguiente_payload() {
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
fn escuchar_udp(addr: SocketAddr, destino: Recipient<PaqueteRecibido>) -> UdpSocket {
    let socket =
        UdpSocket::bind(addr).unwrap_or_else(|e| panic!("no pude bindear UDP en {addr}: {e}"));
    let socket_recv = socket.try_clone().expect("clonar socket UDP");
    thread::spawn(move || {
        let mut buf = [0u8; 65_535];
        while let Ok((n, _)) = socket_recv.recv_from(&mut buf) {
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

/// Cliente TCP request-response: conecta, manda un payload enmarcado y devuelve
/// la respuesta (también enmarcada) por la misma conexión. Es **bloqueante**.
/// Interno del Comunicador: lo usa el handler de `ConsultarTcp`.
fn solicitar_tcp(destino: SocketAddr, datos: &[u8]) -> std::io::Result<Vec<u8>> {
    let frame = enmarcar_payload(datos)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let mut stream = TcpStream::connect(destino)?;
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
