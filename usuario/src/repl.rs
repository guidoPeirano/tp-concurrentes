//! REPL del usuario: lee comandos por consola y los traduce en pedidos a las
//! estaciones por TCP (request-response, con framing). Actualiza el estado del
//! usuario con cada respuesta.

use std::error::Error;
use std::io::{BufRead, Read, Write};
use std::net::{SocketAddr, TcpStream};

use comun::framing::{enmarcar, Desenmarcador};
use comun::mensajes::usuario_estacion::{
    MensajeEstacionAUsuario, MensajeEstacionAUsuarioConsulta, MensajeUsuario,
    MensajeUsuarioAEstacion, MensajeUsuarioAEstacionConsulta,
};
use comun::serializacion::desde_bytes;
use comun::{Config, EstacionId};

use crate::usuario::{EstadoUsuario, Usuario};

pub fn correr(mut usuario: Usuario, config: Config) {
    ayuda();
    let stdin = std::io::stdin();
    for linea in stdin.lock().lines() {
        let Ok(linea) = linea else { break };
        let partes: Vec<&str> = linea.split_whitespace().collect();
        match partes.as_slice() {
            ["alquilar", est, slot] => alquilar(&mut usuario, &config, est, slot),
            ["devolver", est, slot] => devolver(&mut usuario, &config, est, slot),
            ["consultar", lat, lon, radio] => consultar(&usuario, &config, lat, lon, radio),
            ["estado"] => println!("estado: {:?}", usuario.estado()),
            ["ayuda"] => ayuda(),
            ["salir"] | ["exit"] => break,
            [] => {}
            _ => println!("comando desconocido (probá 'ayuda')"),
        }
    }
}

fn ayuda() {
    println!(
        "comandos: alquilar <estacion> <slot> | devolver <estacion> <slot> | \
         consultar <lat> <lon> <radio_km> | estado | ayuda | salir"
    );
}

fn alquilar(usuario: &mut Usuario, config: &Config, est: &str, slot: &str) {
    // Una bici por usuario: si ya tiene una, no puede alquilar otra.
    if let EstadoUsuario::ConBici { bici_id, .. } = usuario.estado() {
        println!("ya tenés la bici {bici_id:?}; devolvela antes de alquilar otra");
        return;
    }
    let (Ok(est_id), Ok(slot_id)) = (est.parse::<u32>(), slot.parse::<u32>()) else {
        println!("uso: alquilar <estacion> <slot> (números)");
        return;
    };
    let Some(addr) = direccion(config, est_id) else {
        println!("no conozco la estación {est_id}");
        return;
    };
    let pedido = MensajeUsuario::Operacion(MensajeUsuarioAEstacion::SolicitudAlquiler {
        usuario_id: usuario.id().clone(),
        slot_id,
        tarjeta: usuario.tarjeta().clone(),
    });
    responder(usuario, addr, &pedido);
}

fn devolver(usuario: &mut Usuario, config: &Config, est: &str, slot: &str) {
    let (Ok(est_id), Ok(slot_id)) = (est.parse::<u32>(), slot.parse::<u32>()) else {
        println!("uso: devolver <estacion> <slot> (números)");
        return;
    };
    let (bici_id, rental_id) = match usuario.estado() {
        EstadoUsuario::ConBici { bici_id, rental_id } => (*bici_id, rental_id.clone()),
        EstadoUsuario::SinBici => {
            println!("no tenés ninguna bici para devolver");
            return;
        }
    };
    let Some(addr) = direccion(config, est_id) else {
        println!("no conozco la estación {est_id}");
        return;
    };
    let pedido = MensajeUsuario::Operacion(MensajeUsuarioAEstacion::SolicitudDevolucion {
        usuario_id: usuario.id().clone(),
        bici_id,
        rental_id,
        slot_id,
    });
    responder(usuario, addr, &pedido);
}

/// Manda el pedido, aplica la respuesta al estado y la imprime.
fn responder(usuario: &mut Usuario, addr: SocketAddr, pedido: &MensajeUsuario) {
    match enviar(addr, pedido) {
        Ok(respuesta) => {
            usuario.aplicar(&respuesta);
            println!("respuesta: {respuesta:?}");
        }
        Err(e) => println!("error hablando con la estación: {e}"),
    }
}

/// CU3: el usuario pregunta por estaciones con bici cerca de un punto. Primero
/// hace discovery del líder (le pregunta a cualquier estación conocida) y después
/// le manda la consulta de disponibilidad al líder.
fn consultar(usuario: &Usuario, config: &Config, lat: &str, lon: &str, radio: &str) {
    let (Ok(lat), Ok(lon), Ok(radio)) =
        (lat.parse::<f64>(), lon.parse::<f64>(), radio.parse::<f64>())
    else {
        println!("uso: consultar <lat> <lon> <radio_km> (números)");
        return;
    };
    // Discovery: le preguntamos al líder a la primera estación de la config.
    let Some(alguna) = config.estaciones.first() else {
        println!("no hay estaciones en la config");
        return;
    };
    let alguna_addr = SocketAddr::from(([127, 0, 0, 1], alguna.puerto));
    let lider_addr = match enviar_consulta(
        alguna_addr,
        &MensajeUsuario::Consulta(MensajeUsuarioAEstacionConsulta::PreguntarLider),
    ) {
        Ok(MensajeEstacionAUsuarioConsulta::RespuestaLider { lider_addr, .. }) => lider_addr,
        Ok(MensajeEstacionAUsuarioConsulta::EnEleccion) => {
            println!("hay una elección de líder en curso, probá de nuevo en unos segundos");
            return;
        }
        Ok(MensajeEstacionAUsuarioConsulta::LiderDesconocido) => {
            println!("esa estación todavía no conoce al líder");
            return;
        }
        Ok(otra) => {
            println!("respuesta inesperada al discovery: {otra:?}");
            return;
        }
        Err(e) => {
            println!("error buscando al líder: {e}");
            return;
        }
    };
    // Consulta de disponibilidad al líder.
    let consulta =
        MensajeUsuario::Consulta(MensajeUsuarioAEstacionConsulta::ConsultaDisponibilidad {
            usuario_id: usuario.id().clone(),
            ubicacion: (lat, lon),
            radio_max_km: radio,
        });
    match enviar_consulta(lider_addr, &consulta) {
        Ok(MensajeEstacionAUsuarioConsulta::RespuestaDisponibilidad { estaciones }) => {
            if estaciones.is_empty() {
                println!("no hay estaciones con bici dentro de {radio} km");
            } else {
                println!("estaciones con bici cerca:");
                for info in estaciones {
                    println!(
                        "  estación {} en {:?}: {} bicis, {} slots libres",
                        info.estacion_id.0,
                        info.ubicacion,
                        info.bicis_disponibles,
                        info.slots_libres
                    );
                }
            }
        }
        Ok(otra) => println!("respuesta inesperada a la consulta: {otra:?}"),
        Err(e) => println!("error consultando disponibilidad: {e}"),
    }
}

fn direccion(config: &Config, est_id: u32) -> Option<SocketAddr> {
    let est = config.estacion(EstacionId(est_id))?;
    Some(SocketAddr::from(([127, 0, 0, 1], est.puerto)))
}

/// Manda una operación y espera la respuesta de operación.
fn enviar(
    addr: SocketAddr,
    pedido: &MensajeUsuario,
) -> Result<MensajeEstacionAUsuario, Box<dyn Error>> {
    let payload = intercambiar(addr, pedido)?;
    Ok(desde_bytes(&payload)?)
}

/// Manda una consulta y espera la respuesta de consulta.
fn enviar_consulta(
    addr: SocketAddr,
    pedido: &MensajeUsuario,
) -> Result<MensajeEstacionAUsuarioConsulta, Box<dyn Error>> {
    let payload = intercambiar(addr, pedido)?;
    Ok(desde_bytes(&payload)?)
}

/// Cliente TCP request-response: conecta, manda el pedido enmarcado y devuelve el
/// payload (sin deserializar) de la respuesta que llega por la misma conexión.
fn intercambiar(addr: SocketAddr, pedido: &MensajeUsuario) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut stream = TcpStream::connect(addr)?;
    stream.write_all(&enmarcar(pedido)?)?;

    let mut desenmarcador = Desenmarcador::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            return Err("la estación cerró la conexión sin responder".into());
        }
        desenmarcador.alimentar(&buf[..n]);
        if let Some(payload) = desenmarcador.siguiente_payload() {
            return Ok(payload);
        }
    }
}
