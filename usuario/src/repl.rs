//! REPL del usuario: lee comandos por consola y los traduce en pedidos a las
//! estaciones por TCP (request-response, con framing). Actualiza el estado del
//! usuario con cada respuesta.

use std::error::Error;
use std::io::{BufRead, Read, Write};
use std::net::{SocketAddr, TcpStream};

use comun::framing::{enmarcar, Desenmarcador};
use comun::mensajes::usuario_estacion::{
    MensajeEstacionAUsuario, MensajeUsuario, MensajeUsuarioAEstacion,
};
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
        "comandos: alquilar <estacion> <slot> | devolver <estacion> <slot> | estado | ayuda | salir"
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

fn direccion(config: &Config, est_id: u32) -> Option<SocketAddr> {
    let est = config.estacion(EstacionId(est_id))?;
    Some(SocketAddr::from(([127, 0, 0, 1], est.puerto)))
}

/// Cliente TCP request-response: conecta, manda el pedido enmarcado y lee una
/// respuesta enmarcada por la misma conexión.
fn enviar(
    addr: SocketAddr,
    pedido: &MensajeUsuario,
) -> Result<MensajeEstacionAUsuario, Box<dyn Error>> {
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
        if let Some(respuesta) = desenmarcador.siguiente::<MensajeEstacionAUsuario>()? {
            return Ok(respuesta);
        }
    }
}
