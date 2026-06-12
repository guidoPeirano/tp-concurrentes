//! Tests end-to-end: levantan los binarios reales (pasarela + estación + usuario)
//! como procesos separados y manejan al usuario por su REPL (comandos por stdin),
//! verificando la salida. Es lo más cercano a operar el sistema a mano.
//!
//! Requiere que los binarios estén compilados (corré los tests con `cargo test`
//! desde la raíz del workspace, que compila todo antes de ejecutar).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// Mata el proceso hijo al salir del scope (incluso si el test paniquea).
struct Proceso(Child);

impl Drop for Proceso {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Ruta a un binario del workspace (`target/debug/<nombre>`), derivada de la
/// ubicación del binario de test.
fn bin(nombre: &str) -> PathBuf {
    let mut dir = std::env::current_exe().expect("current_exe");
    dir.pop(); // saca el ejecutable de test
    if dir.ends_with("deps") {
        dir.pop();
    }
    let ruta = dir.join(nombre);
    assert!(
        ruta.exists(),
        "no encuentro el binario {nombre} en {ruta:?}; corré `cargo build` o `cargo test` desde la raíz"
    );
    ruta
}

/// Espera hasta que un puerto TCP esté escuchando (o paniquea tras ~5s).
fn esperar_puerto(puerto: u16) {
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", puerto)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("el puerto {puerto} no quedó escuchando a tiempo");
}

fn escribir_config(puerto_estacion: u16, puerto_pasarela: u16) -> PathBuf {
    let ruta = std::env::temp_dir().join(format!("tp_e2e_{}.json", std::process::id()));
    let contenido = format!(
        r#"{{
  "estaciones": [
    {{ "id": 1, "puerto": {puerto_estacion}, "ubicacion": [-34.6, -58.4] }}
  ],
  "pasarela": {{ "puerto": {puerto_pasarela} }},
  "tarifa": {{ "base": 50.0, "por_minuto": 10.0 }}
}}"#
    );
    std::fs::write(&ruta, contenido).expect("escribir config temporal");
    ruta
}

#[test]
fn flujo_completo_por_la_repl() {
    let puerto_estacion = 18911;
    let puerto_pasarela = 19011;
    let config = escribir_config(puerto_estacion, puerto_pasarela);
    let config = config.to_str().unwrap();

    // Levantamos el sistema: pasarela + estación.
    let _pasarela = Proceso(
        Command::new(bin("pasarela"))
            .args(["--puerto", "19011", "--config", config])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn pasarela"),
    );
    let _estacion = Proceso(
        Command::new(bin("estacion"))
            .args(["--id", "1", "--puerto", "18911", "--config", config])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn estacion"),
    );
    esperar_puerto(puerto_estacion);
    esperar_puerto(puerto_pasarela);

    // Manejamos al usuario por su REPL, igual que a mano.
    let mut usuario = Command::new(bin("usuario"))
        .args(["--id", "alice", "--config", config])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn usuario");

    let comandos = "\
        alquilar 1 0\n\
        estado\n\
        alquilar 1 2\n\
        devolver 1 1\n\
        devolver 1 0\n\
        estado\n\
        salir\n";
    usuario
        .stdin
        .take()
        .unwrap()
        .write_all(comandos.as_bytes())
        .expect("escribir comandos");

    let mut salida = String::new();
    usuario
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut salida)
        .expect("leer salida");
    usuario.wait().expect("esperar usuario");

    // El slot 0 tiene bici → alquiler OK, el usuario queda ConBici.
    assert!(salida.contains("AlquilerConfirmado"), "salida:\n{salida}");
    assert!(salida.contains("ConBici"), "salida:\n{salida}");
    // Con una bici en mano, un segundo alquiler lo bloquea el propio cliente.
    assert!(salida.contains("ya tenés"), "salida:\n{salida}");
    // Devolver al slot 1 (ocupado) se rechaza; al slot 0 (vacío) se acepta.
    assert!(salida.contains("DevolucionRechazada"), "salida:\n{salida}");
    assert!(salida.contains("DevolucionAceptada"), "salida:\n{salida}");
    // Tras devolver, vuelve a SinBici.
    assert!(
        salida.trim_end().ends_with("SinBici"),
        "el estado final debería ser SinBici. salida:\n{salida}"
    );
}
