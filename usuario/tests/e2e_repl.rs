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
  "tarifa": {{ "base": 50.0, "por_minuto": 10.0 }},
  "lider": 1
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
    // La pre-autorización la dio la pasarela (id "P-...", no el "local" provisorio):
    // confirma que el alquiler hizo el 2PC contra la pasarela de verdad.
    assert!(
        salida.contains("preauth_id: Some(\"P-"),
        "salida:\n{salida}"
    );
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

/// Config de dos estaciones (1 = líder, 2 = follower) + pasarela, en puertos
/// propios de este test.
fn escribir_config_multi() -> PathBuf {
    let ruta = std::env::temp_dir().join(format!("tp_e2e_multi_{}.json", std::process::id()));
    let contenido = r#"{
  "estaciones": [
    { "id": 1, "puerto": 18921, "ubicacion": [-34.60, -58.40] },
    { "id": 2, "puerto": 18922, "ubicacion": [-34.61, -58.41] }
  ],
  "pasarela": { "puerto": 19021 },
  "tarifa": { "base": 50.0, "por_minuto": 10.0 },
  "lider": 1
}"#;
    std::fs::write(&ruta, contenido).expect("escribir config temporal");
    ruta
}

#[test]
fn alquilar_en_una_estacion_y_devolver_en_otra() {
    let config = escribir_config_multi();
    let config = config.to_str().unwrap();

    // Líder + origen (estación 1), destino (estación 2), pasarela.
    let _pasarela = Proceso(
        Command::new(bin("pasarela"))
            .args(["--puerto", "19021", "--config", config])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn pasarela"),
    );
    let _estacion1 = Proceso(
        Command::new(bin("estacion"))
            .args(["--id", "1", "--puerto", "18921", "--config", config])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn estacion 1"),
    );
    let _estacion2 = Proceso(
        Command::new(bin("estacion"))
            .args(["--id", "2", "--puerto", "18922", "--config", config])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn estacion 2"),
    );
    esperar_puerto(19021);
    esperar_puerto(18921);
    esperar_puerto(18922);

    // Alquila en la estación 1 (slot 0, con bici) y devuelve en la estación 2
    // (slot 5, vacío). La estación 2 consulta al líder (estación 1), cobra y cierra.
    let mut usuario = Command::new(bin("usuario"))
        .args(["--id", "bob", "--config", config])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn usuario");
    let comandos = "\
        alquilar 1 0\n\
        estado\n\
        devolver 2 5\n\
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

    assert!(salida.contains("AlquilerConfirmado"), "salida:\n{salida}");
    assert!(salida.contains("ConBici"), "salida:\n{salida}");
    assert!(salida.contains("DevolucionAceptada"), "salida:\n{salida}");
    assert!(
        salida.trim_end().ends_with("SinBici"),
        "el estado final debería ser SinBici. salida:\n{salida}"
    );
}

/// Config de dos estaciones (1 = líder) + pasarela, en puertos propios para el
/// test de consulta de disponibilidad (CU3).
fn escribir_config_consulta() -> PathBuf {
    let ruta = std::env::temp_dir().join(format!("tp_e2e_consulta_{}.json", std::process::id()));
    let contenido = r#"{
  "estaciones": [
    { "id": 1, "puerto": 18931, "ubicacion": [-34.60, -58.40] },
    { "id": 2, "puerto": 18932, "ubicacion": [-34.61, -58.41] }
  ],
  "pasarela": { "puerto": 19031 },
  "tarifa": { "base": 50.0, "por_minuto": 10.0 },
  "lider": 1
}"#;
    std::fs::write(&ruta, contenido).expect("escribir config temporal");
    ruta
}

#[test]
fn consulta_de_disponibilidad_por_la_repl() {
    let config = escribir_config_consulta();
    let config = config.to_str().unwrap();

    let _pasarela = Proceso(
        Command::new(bin("pasarela"))
            .args(["--puerto", "19031", "--config", config])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn pasarela"),
    );
    let _estacion1 = Proceso(
        Command::new(bin("estacion"))
            .args(["--id", "1", "--puerto", "18931", "--config", config])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn estacion 1"),
    );
    let _estacion2 = Proceso(
        Command::new(bin("estacion"))
            .args(["--id", "2", "--puerto", "18932", "--config", config])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn estacion 2"),
    );
    esperar_puerto(19031);
    esperar_puerto(18931);
    esperar_puerto(18932);

    // Las dos estaciones gossipean su estado al líder cada 3s; esperamos una ronda
    // para que la cache del líder se pueble antes de consultar.
    std::thread::sleep(Duration::from_millis(4000));

    // Consulta cerca de ambas estaciones (radio 5 km): las dos tienen bicis.
    let mut usuario = Command::new(bin("usuario"))
        .args(["--id", "carol", "--config", config])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn usuario");
    let comandos = "\
        consultar -34.60 -58.40 5\n\
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

    assert!(
        salida.contains("estaciones con bici cerca"),
        "salida:\n{salida}"
    );
    // Ambas estaciones (incluido el líder, que se gossipea a sí mismo) deben aparecer.
    assert!(salida.contains("estación 1"), "salida:\n{salida}");
    assert!(salida.contains("estación 2"), "salida:\n{salida}");
}
