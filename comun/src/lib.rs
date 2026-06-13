//! Tipos compartidos por las cuatro aplicaciones del sistema: identificadores,
//! tipos de dominio, mensajes de red y helpers de serialización.

pub mod args;
pub mod comunicador;
pub mod config;
pub mod dominio;
pub mod framing;
pub mod ids;
pub mod mensajes;
pub mod persistencia;
pub mod serializacion;
pub mod tiempo;

pub use config::Config;
pub use dominio::{Alquiler, EstadoAlquiler, InfoEstacion};
pub use ids::{BiciId, DatosTarjeta, EstacionId, EventId, RentalId, TransaccionId, UsuarioId};
pub use tiempo::Timestamp;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mensajes::estacion_estacion::MensajeEntreEstacionesTCP;
    use crate::mensajes::estacion_pasarela::{
        MensajeEstacionAPasarela, MensajePasarelaAEstacion, VotoResultado,
    };
    use crate::mensajes::usuario_estacion::{
        MensajeEstacionAUsuarioConsulta, MensajeUsuario, MensajeUsuarioAEstacion,
        MensajeUsuarioAEstacionConsulta,
    };
    use crate::serializacion::{a_json, desde_json};

    /// Serializa un valor y lo vuelve a leer; debe ser idéntico.
    fn round_trip<T>(valor: T)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let json = a_json(&valor).expect("serializa");
        let recuperado: T = desde_json(&json).expect("deserializa");
        assert_eq!(valor, recuperado);
    }

    #[test]
    fn round_trip_de_ids() {
        round_trip(EstacionId(7));
        round_trip(BiciId(42));
        round_trip(RentalId("R1".to_string()));
        round_trip(EventId("E_001".to_string()));
        round_trip(TransaccionId("T_001".to_string()));
        round_trip(UsuarioId("alice".to_string()));
    }

    #[test]
    fn round_trip_de_tarjeta() {
        round_trip(DatosTarjeta {
            numero: "4111111111111111".to_string(),
            titular: "Alice".to_string(),
            vencimiento: "12/29".to_string(),
            cvv: "123".to_string(),
        });
    }

    #[test]
    fn round_trip_de_alquiler() {
        round_trip(Alquiler {
            rental_id: RentalId("R1".to_string()),
            bici_id: BiciId(42),
            usuario_id: UsuarioId("alice".to_string()),
            estacion_origen: EstacionId(1),
            inicio: Timestamp(1_000),
            fin: Some(Timestamp(61_000)),
            preauth_id: Some("p123".to_string()),
            estado: EstadoAlquiler::Activo,
        });
    }

    #[test]
    fn round_trip_de_info_estacion() {
        round_trip(InfoEstacion {
            estacion_id: EstacionId(2),
            ubicacion: (-34.6037, -58.3816),
            bicis_disponibles: 5,
            slots_libres: 10,
            last_seen: Timestamp(123_456),
        });
    }

    #[test]
    fn round_trip_mensaje_usuario_estacion() {
        round_trip(MensajeUsuarioAEstacion::SolicitudAlquiler {
            usuario_id: UsuarioId("alice".to_string()),
            slot_id: 3,
            tarjeta: DatosTarjeta {
                numero: "4111111111111111".to_string(),
                titular: "Alice".to_string(),
                vencimiento: "12/29".to_string(),
                cvv: "123".to_string(),
            },
        });
    }

    #[test]
    fn round_trip_mensaje_consulta_usuario() {
        round_trip(MensajeUsuario::Consulta(
            MensajeUsuarioAEstacionConsulta::PreguntarLider,
        ));
        round_trip(MensajeEstacionAUsuarioConsulta::RespuestaLider {
            lider_id: EstacionId(5),
            lider_addr: "127.0.0.1:8005".parse().expect("addr válida"),
            term: 3,
        });
        round_trip(MensajeEstacionAUsuarioConsulta::EnEleccion);
    }

    #[test]
    fn round_trip_mensaje_estacion_pasarela() {
        round_trip(MensajeEstacionAPasarela::ProcesarCobro {
            preauth_id: "p123".to_string(),
            t0: Timestamp(1_000),
            t1: Timestamp(601_000),
        });
        round_trip(MensajePasarelaAEstacion::Voto {
            tx_id: TransaccionId("T_001".to_string()),
            resultado: VotoResultado::No {
                motivo: "tarjeta inválida".to_string(),
            },
            preauth_id: None,
        });
    }

    #[test]
    fn round_trip_mensaje_entre_estaciones() {
        round_trip(MensajeEntreEstacionesTCP::Election {
            ids: vec![EstacionId(1), EstacionId(2), EstacionId(5)],
            iniciador: EstacionId(1),
        });
        round_trip(MensajeEntreEstacionesTCP::Coordinator {
            lider: EstacionId(5),
            term: 3,
        });
    }
}
