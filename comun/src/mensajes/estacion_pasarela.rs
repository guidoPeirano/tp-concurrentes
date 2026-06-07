//! Estación ↔ Pasarela (TCP). 2PC de la pre-autorización (CU1) y cobro final (CU2).
//! Flujos completos en `docs/CASOS_DE_USO.md`.

use serde::{Deserialize, Serialize};

use crate::{DatosTarjeta, Timestamp, TransaccionId, UsuarioId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeEstacionAPasarela {
    /// CU1 — fase Prepare del 2PC: valida tarjeta y reserva fondos.
    PreparePreauth {
        tx_id: TransaccionId,
        usuario_id: UsuarioId,
        tarjeta: DatosTarjeta,
        monto_propuesto: f64,
    },
    /// CU1 — fase Commit: deja la preauth activa.
    CommitPreauth {
        tx_id: TransaccionId,
        preauth_id: String,
    },
    /// CU1 — aborto: libera los fondos reservados.
    AbortPreauth {
        tx_id: TransaccionId,
        preauth_id: String,
    },
    /// CU2 — cobro final proporcional a `t1 - t0` (lo dispara el líder al devolverse).
    ProcesarCobro {
        preauth_id: String,
        t0: Timestamp,
        t1: Timestamp,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajePasarelaAEstacion {
    /// CU1 — voto de la pasarela en la fase Prepare.
    Voto {
        tx_id: TransaccionId,
        resultado: VotoResultado,
        preauth_id: Option<String>,
    },
    /// CU1 — confirma el Commit de la preauth.
    PreauthConfirmada { preauth_id: String },
    /// CU1 — confirma el aborto de la preauth.
    PreauthAnulada { preauth_id: String },
    /// CU2 — cobro realizado con éxito.
    CobroConfirmado { preauth_id: String, monto: f64 },
    /// CU2 — el cobro falló (sin fondos, etc.).
    CobroRechazado { preauth_id: String, motivo: String },
}

/// Voto de un participante en el 2PC.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum VotoResultado {
    Yes,
    No { motivo: String },
}
