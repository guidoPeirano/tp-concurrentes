//! Estación ↔ Pasarela (TCP). 2PC de la pre-autorización y cobro final.

use serde::{Deserialize, Serialize};

use crate::{DatosTarjeta, Timestamp, TransaccionId, UsuarioId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajeEstacionAPasarela {
    PreparePreauth {
        tx_id: TransaccionId,
        usuario_id: UsuarioId,
        tarjeta: DatosTarjeta,
        monto_propuesto: f64,
    },
    CommitPreauth {
        tx_id: TransaccionId,
        preauth_id: String,
    },
    AbortPreauth {
        tx_id: TransaccionId,
        preauth_id: String,
    },
    ProcesarCobro {
        preauth_id: String,
        t0: Timestamp,
        t1: Timestamp,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MensajePasarelaAEstacion {
    Voto {
        tx_id: TransaccionId,
        resultado: VotoResultado,
        preauth_id: Option<String>,
    },
    PreauthConfirmada { preauth_id: String },
    PreauthAnulada { preauth_id: String },
    CobroConfirmado { preauth_id: String, monto: f64 },
    CobroRechazado { preauth_id: String, motivo: String },
}

/// Voto de un participante en el 2PC.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum VotoResultado {
    Yes,
    No { motivo: String },
}
