//! Cliente de usuario.
//!
//! Esqueleto de la Etapa 0: guarda el id. El estado de alquiler, la conectividad,
//! el líder conocido y las estaciones conocidas se agregan en la Etapa 2 junto
//! con la REPL.

use comun::UsuarioId;

pub struct Usuario {
    id: UsuarioId,
}

impl Usuario {
    pub fn new(id: UsuarioId) -> Self {
        Self { id }
    }

    pub fn id(&self) -> &UsuarioId {
        &self.id
    }
}
