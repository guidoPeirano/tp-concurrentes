//! Registro autoritativo de alquileres abiertos que mantiene el líder.
//!
//! Es la "fuente de verdad" del sistema sobre qué alquileres están en curso. Cada
//! estación, además, guarda los suyos en `alquileres_propios` (eso permite
//! reconstruir este registro tras una elección, en la Etapa 5).

use std::collections::HashMap;

use comun::{Alquiler, BiciId, EstadoAlquiler, RentalId};

#[derive(Default)]
pub struct Registro {
    alquileres: HashMap<RentalId, Alquiler>,
}

impl Registro {
    pub fn new() -> Self {
        Self::default()
    }

    /// Incorpora (o reemplaza) un alquiler al registro.
    pub fn agregar(&mut self, alquiler: Alquiler) {
        self.alquileres.insert(alquiler.rental_id.clone(), alquiler);
    }

    /// Busca el alquiler **activo** de una bicicleta. Lo usa el flujo de devolución
    /// (Etapa 4b); por eso todavía no se llama desde el binario.
    #[allow(dead_code)]
    pub fn buscar_por_bici(&self, bici_id: BiciId) -> Option<&Alquiler> {
        self.alquileres
            .values()
            .find(|a| a.bici_id == bici_id && a.estado == EstadoAlquiler::Activo)
    }

    /// Marca un alquiler como cerrado. Devuelve `true` si existía. Lo usa la
    /// devolución (Etapa 4b).
    #[allow(dead_code)]
    pub fn cerrar(&mut self, rental_id: &RentalId) -> bool {
        match self.alquileres.get_mut(rental_id) {
            Some(a) => {
                a.estado = EstadoAlquiler::Cerrado;
                true
            }
            None => false,
        }
    }

    /// Cantidad de alquileres activos (para diagnóstico/consulta).
    pub fn activos(&self) -> usize {
        self.alquileres
            .values()
            .filter(|a| a.estado == EstadoAlquiler::Activo)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use comun::{EstacionId, Timestamp, UsuarioId};

    fn alquiler(rental: &str, bici: u32) -> Alquiler {
        Alquiler {
            rental_id: RentalId(rental.to_string()),
            bici_id: BiciId(bici),
            usuario_id: UsuarioId("alice".to_string()),
            estacion_origen: EstacionId(1),
            inicio: Timestamp(0),
            fin: None,
            preauth_id: "P-1".to_string(),
            estado: EstadoAlquiler::Activo,
        }
    }

    #[test]
    fn agrega_busca_por_bici_y_cierra() {
        let mut reg = Registro::new();
        reg.agregar(alquiler("R1", 42));
        reg.agregar(alquiler("R2", 7));
        assert_eq!(reg.activos(), 2);

        // Busca por bici_id el alquiler activo.
        let encontrado = reg.buscar_por_bici(BiciId(42)).expect("debería estar");
        assert_eq!(encontrado.rental_id, RentalId("R1".to_string()));

        // Cierra R1: deja de aparecer como activo.
        assert!(reg.cerrar(&RentalId("R1".to_string())));
        assert_eq!(reg.activos(), 1);
        assert!(reg.buscar_por_bici(BiciId(42)).is_none());

        // Cerrar algo inexistente devuelve false.
        assert!(!reg.cerrar(&RentalId("R9".to_string())));
    }
}
