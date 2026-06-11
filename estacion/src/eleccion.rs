//! Algoritmo de elección de líder por **anillo (Ring)**.
//!
//! La lógica vive acá, separada del actor, para poder testearla sin levantar la
//! red ni `actix`. El actor `Estacion` traduce las [`AccionRing`] que devuelven
//! estos métodos a envíos concretos por el `Comunicador` (resuelve las
//! direcciones desde su tabla de estaciones).
//!
//! Mecánica (ver `docs/CASOS_DE_USO.md`, CU4): una estación arranca la elección
//! haciendo circular un `Election` que **acumula ids** por el anillo. Cuando el
//! mensaje vuelve al iniciador, gana el id mayor (`max(ids)`) y se anuncia con un
//! `Coordinator` que recorre el anillo y, al volver al que lo emitió, lo cierra.
//! El `term` se incrementa en cada elección para descartar anuncios viejos.

use comun::mensajes::estacion_estacion::MensajeEntreEstacionesTCP;
use comun::EstacionId;

/// Conocimiento de una estación sobre quién es el líder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EstadoLider {
    /// Hay un líder confirmado por el último `Coordinator`.
    Conocido(EstacionId),
    /// Hay una elección en curso (circuló un `Election` que todavía no cerró).
    EnEleccion,
    /// Todavía no se aprendió quién es el líder.
    Desconocido,
}

/// Lo que la estación debe hacer tras procesar un mensaje del Ring. El actor la
/// ejecuta: resuelve la dirección del `destino` y manda el `mensaje` por TCP.
#[derive(Debug, Clone, PartialEq)]
pub enum AccionRing {
    /// El mensaje era obsoleto o ya dio la vuelta: no hay nada que hacer.
    Ignorar,
    /// Reenviar `mensaje` al siguiente nodo del anillo.
    Reenviar {
        destino: EstacionId,
        mensaje: MensajeEntreEstacionesTCP,
    },
    /// Asumo el liderazgo (el `Coordinator` me nombra a mí) y, si el anillo tiene
    /// más de un nodo, reenvío el anuncio para que el resto se entere.
    AsumirYReenviar {
        destino: Option<EstacionId>,
        mensaje: MensajeEntreEstacionesTCP,
    },
}

/// Estado de elección de una estación: su lugar en el anillo, el término actual y
/// a quién reconoce como líder.
pub struct Eleccion {
    mi_id: EstacionId,
    /// Ids de todas las estaciones del anillo, ordenados ascendentemente. El
    /// "siguiente" de cada nodo es el de id inmediato mayor, y del mayor se vuelve
    /// al menor (anillo).
    anillo: Vec<EstacionId>,
    term: u64,
    estado: EstadoLider,
}

impl Eleccion {
    /// Crea el estado de elección con el líder designado por config (term 0) y el
    /// anillo formado por todas las estaciones conocidas (más uno mismo).
    pub fn new(mi_id: EstacionId, estaciones: impl IntoIterator<Item = EstacionId>) -> Self {
        let mut anillo: Vec<EstacionId> = estaciones.into_iter().collect();
        if !anillo.contains(&mi_id) {
            anillo.push(mi_id);
        }
        anillo.sort_unstable();
        anillo.dedup();
        Self {
            mi_id,
            anillo,
            term: 0,
            estado: EstadoLider::Desconocido,
        }
    }

    /// Fija el líder inicial conocido por config (sin elección: term 0).
    pub fn con_lider_inicial(mut self, lider: EstacionId) -> Self {
        self.estado = EstadoLider::Conocido(lider);
        self
    }

    /// Término actual. Lo va a consumir el discovery (que deja de devolver un term
    /// fijo) en el PR de detección de caída del líder.
    #[allow(dead_code)]
    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn lider_conocido(&self) -> EstadoLider {
        self.estado
    }

    /// Arranca una elección: marca el estado en curso y manda un `Election` con el
    /// propio id al siguiente del anillo. Si soy el único nodo, me autoproclamo. La
    /// dispara la detección de caída del líder (timeout), que llega en el próximo PR.
    #[allow(dead_code)]
    pub fn iniciar(&mut self) -> AccionRing {
        self.estado = EstadoLider::EnEleccion;
        match self.siguiente() {
            Some(destino) => AccionRing::Reenviar {
                destino,
                mensaje: MensajeEntreEstacionesTCP::Election {
                    ids: vec![self.mi_id],
                    iniciador: self.mi_id,
                },
            },
            None => self.autoproclamarse(),
        }
    }

    /// Procesa un `Election` que llegó del anillo.
    ///
    /// - Si soy el iniciador, el mensaje dio la vuelta completa: gana `max(ids)` y
    ///   anuncio el resultado con un `Coordinator` (incrementando el `term`).
    /// - Si mi id ya está en la lista (pero no soy el iniciador), el mensaje ya
    ///   pasó por mí: no lo reenvío (evita que gire para siempre).
    /// - Si no, agrego mi id y lo reenvío al siguiente.
    pub fn recibir_election(&mut self, ids: Vec<EstacionId>, iniciador: EstacionId) -> AccionRing {
        if iniciador == self.mi_id {
            return self.cerrar_eleccion(&ids);
        }
        self.estado = EstadoLider::EnEleccion;
        if ids.contains(&self.mi_id) {
            return AccionRing::Ignorar;
        }
        let mut ids = ids;
        ids.push(self.mi_id);
        match self.siguiente() {
            Some(destino) => AccionRing::Reenviar {
                destino,
                mensaje: MensajeEntreEstacionesTCP::Election { ids, iniciador },
            },
            None => AccionRing::Ignorar,
        }
    }

    /// Procesa un `Coordinator` (anuncio del líder electo).
    ///
    /// Adopta el líder y el `term` si son nuevos, y reenvía el anuncio para que el
    /// resto del anillo se entere. Si el `term` es viejo, o ya conocía a ese mismo
    /// líder en ese mismo `term` (el anuncio dio la vuelta completa), lo ignora:
    /// así se cierra el anillo.
    pub fn recibir_coordinator(&mut self, lider: EstacionId, term: u64) -> AccionRing {
        let ya_anunciado = self.estado == EstadoLider::Conocido(lider);
        if term < self.term || (term == self.term && ya_anunciado) {
            return AccionRing::Ignorar;
        }
        self.term = term;
        self.estado = EstadoLider::Conocido(lider);
        let mensaje = MensajeEntreEstacionesTCP::Coordinator { lider, term };
        let destino = self.siguiente();
        if lider == self.mi_id {
            AccionRing::AsumirYReenviar { destino, mensaje }
        } else {
            match destino {
                Some(destino) => AccionRing::Reenviar { destino, mensaje },
                None => AccionRing::Ignorar,
            }
        }
    }

    /// El `Election` volvió al iniciador: calcula el ganador y emite el `Coordinator`.
    fn cerrar_eleccion(&mut self, ids: &[EstacionId]) -> AccionRing {
        let ganador = ids.iter().copied().max().unwrap_or(self.mi_id);
        self.term += 1;
        self.estado = EstadoLider::Conocido(ganador);
        let mensaje = MensajeEntreEstacionesTCP::Coordinator {
            lider: ganador,
            term: self.term,
        };
        let destino = self.siguiente();
        if ganador == self.mi_id {
            AccionRing::AsumirYReenviar { destino, mensaje }
        } else {
            match destino {
                Some(destino) => AccionRing::Reenviar { destino, mensaje },
                None => AccionRing::Ignorar,
            }
        }
    }

    /// Anillo de un solo nodo: me convierto en líder sin circular nada.
    #[allow(dead_code)]
    fn autoproclamarse(&mut self) -> AccionRing {
        self.term += 1;
        self.estado = EstadoLider::Conocido(self.mi_id);
        AccionRing::AsumirYReenviar {
            destino: None,
            mensaje: MensajeEntreEstacionesTCP::Coordinator {
                lider: self.mi_id,
                term: self.term,
            },
        }
    }

    /// Siguiente nodo del anillo (id inmediato mayor, con wraparound). `None` si
    /// soy el único.
    fn siguiente(&self) -> Option<EstacionId> {
        if self.anillo.len() < 2 {
            return None;
        }
        let pos = self.anillo.iter().position(|&id| id == self.mi_id)?;
        Some(self.anillo[(pos + 1) % self.anillo.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn est(id: u32) -> EstacionId {
        EstacionId(id)
    }

    /// Anillo de prueba: estaciones 1, 2, 3 y 5 (gana la 5).
    fn anillo() -> Vec<EstacionId> {
        vec![est(1), est(2), est(3), est(5)]
    }

    #[test]
    fn el_siguiente_del_mayor_es_el_menor() {
        let e = Eleccion::new(est(5), anillo());
        // No hay accessor público de `siguiente`, lo verificamos vía iniciar().
        let mut e = e;
        match e.iniciar() {
            AccionRing::Reenviar { destino, .. } => assert_eq!(destino, est(1)),
            otra => panic!("esperaba Reenviar al 1, fue {otra:?}"),
        }
    }

    #[test]
    fn election_acumula_id_y_reenvia_al_siguiente() {
        let mut e = Eleccion::new(est(2), anillo());
        let accion = e.recibir_election(vec![est(1)], est(1));
        match accion {
            AccionRing::Reenviar { destino, mensaje } => {
                assert_eq!(destino, est(3), "el siguiente de la 2 es la 3");
                assert_eq!(
                    mensaje,
                    MensajeEntreEstacionesTCP::Election {
                        ids: vec![est(1), est(2)],
                        iniciador: est(1),
                    }
                );
            }
            otra => panic!("esperaba Reenviar, fue {otra:?}"),
        }
        assert_eq!(e.lider_conocido(), EstadoLider::EnEleccion);
    }

    #[test]
    fn election_ya_vista_no_se_reenvia() {
        // La 2 recibe un Election cuyo recorrido ya la incluye: no debe reenviarlo.
        let mut e = Eleccion::new(est(2), anillo());
        let accion = e.recibir_election(vec![est(1), est(2), est(3)], est(1));
        assert_eq!(accion, AccionRing::Ignorar);
    }

    #[test]
    fn el_iniciador_cierra_y_gana_el_id_mayor() {
        // El Election vuelve a la 1 (iniciador) con todos los ids: gana la 5.
        let mut e = Eleccion::new(est(1), anillo());
        let accion = e.recibir_election(vec![est(1), est(2), est(3), est(5)], est(1));
        match accion {
            AccionRing::Reenviar { destino, mensaje } => {
                assert_eq!(destino, est(2), "el Coordinator arranca por el siguiente");
                assert_eq!(
                    mensaje,
                    MensajeEntreEstacionesTCP::Coordinator {
                        lider: est(5),
                        term: 1,
                    }
                );
            }
            otra => panic!("esperaba Reenviar el Coordinator, fue {otra:?}"),
        }
        assert_eq!(e.lider_conocido(), EstadoLider::Conocido(est(5)));
    }

    #[test]
    fn el_term_incrementa_al_cerrar_la_eleccion() {
        let mut e = Eleccion::new(est(1), anillo());
        assert_eq!(e.term(), 0);
        let _ = e.recibir_election(vec![est(1), est(5)], est(1));
        assert_eq!(e.term(), 1, "cerrar una elección incrementa el term");

        // Una segunda elección lo vuelve a incrementar.
        let _ = e.recibir_election(vec![est(1), est(5)], est(1));
        assert_eq!(e.term(), 2);
    }

    #[test]
    fn follower_adopta_el_term_mayor_y_reenvia() {
        let mut e = Eleccion::new(est(2), anillo());
        let accion = e.recibir_coordinator(est(5), 3);
        match accion {
            AccionRing::Reenviar { destino, mensaje } => {
                assert_eq!(destino, est(3));
                assert_eq!(
                    mensaje,
                    MensajeEntreEstacionesTCP::Coordinator {
                        lider: est(5),
                        term: 3,
                    }
                );
            }
            otra => panic!("esperaba Reenviar, fue {otra:?}"),
        }
        assert_eq!(e.lider_conocido(), EstadoLider::Conocido(est(5)));
        assert_eq!(e.term(), 3);
    }

    #[test]
    fn follower_descarta_un_coordinator_viejo() {
        let mut e = Eleccion::new(est(2), anillo());
        let _ = e.recibir_coordinator(est(5), 3); // term actual = 3
                                                  // Llega un anuncio de una elección anterior (term menor): se ignora.
        let accion = e.recibir_coordinator(est(3), 2);
        assert_eq!(accion, AccionRing::Ignorar);
        assert_eq!(e.lider_conocido(), EstadoLider::Conocido(est(5)));
        assert_eq!(e.term(), 3);
    }

    #[test]
    fn el_coordinator_que_da_la_vuelta_cierra_el_anillo() {
        // La 2 ya aprendió al líder 5 en term 3; el mismo anuncio vuelve a pasar
        // por ella (dio la vuelta completa): no lo reenvía.
        let mut e = Eleccion::new(est(2), anillo());
        let _ = e.recibir_coordinator(est(5), 3);
        let accion = e.recibir_coordinator(est(5), 3);
        assert_eq!(accion, AccionRing::Ignorar);
    }

    #[test]
    fn el_ganador_asume_el_liderazgo() {
        let mut e = Eleccion::new(est(5), anillo());
        let accion = e.recibir_coordinator(est(5), 1);
        match accion {
            AccionRing::AsumirYReenviar { destino, mensaje } => {
                assert_eq!(destino, Some(est(1)));
                assert_eq!(
                    mensaje,
                    MensajeEntreEstacionesTCP::Coordinator {
                        lider: est(5),
                        term: 1,
                    }
                );
            }
            otra => panic!("esperaba AsumirYReenviar, fue {otra:?}"),
        }
        assert_eq!(e.lider_conocido(), EstadoLider::Conocido(est(5)));
    }

    #[test]
    fn un_anillo_de_un_solo_nodo_se_autoelige() {
        let mut e = Eleccion::new(est(7), vec![est(7)]);
        match e.iniciar() {
            AccionRing::AsumirYReenviar { destino, mensaje } => {
                assert_eq!(destino, None, "no hay a quién reenviar");
                assert_eq!(
                    mensaje,
                    MensajeEntreEstacionesTCP::Coordinator {
                        lider: est(7),
                        term: 1,
                    }
                );
            }
            otra => panic!("esperaba autoproclamarse, fue {otra:?}"),
        }
        assert_eq!(e.lider_conocido(), EstadoLider::Conocido(est(7)));
        assert_eq!(e.term(), 1);
    }

    #[test]
    fn el_iniciador_que_es_el_mayor_asume_al_cerrar() {
        // La 5 inicia y, al cerrar, ella misma es la ganadora.
        let mut e = Eleccion::new(est(5), anillo());
        let accion = e.recibir_election(vec![est(5), est(1), est(2), est(3)], est(5));
        match accion {
            AccionRing::AsumirYReenviar { destino, mensaje } => {
                assert_eq!(destino, Some(est(1)));
                assert_eq!(
                    mensaje,
                    MensajeEntreEstacionesTCP::Coordinator {
                        lider: est(5),
                        term: 1,
                    }
                );
            }
            otra => panic!("esperaba AsumirYReenviar, fue {otra:?}"),
        }
    }
}
