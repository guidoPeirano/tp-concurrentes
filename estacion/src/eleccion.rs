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
/// ejecuta: prueba entregar el `mensaje` a cada candidato de `destinos` **en
/// orden** y se queda con el primero que acepte la conexión. Así el anillo
/// saltea a los nodos caídos (típicamente, al líder que motivó la elección).
#[derive(Debug, Clone, PartialEq)]
pub enum AccionRing {
    /// El mensaje era obsoleto o ya dio la vuelta: no hay nada que hacer.
    Ignorar,
    /// Reenviar `mensaje` al primer nodo vivo de `destinos` (orden de anillo).
    Reenviar {
        destinos: Vec<EstacionId>,
        mensaje: MensajeEntreEstacionesTCP,
    },
    /// Asumo el liderazgo (el `Coordinator` me nombra a mí) y reenvío el anuncio
    /// para que el resto se entere (`destinos` vacío si el anillo soy yo solo).
    AsumirYReenviar {
        destinos: Vec<EstacionId>,
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

    /// Término actual (lo informa el discovery para que el usuario pueda descartar
    /// respuestas viejas).
    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn lider_conocido(&self) -> EstadoLider {
        self.estado
    }

    /// Arranca una elección: marca el estado en curso y manda un `Election` con el
    /// propio id al siguiente del anillo. Si soy el único nodo, me autoproclamo.
    /// La dispara la vigilancia del líder cuando el sondeo falla.
    pub fn iniciar(&mut self) -> AccionRing {
        let destinos = self.sucesores();
        if destinos.is_empty() {
            return self.autoproclamarse();
        }
        self.estado = EstadoLider::EnEleccion;
        AccionRing::Reenviar {
            destinos,
            mensaje: MensajeEntreEstacionesTCP::Election {
                ids: vec![self.mi_id],
                iniciador: self.mi_id,
            },
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
        AccionRing::Reenviar {
            destinos: self.sucesores(),
            mensaje: MensajeEntreEstacionesTCP::Election { ids, iniciador },
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
        let destinos = self.sucesores();
        if lider == self.mi_id {
            AccionRing::AsumirYReenviar { destinos, mensaje }
        } else {
            AccionRing::Reenviar { destinos, mensaje }
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
        let destinos = self.sucesores();
        if ganador == self.mi_id {
            AccionRing::AsumirYReenviar { destinos, mensaje }
        } else {
            AccionRing::Reenviar { destinos, mensaje }
        }
    }

    /// Anillo de un solo nodo: me convierto en líder sin circular nada.
    fn autoproclamarse(&mut self) -> AccionRing {
        self.term += 1;
        self.estado = EstadoLider::Conocido(self.mi_id);
        AccionRing::AsumirYReenviar {
            destinos: Vec::new(),
            mensaje: MensajeEntreEstacionesTCP::Coordinator {
                lider: self.mi_id,
                term: self.term,
            },
        }
    }

    /// Sucesores en orden de anillo: el siguiente (id inmediato mayor, con
    /// wraparound), después el que le sigue, etc. El que reenvía un mensaje del
    /// Ring prueba en este orden hasta que un nodo acepte la conexión; un nodo
    /// salteado por caído tampoco aparece en `ids`, así que no puede ganar la
    /// elección (la gana la estación viva de id más alto, por construcción).
    fn sucesores(&self) -> Vec<EstacionId> {
        let Some(pos) = self.anillo.iter().position(|&id| id == self.mi_id) else {
            return Vec::new();
        };
        (1..self.anillo.len())
            .map(|i| self.anillo[(pos + i) % self.anillo.len()])
            .collect()
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
    fn los_sucesores_del_mayor_arrancan_en_el_menor() {
        // No hay accessor público de `sucesores`, lo verificamos vía iniciar().
        let mut e = Eleccion::new(est(5), anillo());
        match e.iniciar() {
            AccionRing::Reenviar { destinos, .. } => {
                assert_eq!(destinos, vec![est(1), est(2), est(3)]);
            }
            otra => panic!("esperaba Reenviar con sucesores [1,2,3], fue {otra:?}"),
        }
    }

    #[test]
    fn election_acumula_id_y_reenvia_al_siguiente() {
        let mut e = Eleccion::new(est(2), anillo());
        let accion = e.recibir_election(vec![est(1)], est(1));
        match accion {
            AccionRing::Reenviar { destinos, mensaje } => {
                assert_eq!(
                    destinos,
                    vec![est(3), est(5), est(1)],
                    "primero la 3; si está caída, la 5; y la 1 cierra"
                );
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
            AccionRing::Reenviar { destinos, mensaje } => {
                assert_eq!(
                    destinos[0],
                    est(2),
                    "el Coordinator arranca por el siguiente"
                );
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
            AccionRing::Reenviar { destinos, mensaje } => {
                assert_eq!(destinos[0], est(3));
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
            AccionRing::AsumirYReenviar { destinos, mensaje } => {
                assert_eq!(destinos, vec![est(1), est(2), est(3)]);
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
            AccionRing::AsumirYReenviar { destinos, mensaje } => {
                assert!(destinos.is_empty(), "no hay a quién reenviar");
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
            AccionRing::AsumirYReenviar { destinos, mensaje } => {
                assert_eq!(destinos[0], est(1));
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

    /// Simula la red completa con una cola de mensajes: cada `AccionRing` se
    /// entrega a su primer destino (acá están todos vivos). Dos estaciones
    /// detectan la caída a la vez e inician elecciones simultáneas: el sistema
    /// tiene que converger al mismo líder y al mismo term en todos los nodos,
    /// sin mensajes girando para siempre.
    #[test]
    fn dos_elecciones_simultaneas_convergen_al_mismo_lider() {
        use std::collections::{HashMap, VecDeque};

        let mut nodos: HashMap<EstacionId, Eleccion> = anillo()
            .into_iter()
            .map(|id| (id, Eleccion::new(id, anillo())))
            .collect();
        let mut cola: VecDeque<(EstacionId, MensajeEntreEstacionesTCP)> = VecDeque::new();

        // La 1 y la 3 inician sendas elecciones al mismo tiempo.
        for iniciador in [est(1), est(3)] {
            if let AccionRing::Reenviar { destinos, mensaje } =
                nodos.get_mut(&iniciador).unwrap().iniciar()
            {
                cola.push_back((destinos[0], mensaje));
            }
        }

        let mut pasos = 0;
        while let Some((destino, mensaje)) = cola.pop_front() {
            pasos += 1;
            assert!(pasos < 100, "la elección no converge: mensajes girando");
            let nodo = nodos.get_mut(&destino).unwrap();
            let accion = match mensaje {
                MensajeEntreEstacionesTCP::Election { ids, iniciador } => {
                    nodo.recibir_election(ids, iniciador)
                }
                MensajeEntreEstacionesTCP::Coordinator { lider, term } => {
                    nodo.recibir_coordinator(lider, term)
                }
                otro => panic!("mensaje inesperado en el anillo: {otro:?}"),
            };
            match accion {
                AccionRing::Ignorar => {}
                AccionRing::Reenviar { destinos, mensaje }
                | AccionRing::AsumirYReenviar { destinos, mensaje } => {
                    if let Some(&siguiente) = destinos.first() {
                        cola.push_back((siguiente, mensaje));
                    }
                }
            }
        }

        // Sin mensajes pendientes: todos reconocen a la 5, con el mismo term.
        let term = nodos[&est(1)].term();
        assert!(term >= 1);
        for (id, nodo) in &nodos {
            assert_eq!(
                nodo.lider_conocido(),
                EstadoLider::Conocido(est(5)),
                "la {id:?} debería reconocer a la 5"
            );
            assert_eq!(nodo.term(), term, "term distinto en {id:?}");
        }
    }
}
