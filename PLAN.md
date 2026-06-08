# Plan de trabajo — Sistema de Alquiler de Bicicletas (75.59)

Implementación incremental del diseño descrito en `README.md`. Dividimos el trabajo en etapas
que se construyen una sobre la otra; cada etapa termina con algo que compila, corre y tiene
tests. El detalle de tareas y los criterios de aceptación están en `PLAN_DESARROLLO.md`.

## División del trabajo

Arrancamos armando la base entre todos, con Guido llevando el andamiaje inicial (workspace,
tipos compartidos, comunicación y estación local) para destrabar al resto, y de ahí en más
repartimos por subsistema.

| Etapa | Tema | Responsable(s) |
|---|---|---|
| 0 — Andamiaje | workspace, `comun`, tipos, config | Guido |
| 1 — Comunicación | `Comunicador`, TCP/UDP, framing | Guido |
| 2 — Estación local | `Slot`, `Estacion`, REPL usuario | Guido |
| 3 — Pasarela + 2PC | `ProcesadorPagos`, coordinador 2PC | Tomas, Paul |
| 4 — Líder fijo | registro, devolución (CU2), discovery + consulta directa al líder (CU3) | Paul, Tomas |
| 5 — Elección de líder | algoritmo Ring, reconstrucción de registro | Guido, Paul |
| 6 — Tolerancia a fallas | timeouts 2PC, idempotencia, persistencia, huérfanas | Tomas, Paul |
| 7 — Modo desconectado | servicios alcanzables, pagos pendientes, regularización | Tomas, Paul |
| 8 — Pulido | batería de integración, simulación de fallas, doc final | Todos |

## Convenciones

- Una rama por tarea: `feat/etapaN-slug`. PR a `main`, revisado por otro integrante antes de mergear.
- Mensajes de commit en español, descriptivos.
- Cada `struct`/actor en su propio archivo (requisito del enunciado).
- Antes de cerrar una etapa: `cargo test` verde, `cargo clippy` sin warnings, sin `unsafe`.
- Docs básicas: doc-comments en actores y tipos públicos; sin sobre-comentar.

## Cronograma

| Fecha | Hito |
|---|---|
| 7–8 jun | Etapas 0–2 (base: compila, comunicación, estación local) |
| 9–10 jun | Etapas 3–4 (alquiler atómico, líder fijo, devolución y consulta) |
| 11–12 jun | Etapas 5–6 (elección de líder, tolerancia a fallas) |
| 13 jun | Etapas 7–8 (modo offline, pulido) — objetivo: todo listo |
| 14–15 jun | Buffer para fixes y pruebas finales |
| 16 jun | Entrega |

## Cómo correr y testear

```bash
cargo build
cargo test                                   # unitarios + integración
cargo clippy --all-targets -- -D warnings    # sin warnings
```

Para correr el sistema completo, ver la sección 10 de `README.md`.
