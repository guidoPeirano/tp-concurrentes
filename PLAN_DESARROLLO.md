# Plan de desarrollo iterativo — Sistema de Alquiler de Bicicletas

Trabajo Práctico — Programación Concurrente (75.59) — FIUBA

Este documento organiza el desarrollo en etapas incrementales, de lo más genérico (que el sistema compile y los procesos se comuniquen) a lo más específico (tolerancia a fallas y modo desconectado). Cada etapa termina con algo que corre y se puede probar.

## Metodología

- Cada etapa se construye sobre la anterior; lo que se prueba en una etapa se mantiene verde en las siguientes (los tests se acumulan, no se reemplazan).
- Los **criterios de aceptación** arrancan siendo tests unitarios (`cargo test`, lógica aislada de cada actor/estructura) y, a partir de la Etapa 2/3, se suman **tests de integración** que levantan actores o procesos reales y verifican flujos completos.
- Para los tests de integración entre procesos se usa un helper que lanza los binarios con configs de prueba (puertos efímeros) y un cliente que envía mensajes y verifica respuestas.
- Regla transversal de todas las etapas: sin `unsafe`, y `cargo clippy` sin warnings antes de cerrar la etapa.

---

## Etapa 0 — Andamiaje del workspace

**Objetivo:** que el proyecto compile y cada proceso arranque, sin lógica de negocio todavía.

**Tareas:**
- Workspace de Cargo con la crate library `comun` y los binarios `estacion`, `pasarela`, `usuario`.
- Tipos base en `comun`: `EstacionId`, `RentalId`, `BiciId`, `EventId`, `TransaccionId`, `UsuarioId`, `DatosTarjeta`.
- Definición de los enums de mensajes (vacíos o mínimos) con `serde::{Serialize, Deserialize}`.
- Parsing del archivo de configuración `estaciones.json` (estaciones, pasarela, tarifa) con `serde_json`.
- Salida por consola (`println!`/`eprintln!`) y esqueletos de los actores `actix` que arrancan y quedan a la espera.

**Entregable:** `cargo build` compila todo y cada binario levanta e imprime su configuración.

**Verificación (liviana, sin acceptance formal):**
- Unitario: round-trip de serialización (`serialize` → `deserialize`) de los tipos base e IDs.
- Unitario: parsing correcto de un `estaciones.toml` de ejemplo (cantidad de estaciones, puertos, tarifa).

---

## Etapa 1 — Comunicación entre procesos

**Objetivo:** dos procesos se mandan mensajes serializados por la red.

**Tareas:**
- Actor `Comunicador`: abrir `TcpListener`, conectarse a otros nodos, y enviar/recibir mensajes con framing (prefijo de longitud).
- Socket UDP para mensajes no críticos.
- Envelope `MensajeUsuario { Operacion, Consulta }` y reenvío de los mensajes recibidos al actor de negocio.

**Entregable:** un proceso le manda un mensaje a otro y recibe respuesta (basta un echo).

**Criterios de aceptación:**
- Unitario: round-trip de cada variante de mensaje (`MensajeUsuarioAEstacion`, `MensajeEstacionAUsuario`, etc.).
- Unitario: el codificador de framing (length-prefix) codifica y decodifica un mensaje sin pérdida, incluyendo mensajes partidos en varios `read`.
- Integración: levantar dos `Comunicador`, A envía un mensaje a B por TCP y B lo recibe idéntico.
- Integración: A envía un datagrama UDP a B y B lo recibe.

---

## Etapa 2 — Estación local (una sola estación, sin red externa)

**Objetivo:** alquilar y devolver una bici contra una estación aislada, en memoria.

**Tareas:**
- Actor `Slot`: estado `bici: Option<BiciId>`, `reservado_para: Option<TransaccionId>` y sus handlers (`PrepareLiberacion`, `CommitLiberacion`, `AbortLiberacion`, `AceptarBici`, `ConsultarEstado`).
- Actor `Estacion` con `Vec<Addr<Slot>>` y el ruteo de solicitudes al slot correspondiente.
- REPL del `usuario` para disparar acciones por consola.

**Entregable:** flujo de alquiler y devolución funcionando dentro de un solo proceso, sin pasarela ni líder.

**Criterios de aceptación:**
- Unitario (`Slot`): vota `No` si está vacío; vota `Yes` y reserva si tiene la bici; `CommitLiberacion` limpia la bici; `AbortLiberacion` limpia la reserva sin tocar la bici; `AceptarBici` asegura en slot vacío y rechaza en slot ocupado.
- Unitario (`Estacion`): una `SolicitudAlquiler` se rutea al slot indicado.
- Integración: alquiler local end-to-end deja el slot vacío y el usuario en `ConBici`; la devolución deja el slot ocupado y el usuario en `SinBici`.

---

## Etapa 3 — Pasarela y 2PC del alquiler (camino feliz)

**Objetivo:** alquiler atómico de punta a punta entre estación y pasarela.

**Tareas:**
- Actor `ProcesadorPagos` con `PreparePreauth` / `CommitPreauth` / `AbortPreauth` y el cálculo de monto.
- Coordinador del 2PC en `Estacion`: fase Prepare en paralelo a `Slot` + `Pasarela`, decisión de Commit/Abort, confirmación al usuario.
- Mensajes Estación↔Pasarela por TCP a través del `Comunicador`.

**Entregable:** alquiler confirmado con bici liberada y pre-autorización activa, en procesos separados.

**Criterios de aceptación:**
- Unitario (`ProcesadorPagos`): `Prepare` crea preauth `Preparada`, `Commit` la pasa a `Activa`, `Abort` a `Anulada`; idempotencia por `tx_id` / `preauth_id`.
- Unitario (coordinador): decide `Commit` si ambos votan `Yes`, `Abort` si alguno vota `No`.
- Integración: ambos votan `Yes` → bici liberada + preauth activa + usuario `ConBici`.
- Integración: la pasarela vota `No` (tarjeta inválida) → abort, el slot libera la reserva, el usuario recibe `AlquilerRechazado`.

---

## Etapa 4 — Líder fijo: registro, devolución y consulta

**Objetivo:** ciclo completo alquiler→devolución→cobro con un líder estable (designado por config, sin elección todavía).

**Tareas:**
- Rol `Lider` con el registro de alquileres; reporte `AlquilerAbierto` desde la estación de origen (solo con preauth obtenida).
- Flujo CU2: `NotificarDevolucion` → `DatosParaCobro` (incluye `estacion_origen`) → la estación B cobra a la pasarela → `DevolucionProcesada` al líder + `CierreAlquiler` directo al origen.
- Flujo CU3: consulta de disponibilidad directa al líder, con filtrado por proximidad.
- Discovery simple del líder (dirección fija por config).

**Entregable:** alquilar en la estación A, devolver en B, cobrar el monto proporcional al tiempo, y ver el alquiler cerrado en A, B y el líder.

**Criterios de aceptación:**
- Unitario: el registro del líder agrega un alquiler, lo busca por `bici_id` y lo marca cerrado; el filtrado por proximidad devuelve solo estaciones dentro del radio.
- Unitario: la estación B arma correctamente el `ProcesarCobro` a partir de un `DatosParaCobro`.
- Integración (multi-proceso A, B, pasarela, líder): alquiler en A + devolución en B → monto proporcional a `T1 - T0`, `R1` cerrado en A, B y líder.
- Integración: `ConsultaDisponibilidad` devuelve las estaciones cercanas con los conteos correctos de bicis y slots libres.

---

## Etapa 5 — Elección de líder dinámica (Ring)

**Objetivo:** que el sistema elija un líder solo y se recupere de su caída.

**Tareas:**
- Anillo lógico ordenado por `EstacionId`, mensajes `Election` y `Coordinator`, contador `term`.
- Detección de caída del líder (timeout) e inicio de elección.
- Reconstrucción del registro consultando los `alquileres_propios` de cada estación.
- Discovery del líder con `RespuestaLider` / `EnEleccion` / `LiderDesconocido`.

**Entregable:** matar al líder y verificar que se elige uno nuevo y el registro se reconstruye.

**Criterios de aceptación:**
- Unitario: el cálculo del ganador del Ring es `max(ids)`; `term` se incrementa en cada elección; una estación que recibe un `term` mayor pasa a `Follower`.
- Unitario: un `Election` ya visto por el iniciador cierra el anillo (no se reenvía indefinidamente).
- Integración: con N estaciones, matar al líder → gana el de mayor id, `term` incrementa y el registro queda reconstruido a partir de los alquileres propios.
- Integración: dos detecciones simultáneas convergen al mismo líder (los `Coordinator` con el mismo `term` son idempotentes).
- Integración: un usuario que consulta durante la elección recibe `EnEleccion` / `LiderDesconocido` y luego un `RespuestaLider` válido.

---

## Etapa 6 — Tolerancia a fallas e idempotencia

**Objetivo:** sobrevivir caídas y reintentos sin inconsistencias ni cobros duplicados.

**Tareas:**
- Timeouts del 2PC (casos A–D de la sección 7.1.1) con aborto unilateral del participante.
- Idempotencia por `event_id` (`HashSet` en el líder) y por `tx_id` en los participantes.
- Cola de mensajes diferidos en el `Comunicador` y flush al conocerse el líder.
- Reintentos hasta ACK y respuesta `NoRegistradoAun`.
- Persistencia en JSON de `Estacion` y `ProcesadorPagos`, con recuperación al reiniciar.
- Manejo de bicis huérfanas (sección 8.2.1).

**Entregable:** el sistema resiste caídas del coordinador, del líder y de la pasarela, y se recupera desde disco.

**Criterios de aceptación:**
- Unitario: deduplicación por `event_id`; un participante responde idempotente a `Prepare`/`Commit`/`Abort` duplicados (misma respuesta, sin reprocesar).
- Unitario: timeout `Prepare → voto` se interpreta como `No` implícito; timeout `voto Yes → Commit` dispara aborto unilateral y liberación del recurso.
- Unitario: round-trip de serialización del estado persistido (registro, preauths, pagos pendientes).
- Integración: matar al coordinador entre Prepare y Commit → ambos participantes liberan sus recursos reservados.
- Integración: matar al líder con eventos en cola → se despachan al nuevo líder sin duplicar (idempotencia).
- Integración: reiniciar la pasarela / una estación → recuperan el estado desde disco y continúan.
- Integración: una bici que llega sin alquiler abierto → recuperación automática si alguna estación lo tenía, o huérfana confirmada si nadie lo reconoce.

---

## Etapa 7 — Modo desconectado (offline)

**Objetivo:** alquilar y devolver sin conectividad global, y regularizar al reconectar.

**Tareas:**
- `ServiciosAlcanzables { pasarela, lider }` en el `Comunicador`, actualizado por resultado de los intentos.
- Caso E: resolver el alquiler solo con el voto del `Slot` cuando la pasarela no es alcanzable; `preauth_id = None`.
- `PagoPendiente` persistido en disco; regularización al reconectar (preauth + reporte al líder, sin cobrar acá).
- Usuario en modo `SoloLocal`.
- `CobroFallido` como estado en la pasarela.

**Entregable:** alquiler offline que se completa localmente y se regulariza cuando vuelve la conectividad.

**Criterios de aceptación:**
- Unitario: el `Comunicador` marca la pasarela inalcanzable tras el timeout de 5s; la `Estacion` resuelve el 2PC solo con el `Slot` y registra `preauth_id = None`.
- Unitario: round-trip de serialización de `PagoPendiente`; la pasarela marca `CobroFallido` cuando no hay fondos.
- Integración: alquilar con la pasarela caída → bici entregada, `PagoPendiente` en disco, líder no enterado; al volver la pasarela → preauth + `AlquilerAbierto` reportado.
- Integración: devolver una bici alquilada offline → el líder responde `NoRegistradoAun` hasta que el origen regulariza, y luego el flujo de cobro se completa.
- Integración: usuario `SoloLocal` puede alquilar y devolver pero no consultar disponibilidad global.

---

## Etapa 8 — Pulido y pruebas finales

**Objetivo:** dejar el sistema listo para entregar.

**Tareas:**
- Batería de tests de integración cubriendo escenarios combinados.
- Simulación de pérdidas y recuperación de conectividad por input de consola.
- Revisión de logging y documentación (README principal alineado con el código).

**Criterios de aceptación:**
- Integración: pasa toda la batería, incluyendo escenarios combinados (p. ej. caída del líder en medio de una devolución → la devolución igual se cierra contra el origen).
- Integración: elección concurrente bajo carga converge a un único líder.
- `cargo clippy` sin warnings, sin bloques `unsafe`, y todos los tests de las etapas anteriores siguen en verde.
