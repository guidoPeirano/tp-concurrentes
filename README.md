# Sistema de Alquiler de Bicicletas

Trabajo Práctico — Programación Concurrente (75.59) — Facultad de Ingeniería, UBA — Primer cuatrimestre 2026

## Integrantes

- Guido Peirano - 98187
- Tomas Goncalves Rei - 111405
- Paul Gonzalo Garcia Lopez - 112363

---

## Tabla de contenidos

1. [Descripción del problema](#1-descripción-del-problema)
2. [Arquitectura general](#2-arquitectura-general)
3. [Aplicaciones y procesos](#3-aplicaciones-y-procesos)
4. [Entidades y actores](#4-entidades-y-actores)
5. [Mensajes del sistema](#5-mensajes-del-sistema)
6. [Protocolos de comunicación](#6-protocolos-de-comunicación)
7. [Herramientas de concurrencia distribuida](#7-herramientas-de-concurrencia-distribuida)
8. [Flujos principales (casos de uso)](#8-flujos-principales-casos-de-uso)
9. [Estructura del proyecto](#9-estructura-del-proyecto)
10. [Cómo ejecutar el sistema](#10-cómo-ejecutar-el-sistema)
11. [Decisiones de diseño](#11-decisiones-de-diseño)
12. [Cambios desde la primera entrega](#12-cambios-desde-la-primera-entrega)

---

## 1. Descripción del problema

Se debe implementar un sistema distribuido para el alquiler y devolución automatizada de bicicletas en una ciudad. El sistema consta de los siguientes elementos:

- **Estaciones** distribuidas por puntos de interés, cada una con entre 10 y 20 slots que pueden albergar una bicicleta. Cuando un slot está vacío, puede detectar una bicicleta que se acerca (simulado por input), tomarla y asegurarla, liberando al usuario y cobrando un monto proporcional al tiempo de uso.
- **Bicicletas** con un identificador único que las estaciones pueden reconocer.
- **Usuarios** que mediante una aplicación móvil consultan estaciones cercanas con disponibilidad, alquilan, devuelven y pagan.
- **Pasarela de pagos** que simula al procesador externo de tarjetas de crédito, ejecutando pre-autorizaciones y cobros.

### Requerimientos transversales

- El sistema debe seguir operando aunque alguna estación pierda conectividad o el usuario no tenga señal celular.
- Se debe minimizar el tráfico de red, comunicándose preferentemente con nodos cercanos.
- Toda la implementación está hecha en Rust, sin bloques `unsafe`.
- Al menos algunas aplicaciones usan el modelo de actores (utilizando `actix`).
- Cada instancia de cada aplicación corre en un proceso independiente.

---

## 2. Arquitectura general

El sistema está compuesto por cuatro tipos de aplicaciones independientes que se comunican por sockets. Cada aplicación corre en un proceso del sistema operativo separado.

### 2.1 Vista de procesos y comunicación

```mermaid
flowchart TB
    subgraph U["Proceso usuario"]
        UA["Cliente<br/>(no actor)"]
    end

    subgraph CL["Proceso cloud"]
        CLA["Gateway<br/>(no actor)"]
    end

    subgraph E1["Proceso estacion_1"]
        E1A["Estacion"]
        E1S["Slots 0..N<br/>(actores)"]
        E1C["Comunicador"]
        E1A <--> E1S
        E1A <--> E1C
    end

    subgraph E2["Proceso estacion_2"]
        E2A["Estacion"]
        E2S["Slots 0..N<br/>(actores)"]
        E2C["Comunicador"]
        E2A <--> E2S
        E2A <--> E2C
    end

    subgraph P["Proceso pasarela"]
        PA["ProcesadorPagos"]
        PC["Comunicador"]
        PA <--> PC
    end

    UA <-."consulta (TCP)".-> CLA
    UA <-."alquiler/devolución (TCP)".-> E1C
    UA <-."alquiler/devolución (TCP)".-> E2C
    CLA <-.TCP.-> E1C
    CLA <-.TCP.-> E2C
    E1C <-.TCP/UDP.-> E2C
    E1C <-.TCP.-> PC
    E2C <-.TCP.-> PC
```

Las líneas continuas son comunicación entre actores del mismo proceso (vía `Addr`). Las líneas punteadas son comunicación entre procesos por socket.

### 2.2 Decisiones de alto nivel

- **Modelo de actores** en las aplicaciones `estacion` y `pasarela`, usando `actix`. Las aplicaciones `cloud` y `usuario` son clientes simples (no necesariamente con actores).
- **Comunicación entre procesos** por sockets TCP (operaciones críticas, request/response) y UDP (estado agregado periódico, no crítico).
- **Comunicación entre actores del mismo proceso** por `Addr`, en memoria, asincrónico.
- **Topología estática**: cada estación arranca con un archivo de configuración que lista los puertos y datos de todas las estaciones, la pasarela y el cloud.
- **Líder dinámico** elegido entre las estaciones por algoritmo Ring. El líder mantiene un registro autoritativo de alquileres abiertos en el sistema.
- **Cada `struct` y cada tipo de actor en su propio archivo fuente**, según el requerimiento del enunciado.

---

## 3. Aplicaciones y procesos

| Aplicación | Cantidad de instancias | Modelo de actores | Rol |
|---|---|---|---|
| `estacion` | M (una por estación física) | Sí (Estacion + Slot + Comunicador) | Maneja slots, libera y asegura bicis, coordina con vecinas y con pagos. Una de las estaciones es el líder. |
| `pasarela` | 1 | Sí (ProcesadorPagos + Comunicador) | Simula la pasarela de pagos. Maneja pre-autorizaciones y cobros. |
| `cloud` | 1 | No | Gateway que reenvía consultas de disponibilidad al líder. |
| `usuario` | N (una por usuario) | No | Cliente que alquila, devuelve, consulta y paga. |

Las cuatro aplicaciones se construyen como binarios independientes dentro de un mismo workspace de Cargo, compartiendo una crate de utilidades (`comun`) donde viven los tipos de mensajes serializables y las definiciones compartidas.

---

## 4. Entidades y actores

### 4.1 Aplicación `estacion`

#### 4.1.1 Actor `Estacion`

**Finalidad.** Coordina el estado agregado de la estación, gestiona los alquileres iniciados en ella, comunica con el exterior a través del `Comunicador`, y opera el algoritmo de elección de líder. Es el coordinador del 2PC para los alquileres.

**Estado interno.**

```rust
struct Estacion {
    id: EstacionId,
    ubicacion: (f64, f64),
    slots: Vec<Addr<Slot>>,
    alquileres_propios: HashMap<RentalId, Alquiler>,
    rol: RolEstacion,
    lider_conocido: EstadoLider,
    term_actual: u64,
    comunicador: Addr<Comunicador>,
    archivo_persistencia: PathBuf,
}

enum RolEstacion {
    Lider {
        registro_alquileres: HashMap<RentalId, Alquiler>,
        cache_estados: HashMap<EstacionId, EstadoEstacion>,
        eventos_procesados: HashSet<EventId>,
    },
    Follower,
}

enum EstadoLider {
    Conocido(EstacionId, SocketAddr),
    EnEleccion,
    Desconocido,
}

struct Alquiler {
    rental_id: RentalId,
    bici_id: BiciId,
    usuario_id: UsuarioId,
    estacion_origen: EstacionId,
    inicio: Instant,
    fin: Option<Instant>,
    preauth_id: String,
    estado: EstadoAlquiler,
}

enum EstadoAlquiler {
    Activo,
    Cerrado,
}
```

**Ciclo de vida del Alquiler.**

```mermaid
stateDiagram-v2
    [*] --> Activo: 2PC OK<br/>+ AlquilerAbierto reportado al líder
    Activo --> Cerrado: CierreAlquiler<br/>recibido del líder
    Cerrado --> [*]
```

**Mensajes que recibe (selección).**

| Mensaje | Origen | Comportamiento |
|---|---|---|
| `MensajeRecibido(SolicitudAlquiler)` | Comunicador | Inicia el 2PC de alquiler con Slot + Pasarela. |
| `MensajeRecibido(SolicitudDevolucion)` | Comunicador | Acepta físicamente la bici en el Slot, notifica al líder. |
| `Voto(Yes/No)` | Slot / Pasarela | Recolecta votos durante el 2PC. |
| `MensajeRecibido(CierreAlquiler)` | Comunicador | Marca el alquiler local como cerrado. |
| `MensajeRecibido(Election)` | Comunicador | Procesa mensaje del Ring (agrega su ID, forwardea). |
| `MensajeRecibido(Coordinator)` | Comunicador | Actualiza `lider_conocido` y `term_actual`. |
| `LiderNoResponde` | Comunicador | Inicia nueva elección. |
| `MensajeRecibido(SolicitarAlquileresAbiertos)` | Comunicador (cuando es follower) | Responde con sus alquileres propios. |

**Mensajes que envía (selección).**

| Mensaje | Destino | Cuándo |
|---|---|---|
| `Prepare*` / `Commit*` / `Abort*` | Slot, Pasarela | Durante el 2PC de alquiler. |
| `EnviarAlLider(AlquilerAbierto)` | Comunicador | Tras alquiler exitoso (async). |
| `EnviarAlLider(NotificarDevolucion)` | Comunicador | Tras recibir una bici en devolución. |
| `PropagarEstado` | Comunicador | Periódicamente (UDP, snapshot de estado al líder). |
| `EnviarRingMessage` | Comunicador | Durante elecciones. |

#### 4.1.2 Actor `Slot`

**Finalidad.** Representa una posición física donde puede haber o no una bicicleta. Es la unidad atómica que asegura bicis que llegan y libera bicis a usuarios autorizados. Participa como uno de los participantes del 2PC de alquiler.

**Estado interno.**

```rust
struct Slot {
    id: u32,
    bici: Option<BiciId>,
    reservado_para: Option<TransaccionId>,
}
```

**Mensajes que recibe.**

| Mensaje | Origen | Comportamiento |
|---|---|---|
| `PrepareLiberacion { tx_id }` | Estacion | Si tiene bici y no está reservado, marca `reservado_para = Some(tx_id)`, responde `Voto(Yes)`. |
| `CommitLiberacion { tx_id }` | Estacion | Libera la bici (limpia `bici` y `reservado_para`), responde `BiciLiberada`. |
| `AbortLiberacion { tx_id }` | Estacion | Limpia `reservado_para` (no toca la bici). |
| `AceptarBici { bici_id }` | Estacion | Si está vacío, asegura la bici (`bici = Some(bici_id)`), responde `BiciAsegurada`. |
| `ConsultarEstado` | Estacion | Responde `EstadoSlot { ocupado, bici_id }`. |

**Mensajes que envía.**

A `Estacion`: `Voto(Yes/No)`, `BiciLiberada`, `BiciAsegurada`, `EstadoSlot`.

#### 4.1.3 Actor `Comunicador`

**Finalidad.** Aísla la lógica de red. Mantiene los sockets TCP y UDP, gestiona la conexión con vecinas y con el líder, encola mensajes diferidos durante períodos sin conectividad o sin líder conocido.

**Estado interno.**

```rust
struct Comunicador {
    estacion_id: EstacionId,
    estacion_addr: Addr<Estacion>,
    socket_tcp: TcpListener,
    socket_udp: UdpSocket,
    conexiones_tcp: HashMap<EstacionId, TcpStream>,
    vecinas: HashMap<EstacionId, SocketAddr>,
    pasarela_addr: SocketAddr,
    cloud_addr: Option<SocketAddr>,
    cola_para_lider: VecDeque<MensajeAlLider>,
}

struct MensajeAlLider {
    event_id: EventId,
    payload: PayloadSerializado,
    intentos: u32,
}
```

**Mensajes que recibe (selección).**

| Mensaje | Origen | Comportamiento |
|---|---|---|
| `EnviarConfiable { destino, payload }` | Estacion | Envía por TCP. Si falla, reintenta o encola según el destino. |
| `EnviarGossip { destino, payload }` | Estacion | Envía por UDP, fire and forget. |
| `EnviarAlLider { payload }` | Estacion | Si conoce al líder, lo envía. Si no, encola. |
| `MensajeRecibidoDeRed { origen, payload }` | Tasks de escucha | Reenvía al actor `Estacion`. |
| `NuevoLider { id, term }` | Estacion (tras Coordinator) | Actualiza dirección del líder, dispara `FlushCola`. |
| `FlushCola` | self | Reenvía mensajes pendientes al líder actual. |

### 4.2 Aplicación `pasarela`

#### 4.2.1 Actor `ProcesadorPagos`

**Finalidad.** Simula la pasarela bancaria. Maneja pre-autorizaciones (reserva un monto en la tarjeta) y cobros definitivos. Calcula el monto a cobrar a partir de los timestamps `T0` y `T1` y una tarifa configurable. Persiste el estado en un archivo JSON para sobrevivir reinicios. Garantiza idempotencia sobre el `preauth_id`.

**Estado interno.**

```rust
struct ProcesadorPagos {
    pre_autorizaciones: HashMap<String, PreAutorizacion>,
    cobros_realizados: Vec<Cobro>,
    tarifa: TarifaConfig,
    archivo_persistencia: PathBuf,
}

struct PreAutorizacion {
    id: String,
    usuario_id: UsuarioId,
    tarjeta: DatosTarjeta,
    monto_reservado: f64,
    estacion_solicitante: EstacionId,
    timestamp: Instant,
    estado: EstadoPreAuth,
    tx_id: Option<TransaccionId>,
}

enum EstadoPreAuth {
    Preparada,
    Activa,
    Cobrada { monto_final: f64 },
    Anulada,
}

struct TarifaConfig {
    base: f64,
    por_minuto: f64,
}
```

**Mensajes que recibe.**

| Mensaje | Origen | Comportamiento |
|---|---|---|
| `PreparePreauth { tx_id, tarjeta, monto_propuesto }` | Estacion (TCP) | Valida tarjeta, reserva fondos, crea preauth en estado `Preparada`. Responde `Voto(Yes/No)`. |
| `CommitPreauth { tx_id, preauth_id }` | Estacion (TCP) | Marca preauth como `Activa`. Responde `PreauthConfirmada`. |
| `AbortPreauth { tx_id, preauth_id }` | Estacion (TCP) | Libera fondos reservados, marca `Anulada`. |
| `ProcesarCobro { preauth_id, T0, T1 }` | Líder (TCP) | Calcula `monto = tarifa.base + tarifa.por_minuto * (T1 - T0)`. Cobra. Marca `Cobrada`. Responde con el monto. Idempotente: si ya estaba `Cobrada`, devuelve el monto previo. |

#### 4.2.2 Actor `Comunicador` (pasarela)

Análogo al `Comunicador` de la estación, pero más simple. Mantiene un socket TCP en escucha. No participa de elecciones ni de Ring.

### 4.3 Aplicación `cloud`

**Finalidad.** Gateway centralizado que recibe las consultas de disponibilidad de los usuarios y las reenvía al líder actual. No es source of truth; solo enruta.

**Estado interno.**

```rust
struct Cloud {
    lider_conocido: Option<(EstacionId, SocketAddr, u64)>, // id, addr, term
    estaciones_config: HashMap<EstacionId, SocketAddr>,
    socket: TcpListener,
}
```

**Mensajes que recibe.**

| Mensaje | Origen | Comportamiento |
|---|---|---|
| `ConsultaDisponibilidad { ubicacion, radio }` | Usuario (TCP) | Reenvía al líder. Si no conoce líder, descubre uno preguntando a estaciones. |
| `SoyElLider { id, term }` | Estación electa (TCP) | Actualiza `lider_conocido` si el `term` es mayor al actual. |

### 4.4 Aplicación `usuario`

**Finalidad.** Cliente que el usuario opera por consola. Mantiene su estado de alquiler (con o sin bici), envía solicitudes a las estaciones y al cloud, recibe confirmaciones.

**Estado interno.**

```rust
struct Usuario {
    id: UsuarioId,
    ubicacion: (f64, f64),
    tarjeta: DatosTarjeta,
    estado: EstadoUsuario,
    cloud_addr: SocketAddr,
    estaciones_conocidas: HashMap<EstacionId, SocketAddr>,
}

enum EstadoUsuario {
    SinBici,
    ConBici {
        bici_id: BiciId,
        rental_id: RentalId,
    },
}
```

Modelar el estado como `enum` evita estados inválidos por construcción.

---

## 5. Mensajes del sistema

Esta sección consolida los mensajes que viajan por la red. Los mensajes internos entre actores del mismo proceso (vía `Addr`) están descritos en la sección de cada actor.

### 5.1 Usuario ↔ Estación (TCP)

```rust
enum MensajeUsuarioAEstacion {
    SolicitudAlquiler {
        usuario_id: UsuarioId,
        slot_id: u32,
        tarjeta: DatosTarjeta,
    },
    SolicitudDevolucion {
        usuario_id: UsuarioId,
        bici_id: BiciId,
        rental_id: RentalId,
    },
}

enum MensajeEstacionAUsuario {
    AlquilerConfirmado {
        rental_id: RentalId,
        bici_id: BiciId,
        preauth_id: String,
    },
    AlquilerRechazado {
        motivo: String,
    },
    DevolucionAceptada {
        bici_id: BiciId,
    },
    DevolucionCompletada {
        rental_id: RentalId,
        monto_cobrado: f64,
        tiempo_uso_minutos: u32,
    },
}
```

### 5.2 Usuario ↔ Cloud (TCP)

```rust
enum MensajeUsuarioACloud {
    ConsultaDisponibilidad {
        usuario_id: UsuarioId,
        ubicacion: (f64, f64),
        radio_max_km: f64,
    },
}

enum MensajeCloudAUsuario {
    RespuestaDisponibilidad {
        estaciones: Vec<InfoEstacion>,
    },
    ErrorSistema {
        motivo: String,
    },
}

struct InfoEstacion {
    estacion_id: EstacionId,
    ubicacion: (f64, f64),
    bicis_disponibles: u32,
    slots_libres: u32,
    last_seen: Instant,
}
```

### 5.3 Cloud ↔ Líder (TCP)

```rust
enum MensajeCloudAEstacion {
    ConsultaDisponibilidad {
        ubicacion: (f64, f64),
        radio_max_km: f64,
    },
    PreguntarLider,
}

enum MensajeEstacionACloud {
    RespuestaDisponibilidad { estaciones: Vec<InfoEstacion> },
    RespuestaLider { lider_id: Option<EstacionId>, term: u64 },
    SoyElLider { estacion_id: EstacionId, term: u64 },
}
```

### 5.4 Estación ↔ Estación (TCP para crítico, UDP para periódico)

```rust
enum MensajeEntreEstacionesTCP {
    // 2PC del alquiler (no se usa entre estaciones, queda en local)
    
    // Eventos al líder
    AlquilerAbierto {
        event_id: EventId,
        rental_id: RentalId,
        bici_id: BiciId,
        usuario_id: UsuarioId,
        estacion_origen: EstacionId,
        T0: Instant,
        preauth_id: String,
    },
    NotificarDevolucion {
        event_id: EventId,
        bici_id: BiciId,
        estacion_destino: EstacionId,
        T1: Instant,
    },
    DevolucionProcesada {
        event_id: EventId,
        rental_id: RentalId,
        monto_cobrado: f64,
        tiempo_uso_minutos: u32,
    },
    CierreAlquiler {
        rental_id: RentalId,
        T1: Instant,
        monto_cobrado: f64,
    },
    EventoProcesadoAck { event_id: EventId },

    // Reconstrucción de registro
    SolicitarAlquileresAbiertos { term: u64 },
    RespuestaAlquileres { alquileres: Vec<Alquiler> },
    IngresoTardio { alquileres: Vec<Alquiler> },

    // Manejo de bicis huérfanas
    BuscarAlquilerPropio { event_id: EventId, bici_id: BiciId },
    AlquilerEncontrado { event_id: EventId, alquiler: Alquiler },
    NoLoTengo { event_id: EventId, bici_id: BiciId },
    AlquilerNoEncontrado { bici_id: BiciId },
    ReprocesarDevolucion { bici_id: BiciId },
    BiciHuerfanaConfirmada { bici_id: BiciId },

    // Ring de elección
    Election { ids: Vec<EstacionId>, iniciador: EstacionId },
    Coordinator { lider: EstacionId, term: u64 },
}

enum MensajeEntreEstacionesUDP {
    EstadoEstacion {
        estacion_id: EstacionId,
        ubicacion: (f64, f64),
        bicis_disponibles: u32,
        slots_libres: u32,
        timestamp: Instant,
    },
}
```

### 5.5 Estación ↔ Pasarela (TCP)

```rust
enum MensajeEstacionAPasarela {
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
        T0: Instant,
        T1: Instant,
    },
}

enum MensajePasarelaAEstacion {
    Voto { tx_id: TransaccionId, resultado: VotoResultado, preauth_id: Option<String> },
    PreauthConfirmada { preauth_id: String },
    PreauthAnulada { preauth_id: String },
    CobroConfirmado { preauth_id: String, monto: f64 },
    CobroRechazado { preauth_id: String, motivo: String },
}

enum VotoResultado {
    Yes,
    No { motivo: String },
}
```

---

## 6. Protocolos de comunicación

| Enlace | Protocolo | Justificación |
|---|---|---|
| Usuario ↔ Estación (alquiler, devolución) | TCP | Operaciones críticas, request/response. |
| Usuario ↔ Cloud (consulta disponibilidad) | TCP | Request/response, requiere confiabilidad. |
| Cloud ↔ Líder | TCP | Crítico, no se debe perder respuestas. |
| Estación ↔ Pasarela | TCP | Involucra dinero; debe garantizar entrega y orden. |
| Estación → Líder (eventos de alquiler) | TCP | Crítico: perder un evento equivale a perder un cobro. |
| Estación ↔ Estación (Ring de elección) | TCP | Protocolo secuencial, requiere orden y entrega. |
| Estación → Líder (estado agregado periódico) | UDP | Frecuente, perdidas individuales son tolerables (el siguiente snapshot compensa). |

### Justificación de la elección TCP/UDP

**TCP** se usa donde la pérdida de un mensaje genera inconsistencia o pérdida de dinero, donde el orden importa, y donde se necesita confirmación de recepción. La sobrecarga del handshake es aceptable para volúmenes bajos.

**UDP** se usa para el único caso donde los mensajes son frecuentes y la pérdida no es problemática: las actualizaciones de estado agregado de cada estación al líder. Estas se envían cada ~3 segundos por estación; si se pierde uno, el siguiente actualiza la cache del líder con info más fresca.

---

## 7. Herramientas de concurrencia distribuida

El sistema utiliza dos herramientas de concurrencia distribuida vistas en la cátedra, según el requerimiento del enunciado.

### 7.1 Two-Phase Commit (2PC) — Alquiler de bicicleta

**Aplicación:** garantiza la atomicidad del proceso de alquiler entre los tres participantes involucrados.

**Roles:**

- **Coordinador:** Actor `Estacion` (la estación donde el usuario inicia el alquiler).
- **Participantes:**
  - Actor `Slot` (local, el slot específico que liberará la bici).
  - Actor `ProcesadorPagos` (remoto, en proceso `pasarela`).

**Fases:**

```mermaid
sequenceDiagram
    participant E as Estación (coord)
    participant S as Slot
    participant P as Pasarela

    Note over E: Recibe SolicitudAlquiler<br/>Genera tx_id

    Note over E,P: FASE PREPARE (en paralelo)
    E->>S: PrepareLiberacion(tx_id)
    E->>P: PreparePreauth(tx_id, ...)
    S-->>E: Voto(Yes)
    P-->>E: Voto(Yes, preauth_id)

    Note over E,P: FASE COMMIT (ambos Yes)
    E->>S: CommitLiberacion(tx_id)
    E->>P: CommitPreauth(tx_id, preauth_id)
    S-->>E: BiciLiberada
    P-->>E: PreauthConfirmada

    Note over E: Registra alquiler localmente<br/>Confirma al usuario
```

**Si alguno vota No** (slot vacío, tarjeta inválida, etc.) → el coordinador envía `Abort` a ambos participantes. El slot libera la reserva, la pasarela libera fondos reservados. El usuario recibe `AlquilerRechazado`.

**Garantías:** o las dos acciones ocurren (slot libera bici + pasarela retiene fondos) o ninguna. No quedan estados intermedios donde se cobró sin entregar bici, o viceversa.

#### 7.1.1 Manejo de fallas del 2PC
El 2PC clásico tiene puntos de falla conocidos que debemos manejar explícitamente. Documentamos cada uno:

**Caso A — Participante se cae durante Prepare.**
Si un participante (Slot o Pasarela) se cae antes de votar, el coordinador no recibe el voto. Tras un timeout configurado (default: 3 segundos), el coordinador considera el voto como `No` implícito y aborta la transacción enviando `Abort` al participante sobreviviente. El usuario recibe `AlquilerRechazado` con motivo "timeout en preparación".

**Caso B — Coordinador se cae entre Prepare y Commit (caso crítico clásico).**
Es el peor caso del 2PC: los participantes ya votaron `Yes`, reservaron tentativamente sus recursos, y esperan la decisión final que nunca llega. Cada participante implementa un timeout de transacción (default: 10 segundos): si después de votar `Yes` no recibe `Commit` ni `Abort`, aborta unilateralmente y libera el recurso reservado.

- En el **Slot**, esto significa limpiar `reservado_para = None` y mantener la bici asegurada.
- En la **Pasarela**, significa liberar los fondos reservados y marcar la pre-autorización como `Anulada`.

Cuando el usuario reintente el alquiler, arranca un nuevo 2PC con un nuevo `tx_id`. No hay riesgo de doble cobro porque el `preauth_id` anterior quedó anulado.

**Caso C — Coordinador se cae durante Commit (después de que algún participante ya commiteó).**
Acá puede haber inconsistencia temporal: el Slot commiteó (liberó la bici) pero la Pasarela no llegó a commitear. Mitigación:

- Si la Pasarela timeoutea sobre el `Commit` esperado, no aborta: como ya votó `Yes`, sabe que el coordinador decidió commit (o estaba a punto de hacerlo). Espera reintentos. Cuando el coordinador vuelve, persiste sus alquileres pendientes en disco y completa el Commit que le faltaba.
- Si el coordinador no vuelve nunca (caída permanente), la pre-autorización queda en estado `Preparada` indefinidamente. El sistema la detecta como "transacción huérfana" tras un tiempo prudencial (configurable, default: 1 hora) y la marca como `Anulada`. El usuario que se llevó la bici real (porque el Slot sí commiteó) no es cobrado por este alquiler — el alquiler nunca quedó registrado en el sistema. 

**Caso D — Participante recibe un mensaje duplicado.**
Si por reintentos un participante recibe dos veces el mismo `PrepareLiberacion` con el mismo `tx_id`, responde idempotentemente (misma respuesta que la primera vez, sin re-procesar). Lo mismo aplica para `Commit` y `Abort`. Cada participante mantiene un set de `tx_id` ya procesados.

**Timeouts configurables:**

| Timeout | Valor default | Configurable |
|---|---|---|
| Prepare → Voto | 3 segundos | Si |
| Voto Yes → Commit/Abort | 10 segundos | Si |
| Deteccion de transaccion huerfana | 1 hora | Si |

**Persistencia del estado del 2PC.** El coordinador persiste en disco la transacción cuando todos votaron `Yes` y antes de enviar `Commit`. Esto permite recuperarse de un crash del coordinador entre Prepare y Commit: al reiniciar, lee las transacciones pendientes y completa el Commit que tenía pendiente.

### 7.2 Elección de líder por algoritmo Ring

**Aplicación:** seleccionar dinámicamente entre las estaciones cuál cumple el rol de líder del registro de alquileres. Tolera caídas del líder mediante re-elección.

**Topología lógica:** las estaciones forman un anillo, ordenado por `EstacionId`. Cada estación conoce a su "siguiente en el anillo".

**Fases del Ring:**

1. **Detección:** una estación detecta que el líder no responde (timeout sobre intentos previos).
2. **Inicio:** la estación inicia un mensaje `Election` con su ID, lo envía a la siguiente del anillo.
3. **Propagación:** cada estación que recibe `Election` agrega su ID y lo forwardea.
4. **Decisión:** el mensaje vuelve al iniciador con todos los IDs. El ganador es el de ID mayor.
5. **Anuncio:** el iniciador envía `Coordinator(ganador, term)` por el anillo. Cada estación se actualiza.
6. **Reconstrucción del registro:** el nuevo líder solicita a cada estación sus alquileres propios y consolida el registro.
7. **Anuncio al cloud:** el nuevo líder envía `SoyElLider` al cloud.

```mermaid
sequenceDiagram
    participant A as Estación A
    participant B as Estación B
    participant C as Estación C
    participant E as Estación E (futura líder)

    Note over A: Detecta líder caído<br/>Inicia elección
    A->>B: Election([A])
    B->>C: Election([A, B])
    C->>E: Election([A, B, C])
    E->>A: Election([A, B, C, E])

    Note over A: Ganador = max = E<br/>Envía Coordinator
    A->>B: Coordinator(E, term=N+1)
    B->>C: Coordinator(E, term=N+1)
    C->>E: Coordinator(E, term=N+1)
    Note over E: Sé que soy líder
    E->>A: Coordinator(E, term=N+1)
```

**Detalle sobre `term`:** cada elección incrementa un contador (`term`). Esto previene que un líder "viejo" que vuelve a la vida tras una caída se confunda con el líder actual: si recibe un mensaje con `term` mayor al que tiene, sabe que no es más líder y se actualiza a follower.

**Idempotencia y reintentos:** todos los mensajes al líder (eventos de alquiler, devoluciones) llevan un `event_id`. El líder mantiene un `HashSet<EventId>` de eventos ya procesados. Si recibe un duplicado, responde OK sin reprocesar. Esto permite a las estaciones reintentar indefinidamente durante elecciones sin causar inconsistencias.

---

## 8. Flujos principales (casos de uso)

### 8.1 CU1 — Alquiler exitoso

**Disparador:** El usuario se acerca físicamente a un slot ocupado con una bicicleta y le pide a su app que la alquile.

**Participantes:**

| Proceso | Actores |
|---|---|
| Usuario | (cliente simple) |
| Estación A | Estación, Slot 3, Comunicador |
| Pasarela | ProcesadorPagos, Comunicador |
| Líder (otra estación, async) | Estación en rol líder |

**Diagrama de secuencia:**

```mermaid
sequenceDiagram
    participant U as Usuario
    participant CE as Comunicador A
    participant E as Estación A
    participant S as Slot 3
    participant P as Pasarela
    participant L as Líder

    U->>CE: SolicitudAlquiler<br/>(TCP)
    CE->>E: MensajeRecibido
    Note over E: Genera tx_id=T_001<br/>Inicia 2PC

    par Prepare en paralelo
        E->>S: PrepareLiberacion(T_001)
        and
        E->>P: PreparePreauth(T_001)<br/>(via Comunicador, TCP)
    end

    S-->>E: Voto(Yes)
    P-->>E: Voto(Yes, preauth_id=p123)

    Note over E: Decide Commit

    par Commit en paralelo
        E->>S: CommitLiberacion(T_001)
        and
        E->>P: CommitPreauth(T_001, p123)
    end

    S-->>E: BiciLiberada(X)
    P-->>E: PreauthConfirmada(p123)

    Note over E: Graba R1 en alquileres_propios

    E->>CE: AlquilerConfirmado
    CE->>U: AlquilerConfirmado<br/>(TCP)

    Note over E,L: Asíncrono (no bloquea al usuario)
    E->>CE: EnviarAlLider(AlquilerAbierto)
    CE->>L: AlquilerAbierto(R1, X, ...)<br/>(TCP)
    L-->>CE: ACK
```

**Resultado:**
- Slot 3 vacío, bici X con el usuario.
- Preauth p123 activa en pasarela.
- R1 en `alquileres_propios` de Estación A y en el registro del líder.
- Usuario en estado `ConBici`.

**Casos de error manejados por el 2PC:**
- Slot vota `No` (slot vacío o ya reservado): aborta, usuario recibe `AlquilerRechazado`.
- Pasarela vota `No` (tarjeta inválida, sin fondos): aborta, slot libera reserva, usuario recibe `AlquilerRechazado`.
- Líder caído al momento del reporte async: Comunicador encola el evento, se despacha al elegirse nuevo líder. El alquiler ya está completado para el usuario.

### 8.2 CU2 — Devolución exitosa

**Disparador:** El usuario llega a una estación distinta a la de origen, se acerca a un slot vacío, indica devolver la bici.

**Participantes:**

| Proceso | Rol |
|---|---|
| Usuario | Cliente |
| Estación B (destino) | Asegura la bici, notifica al líder |
| Líder | Procesa cierre + cobro |
| Pasarela | Calcula monto y cobra |
| Estación A (origen) | Recibe notificación async de cierre |

**Diagrama de secuencia:**

```mermaid
sequenceDiagram
    participant U as Usuario
    participant EB as Estación B
    participant SN as Slot N (B)
    participant L as Líder
    participant P as Pasarela
    participant EA as Estación A (origen)

    U->>EB: SolicitudDevolucion<br/>(TCP)
    EB->>SN: AceptarBici(X)
    SN-->>EB: BiciAsegurada
    EB->>U: DevolucionAceptada<br/>(usuario puede irse)

    Note over EB: Genera event_id=E_001

    EB->>L: NotificarDevolucion(E_001, X, T1)<br/>(TCP)
    Note over L: Busca R1 en registro<br/>R1 tiene preauth=p123, T0
    L->>P: ProcesarCobro(p123, T0, T1)<br/>(TCP)
    Note over P: Calcula monto=tarifa(T1-T0)
    P-->>L: CobroConfirmado(p123, monto=Y)
    Note over L: Marca R1 como Cerrado<br/>Registra event_id procesado

    L-->>EB: DevolucionProcesada(R1, monto=Y, tiempo)<br/>(TCP)
    EB->>U: DevolucionCompletada(R1, Y, tiempo)<br/>(TCP)

    Note over L,EA: Asíncrono
    L->>EA: CierreAlquiler(R1, T1, Y)<br/>(TCP)
    EA-->>L: ACK<br/>(marca R1 como Cerrado localmente)
```

**Resultado:**
- Slot N de Estación B ocupado con bici X.
- Preauth p123 cobrada (monto Y).
- R1 cerrado en registro del líder y en `alquileres_propios` de Estación A.
- Usuario en estado `SinBici`, ve confirmación de cobro.

**Manejo de errores (todos resueltos vía idempotencia):**

| Falla | Recuperación |
|---|---|
| Líder caído al notificar devolución | Comunicador de B encola. Nuevo líder se elige (CU4), recibe el evento, procesa. |
| Pasarela caída en `ProcesarCobro` | Líder reintenta con backoff. La preauth queda como referencia idempotente. Si vuelve y ya estaba cobrada, devuelve idempotente. |
| Estación origen offline al notificar cierre | Líder reintenta. Cuando A vuelve, recibe el cierre y se sincroniza. |
| Bici "huérfana" (no hay alquiler abierto con esa bici_id en el registro del lider) | Ver subsección "Manejo de bicis huérfanas" abajo. |

#### 8.2.1 — Manejo de bicis huerfanas

Una bici se considera **huérfana** cuando llega físicamente a un slot pero el líder no encuentra un alquiler abierto asociado a esa `bici_id` en su registro. Esto puede ocurrir por dos motivos:
- El evento `AlquilerAbierto` original nunca llegó al líder (se perdió durante una caída prolongada, o la estación origen lo persistió pero el reporte async quedó en cola).
- Hubo un cambio de líder reciente y la reconstrucción del registro está incompleta (alguna estación no respondió a `SolicitarAlquileresAbiertos` durante la elección).

**Resultado posible 1 — recuperación automática:** alguna estación tenía el alquiler en sus `alquileres_propios` pero no se había reportado al líder. El líder lo reincorpora al registro y el flujo de devolución continúa normalmente.
**Resultado posible 2 — huérfana confirmada:** ninguna estación reconoce el alquiler. La bici queda asegurada en el slot pero no se cobra (no hay pre-autorización asociada). Se registra en el log de la estación destino para auditoría. La bici está disponible para nuevos alquileres a partir de ese momento.

Este protocolo asegura que el sistema converge a un estado consistente para cualquier bici que llegue a un slot, independientemente de qué eventos se hayan perdido en el camino, sin requerir intervención manual del operador.

### 8.3 CU3 — Consulta de disponibilidad vía cloud

**Disparador:** El usuario abre la app y solicita ver las estaciones cercanas con bicis y slots disponibles.

**Diagrama de secuencia:**

```mermaid
sequenceDiagram
    participant U as Usuario
    participant C as Cloud
    participant L as Líder

    U->>C: ConsultaDisponibilidad(ubicacion, radio)<br/>(TCP)

    alt Cloud no conoce líder
        C->>L: PreguntarLider<br/>(a cualquier estación de la config)
        L-->>C: RespuestaLider(id, term)
        Note over C: Actualiza lider_conocido
    end

    C->>L: ConsultaDisponibilidad(ubicacion, radio)<br/>(TCP)
    Note over L: Filtra cache_estados<br/>por proximidad
    L-->>C: RespuestaDisponibilidad([estaciones cercanas])
    C-->>U: RespuestaDisponibilidad
    Note over U: App muestra lista en consola
```

**Resultado:** el usuario ve la lista de estaciones cercanas con sus bicis y slots libres en el momento de la consulta (data con latencia de hasta ~3 segundos, por el ciclo periódico de UDP entre estaciones y líder).

**Casos de error:**

| Falla | Recuperación |
|---|---|
| Cloud no conoce líder | Pregunta a cualquier estación. Si está en elección, reintenta. |
| Líder no responde (timeout) | Cloud marca líder como Desconocido, espera nueva elección. Responde al usuario "sistema reorganizándose". |
| Cache del líder tiene info muy vieja sobre una estación | Líder excluye esa estación de la respuesta o la marca como "stale". |
| Cloud caído | App del usuario muestra "sin conexión, datos cacheados" o error. |

### 8.4 CU4 — Caída del líder y reconstrucción del registro

**Disparador:** Una estación detecta que el líder actual no responde (timeout en intentos previos).

**Diagrama de secuencia:**

```mermaid
sequenceDiagram
    participant A as Estación A
    participant B as Estación B
    participant C as Estación C
    participant E as Estación E<br/>(nueva líder)
    participant CL as Cloud

    Note over A: Timeout esperando<br/>al líder D
    Note over A: Inicia Ring

    A->>B: Election([A])
    B->>C: Election([A, B])
    C->>E: Election([A, B, C])
    E->>A: Election([A, B, C, E])
    Note over A: max(ids) = E

    A->>B: Coordinator(E, term=N+1)
    B->>C: Coordinator(E, term=N+1)
    C->>E: Coordinator(E, term=N+1)
    Note over E: Soy líder

    Note over E: Reconstruye registro
    par broadcast a todas
        E->>A: SolicitarAlquileresAbiertos
        and
        E->>B: SolicitarAlquileresAbiertos
        and
        E->>C: SolicitarAlquileresAbiertos
    end
    A-->>E: RespuestaAlquileres([R1, R5])
    B-->>E: RespuestaAlquileres([R7])
    C-->>E: RespuestaAlquileres([])
    Note over E: Consolida<br/>registro = {R1, R5, R7}

    E->>CL: SoyElLider(E, term=N+1)
    Note over CL: Actualiza lider_conocido

    Note over A,E: Eventos pendientes en colas<br/>se despachan a E
```

**Resultado:**
- E es el nuevo líder reconocido por todos (estaciones + cloud).
- El registro de alquileres está reconstruido a partir de los `alquileres_propios` de cada estación.
- Los eventos en colas de los Comunicadores se despachan al nuevo líder.
- La transición típicamente dura unos pocos segundos. Durante ese tiempo, las operaciones locales siguen funcionando; solo las que requieren al líder se encolan.

**Casos edge:**

| Caso | Manejo |
|---|---|
| Dos estaciones detectan caída simultáneamente | Ambas inician elección. Los dos mensajes Election circulan en paralelo. Ambos calculan el mismo ganador. Los `Coordinator` con mismo `term` son idempotentes. |
| Una estación está caída durante la elección | El mensaje Election la saltea (timeout + skip). Cuando vuelve, sincroniza el `term` y se reincorpora. |
| El "ex-líder" D vuelve | Recibe Coordinator con `term` mayor. Se actualiza como follower. |
| Una estación está caída durante la reconstrucción | E timeoutea, continúa con datos parciales. Cuando la estación vuelve, envía `IngresoTardio` y el líder consolida. |
| Cloud está caído cuando E manda SoyElLider | E reintenta hasta entregar. Mientras tanto, los usuarios reciben "sistema reorganizándose" del cloud cuando vuelva. |

---

## 9. Estructura del proyecto

```
proyecto-bicis/
├── Cargo.toml                       (workspace root)
├── README.md                        (este archivo)
├── comun/                           (crate library: tipos compartidos)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── ids.rs                   (RentalId, BiciId, EstacionId, EventId, ...)
│       ├── mensajes/
│       │   ├── usuario_estacion.rs
│       │   ├── usuario_cloud.rs
│       │   ├── estacion_estacion.rs
│       │   ├── estacion_pasarela.rs
│       │   └── cloud_estacion.rs
│       └── serializacion.rs
├── estacion/                        (crate binary)
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs
│       ├── estacion.rs              (actor Estacion)
│       ├── slot.rs                  (actor Slot)
│       ├── comunicador.rs           (actor Comunicador)
│       ├── alquiler.rs              (Alquiler, EstadoAlquiler)
│       ├── ring.rs                  (lógica del algoritmo de elección)
│       ├── dos_fases.rs             (helpers del 2PC)
│       └── persistencia.rs          (serialización a JSON)
├── pasarela/                        (crate binary)
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs
│       ├── procesador_pagos.rs      (actor ProcesadorPagos)
│       ├── comunicador.rs           (actor Comunicador)
│       ├── tarifa.rs                (cálculo del monto)
│       └── persistencia.rs
├── cloud/                           (crate binary)
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs
│       └── gateway.rs
└── usuario/                         (crate binary)
    ├── Cargo.toml
    └── src/
        ├── main.rs
        ├── usuario.rs
        └── repl.rs                  (lectura de comandos por consola)
```

Cada `struct` y cada tipo de actor en su propio archivo fuente, según el requerimiento del enunciado.

---

## 10. Cómo ejecutar el sistema

**Nota:** la implementación está en curso. Las instrucciones definitivas se actualizarán para la entrega final.

Esquema preliminar:

```bash
# Compilar todo el workspace
cargo build --release

# Lanzar el sistema de pagos
cargo run --release --bin pasarela -- --puerto 9000 --config pasarela.toml

# Lanzar el cloud
cargo run --release --bin cloud -- --puerto 9100 --config cloud.toml

# Lanzar estaciones (en terminales separadas)
cargo run --release --bin estacion -- --id 1 --puerto 8001 --config estaciones.toml
cargo run --release --bin estacion -- --id 2 --puerto 8002 --config estaciones.toml
cargo run --release --bin estacion -- --id 3 --puerto 8003 --config estaciones.toml

# Lanzar usuarios (en terminales separadas)
cargo run --release --bin usuario -- --id alice --cloud localhost:9100
cargo run --release --bin usuario -- --id bob --cloud localhost:9100
```

Cada proceso lee input por consola para simular las acciones (acercarse a un slot, perder/recuperar conectividad, etc.) y escribe el output del sistema también por consola.

### Archivo de configuración de topología (ejemplo)

```toml
# estaciones.toml
[[estaciones]]
id = 1
puerto = 8001
ubicacion = [-34.6037, -58.3816]

[[estaciones]]
id = 2
puerto = 8002
ubicacion = [-34.6100, -58.3850]

[[estaciones]]
id = 3
puerto = 8003
ubicacion = [-34.6200, -58.3900]

[pasarela]
puerto = 9000

[cloud]
puerto = 9100
```

El anillo lógico para Ring se construye por orden de `EstacionId`.

---

## 11. Decisiones de diseño

A continuación se resumen las decisiones tomadas durante el diseño, con su justificación.

**Slots como actores participantes del 2PC.** Cada slot es un actor independiente con su propia mailbox. Modelarlo como actor (en lugar de como un campo `struct` adentro de `Estacion`) responde a tres motivos concretos:
1. **Estado tentativo durante el Prepare:** En la fase Prepare del 2PC, el slot debe reservar la bici sin liberarla todavía. Este estado intermedio "reservado pero no liberado" tiene que ser atómico respecto a otras operaciones concurrentes sobre el mismo slot
2. **Voto significativo:** El slot vota `No` legítimamente si está vacío, si la bici no es la esperada, o si ya está reservado para otra transacción. No es un participante "decorativo": tiene reglas locales que puede rechazar
3. **Aislamiento de fallas:** Si en el futuro un slot necesita lógica más compleja (timeout de reserva, recuperación tras crash, sensores adicionales), está aislada del resto de la estación. La complejidad se contiene en el actor `Slot` sin contaminar `Estacion`.

**Comunicador separado de Estación, Pasarela y Cloud.** Aísla la lógica de red (conexiones, reintentos, encolado por desconexión) del actor de negocio. Permite testear la lógica sin red y centraliza el manejo de fallas de conectividad en un único lugar.

**2PC en el flujo de alquiler, no en la devolución.** En el alquiler, la "abort" tiene un significado claro: no se libera la bici. En la devolución, la bici se acepta físicamente siempre (no se puede "rechazar" una vez que está en el slot), por lo que el patrón natural es secuencial con reintentos idempotentes, no 2PC.

**Líder elegido por algoritmo Ring.** Se elige Ring sobre Bully porque su mensaje `Election` viaja secuencialmente por el anillo, lo que facilita rastrear el estado "en elección" — cada nodo que ya forwardeo el mensaje conoce el estado.

**Costo y escalabilidad.** En Ring, una elección requiere `2N` mensajes (una vuelta de `Election` más una vuelta de `Coordinator`), donde `N` es el número de estaciones. Para las topologías que probaremos en el TP (entre 3 y 5 estaciones), esto representa un volumen bajo y aceptable. Para una red urbana grande con decenas de estaciones, el costo de cada elección crecería linealmente y la latencia sería notable, dado que el algoritmo es secuencial.

**Mitigacion a futuro.** Una topología más escalable agruparía las estaciones por zonas geográficas y elegiría un líder por zona, comunicando solo a los líderes entre sí. Esto está fuera del alcance del TP, pero se menciona como mejora natural si el sistema escalara a más nodos. Encaja además con el requerimiento general del enunciado de minimizar tráfico comunicándose solo con nodos cercanos.

**Pasarela centralizada (single instance).** El enunciado no exige una pasarela distribuida. Una pasarela única simula realisticamente al banco/procesador externo, simplifica el diseño y no compromete los requerimientos. La replicación de la pasarela se menciona como mejora futura.

**Tarifa calculada en la pasarela.** Las estaciones y el líder solo manejan timestamps. La pasarela tiene la lógica de tarifas (base + por_minuto). Esto desacopla: cambios de precios no afectan al resto del sistema.

**El registro de alquileres es source of truth en el líder, pero cada estación tiene su propia copia local de sus alquileres propios.** Esto permite reconstruir el registro consultando a las estaciones cuando se elige un nuevo líder. La fuente última de cada alquiler es la estación que lo originó.

**Idempotencia generalizada vía IDs únicos.** Cada mensaje crítico al líder lleva un `event_id`; cada alquiler tiene un `rental_id`; cada pago tiene un `preauth_id`. Los actores mantienen los conjuntos de IDs ya procesados para responder idempotente a reintentos. Esto permite reintentos seguros durante elecciones y caídas parciales.

**Reporte al líder asíncrono.** Tras un alquiler exitoso, el reporte al líder se hace en background (Comunicador encola si el líder no está disponible). El usuario no espera por esto. Si el líder está caído al momento del alquiler, el alquiler sucede igual; el reporte llega cuando se elige nuevo líder.

**TCP para crítico, UDP para estado periódico.** TCP donde no se puede perder un mensaje (alquileres, devoluciones, pagos, Ring de elección). UDP solo para el estado agregado periódico de cada estación al líder, que se renueva con frecuencia y donde una pérdida individual es tolerable.

**Cloud como gateway de consultas.** El cloud no es source of truth y no participa de elecciones. Su rol es ser el endpoint estable para las consultas de disponibilidad de los usuarios, reenviándolas al líder actual. Justifica el uso del rol de líder como agregador. Se notifica al cloud por push cuando se elige nuevo líder.

**Conocimiento de la topología por configuración estática.** Cada estación arranca con un archivo de config que lista todas las estaciones (id, dirección, ubicación). El anillo se construye por orden de `EstacionId`. El descubrimiento dinámico se deja como mejora futura.

**Persistencia mínima en archivos JSON.** Cumple el requerimiento de no usar base de datos ni GUI. Cada actor con estado importante (Estacion, ProcesadorPagos) serializa periódicamente y al cambiar estados críticos. Al reiniciar, lee del archivo y reanuda.

**Estados de usuario y alquiler como `enum`.** Modelar los estados como `enum` con variantes con datos asociados impide construir estados inválidos por construcción (por ejemplo, un usuario no puede "consultar bici" si está en variante `SinBici`).

---

## 12. Cambios desde la primera entrega

Esta sección documenta los cambios realizados al diseño original en respuesta a las correcciones recibidas durante la evaluación de la primera entrega. Las secciones 1 a 11 permanecen tal como fueron entregadas originalmente; todo lo que se modifica o agrega al diseño se describe aquí.

---

### 12.1 Eliminación del proceso `cloud`

**Problema identificado:** el proceso `cloud` actúa como un gateway centralizado cuya única función es recibir consultas de disponibilidad de los usuarios y reenviarlas al líder. Esto lo convierte en un punto único de falla sin aportar lógica propia: si cae, ningún usuario puede consultar disponibilidad aunque todas las estaciones sigan funcionando correctamente.

**Cambio:** se elimina el proceso `cloud`. Los usuarios se comunican directamente con las estaciones para todo tipo de interacción.

**Discovery del líder:** el usuario arranca con un archivo de configuración que lista las direcciones de todas las estaciones conocidas. Para encontrar al líder, envía `PreguntarLider` a cualquier estación de esa lista. La estación responde con `RespuestaLider { lider_id, lider_addr, term }` si conoce al líder. Si no, responde según su propio estado: `EnEleccion` si hay una elección en curso, o `LiderDesconocido` si todavía no aprendió quién es el líder (estado `Desconocido`). En ambos casos el usuario reintenta con backoff hasta obtener un `RespuestaLider`.

**Consultas de disponibilidad:** una vez que el usuario conoce al líder, le envía `ConsultaDisponibilidad` directamente. Si el líder no responde, el usuario descarta su referencia y repite el proceso de discovery.

**Nuevos tipos de mensajes reemplazando los de cloud:**

```rust
enum MensajeUsuarioAEstacionConsulta {
    PreguntarLider,
    ConsultaDisponibilidad {
        usuario_id: UsuarioId,
        ubicacion: (f64, f64),
        radio_max_km: f64,
    },
}

enum MensajeEstacionAUsuarioConsulta {
    RespuestaLider {
        lider_id: EstacionId,
        lider_addr: SocketAddr,
        term: u64,
    },
    EnEleccion,
    LiderDesconocido,
    RespuestaDisponibilidad {
        estaciones: Vec<InfoEstacion>,
    },
}
```

`EnEleccion` y `LiderDesconocido` se distinguen a propósito: la primera indica que la estación está participando activamente de un Ring en curso; la segunda, que es un follower que todavía no recibió un `Coordinator`. El usuario las trata igual (reintenta), pero la distinción es útil para logging y diagnóstico.

**Demultiplexado en la estación:** el usuario manda dos familias de mensajes a la misma estación por TCP — `MensajeUsuarioAEstacion` (alquiler/devolución, sección 5.1) y `MensajeUsuarioAEstacionConsulta` (discovery/consulta). Para distinguirlas en un único endpoint, ambas viajan envueltas en un enum sobre:

```rust
enum MensajeUsuario {
    Operacion(MensajeUsuarioAEstacion),
    Consulta(MensajeUsuarioAEstacionConsulta),
}
```

**Cambios en `struct Usuario`:** se reemplaza `cloud_addr: SocketAddr` por `lider_conocido: Option<(EstacionId, SocketAddr, u64)>` (id, dirección y `term`, este último para descartar respuestas de discovery más viejas). El campo `estaciones_conocidas` pasa a incluir también la ubicación de cada estación: `HashMap<EstacionId, (SocketAddr, (f64, f64))>`.

**Cambios en el algoritmo Ring:** el paso 7 ("Anuncio al cloud") desaparece. El nuevo líder no necesita notificar a ningún proceso centralizado; los usuarios que consulten durante una elección reciben `EnEleccion` o `LiderDesconocido` y reintentarán solos.

**Secciones del diseño original que quedan obsoletas con este cambio** (se listan explícitamente para evitar ambigüedad al leer las secciones 1–11): la sección 4.3 (actor `Cloud`); los enums `MensajeUsuarioACloud` / `MensajeCloudAUsuario` de la sección 5.2; los enums `MensajeCloudAEstacion` / `MensajeEstacionACloud` (incluida la variante `SoyElLider`) de la sección 5.3; el proceso `cloud` dibujado en el diagrama de la sección 2.1. Todo eso se reemplaza por lo descrito en esta subsección.

**Tabla de protocolos actualizada** (reemplaza a la de sección 6):

| Enlace | Protocolo | Justificación |
|---|---|---|
| Usuario ↔ Estación (alquiler, devolución) | TCP | Operaciones críticas, request/response. |
| Usuario ↔ Estación (consulta disponibilidad, discovery del líder) | TCP | Request/response, requiere confiabilidad. |
| Estación ↔ Pasarela | TCP | Involucra dinero; debe garantizar entrega y orden. |
| Estación → Líder (eventos de alquiler) | TCP | Crítico: perder un evento equivale a perder un cobro. |
| Estación ↔ Estación (Ring de elección) | TCP | Protocolo secuencial, requiere orden y entrega. |
| Estación → Líder (estado agregado periódico) | UDP | Frecuente, pérdidas individuales son tolerables (el siguiente snapshot compensa). |

**Nuevo flujo CU3 — Consulta de disponibilidad** (reemplaza al de sección 8.3):

```mermaid
sequenceDiagram
    participant U as Usuario
    participant E as Estación cualquiera
    participant L as Líder

    alt Usuario no conoce líder
        U->>E: PreguntarLider<br/>(TCP, a cualquier estación de la config)
        alt La estación conoce al líder
            E-->>U: RespuestaLider(id, addr, term)
            Note over U: Actualiza lider_conocido
        else La estación no lo conoce
            E-->>U: EnEleccion / LiderDesconocido
            Note over U: Reintenta con backoff
        end
    end

    U->>L: ConsultaDisponibilidad(ubicacion, radio)<br/>(TCP, directo al líder)
    Note over L: Filtra cache_estados<br/>por proximidad
    L-->>U: RespuestaDisponibilidad([estaciones cercanas])
    Note over U: App muestra lista en consola
```

**Estructura del proyecto actualizada** (reemplaza a la de sección 9):

```
proyecto-bicis/
├── Cargo.toml
├── README.md
├── comun/
│   └── src/
│       ├── mensajes/
│       │   ├── usuario_estacion.rs   (incluye ahora los mensajes de consulta/discovery)
│       │   ├── estacion_estacion.rs
│       │   └── estacion_pasarela.rs
│       └── ...
├── estacion/
├── pasarela/
└── usuario/
```

Se eliminan los archivos `usuario_cloud.rs`, `cloud_estacion.rs` y el binario `cloud/`.

**Ejecución actualizada** (reemplaza al esquema de sección 10):

```bash
cargo run --release --bin pasarela -- --puerto 9000 --config pasarela.toml
cargo run --release --bin estacion -- --id 1 --puerto 8001 --config estaciones.toml
cargo run --release --bin estacion -- --id 2 --puerto 8002 --config estaciones.toml
cargo run --release --bin estacion -- --id 3 --puerto 8003 --config estaciones.toml
cargo run --release --bin usuario -- --id alice --config estaciones.toml
cargo run --release --bin usuario -- --id bob --config estaciones.toml
```

El archivo `estaciones.toml` ya no incluye la sección `[cloud]`.

---

### 12.2 Soporte real para el modo desconectado

**Problema identificado:** el diseño original mencionaba el requerimiento de funcionamiento desconectado pero no lo implementaba concretamente. En particular, el 2PC de alquiler requería que la pasarela fuese alcanzable: si una estación perdía conectividad global, el alquiler fallaba. Tampoco estaba especificado qué podía hacer un usuario sin señal.

**Aclaración del profesor:** la distinción entre conectividad "local" y "global" es de alcance de red, no de protocolo. Ambas usan TCP. "Local" significa que el nodo destino es físicamente cercano y accesible directamente; "global" implica alcanzar al líder, la pasarela u otras estaciones de la red.

#### 12.2.1 Usuario sin conectividad global

Un usuario en modo `SoloLocal` puede igualmente alquilar y devolver bicicletas conectándose directamente a la estación cercana por TCP. El flujo de alquiler y devolución en la estación es idéntico al caso normal. Lo único que el usuario no puede hacer en modo `SoloLocal` es consultar disponibilidad global (requiere al líder).

El estado `SoloLocal` es un concepto que aplica al **usuario** (es móvil y puede quedarse sin señal celular, alcanzando solo una estación físicamente cercana). Para la estación, en cambio, no existe un estado "SoloLocal" — ver 12.2.2.

**Cambio en `struct Usuario`:** se agrega el campo `conectividad: EstadoConectividadUsuario`:

```rust
enum EstadoConectividadUsuario {
    Global,    // puede alcanzar al líder para consultas de disponibilidad
    SoloLocal, // solo puede conectarse a estaciones físicamente cercanas
}
```

**Nuevo CU5 — Alquiler con usuario sin conectividad global:**

```mermaid
sequenceDiagram
    participant U as Usuario (SoloLocal)
    participant CE as Comunicador A
    participant E as Estación A

    Note over U: Sin acceso global<br/>conoce dirección de estación cercana
    U->>CE: SolicitudAlquiler (TCP local)
    CE->>E: MensajeRecibido
    Note over E: Procesa alquiler normalmente<br/>(CU1 o Caso E según su propia conectividad)
    E->>CE: AlquilerConfirmado
    CE->>U: AlquilerConfirmado (TCP local)
    Note over U: Obtiene la bici normalmente
```

#### 12.2.2 Estación sin conectividad con la pasarela

A diferencia del usuario, **la estación no tiene un estado "SoloLocal"**: o alcanza un servicio (pasarela, líder) o no lo alcanza, y eso puede variar servicio por servicio. La conectividad deja de ser un campo de `Estacion` y pasa a ser responsabilidad del `Comunicador`, que es quien maneja la red. Esto es coherente con el motivo por el que el `Comunicador` se separó del actor de negocio en primer lugar.

**Cambios en `struct Estacion`:** se agrega únicamente:

```rust
pagos_pendientes: Vec<PagoPendiente>,
```

(Se elimina el campo `conectividad: EstadoConectividad` que proponía la versión anterior de este documento.) La `Estacion` no consulta un flag global: simplemente intenta enviar a través del `Comunicador` y reacciona al resultado (voto recibido vs. timeout).

**Cambios en el `Comunicador`:** lleva el registro de qué servicios alcanza en este momento:

```rust
struct ServiciosAlcanzables {
    pasarela: bool,
    lider: bool,
}
```

Esto se actualiza según el resultado de los intentos de conexión (éxito / timeout) y permite distinguir, por ejemplo, que una estación llegue a la pasarela pero no al líder, o viceversa — algo que un único flag `Global`/`SoloLocal` no capturaba.

El campo `preauth_id` en `struct Alquiler` cambia de `String` a `Option<String>` para representar alquileres iniciados sin alcanzar a la pasarela.

```rust
/// Alquiler completado localmente sin pasar por la pasarela.
/// Se persiste en disco y se procesa cuando se restaura el acceso a la pasarela.
struct PagoPendiente {
    rental_id: RentalId,
    bici_id: BiciId,
    usuario_id: UsuarioId,
    tarjeta: DatosTarjeta,
    T0: Instant,
}
```

**Regla clave: ningún alquiler se reporta al líder sin pre-autorización.**
En el caso normal esto se cumple solo, porque el 2PC incluye la pre-autorización antes de confirmar el alquiler. En modo offline, el reporte al líder se **difiere** hasta haber obtenido la preauth. Como consecuencia, el registro del líder solo contiene alquileres con `preauth_id` válido, y por eso el `preauth_id` que el líder entrega en `DatosParaCobro` (sección 12.3) puede ser `String` (nunca `None`).

**Nuevo Caso E en el 2PC** (se agrega a la sección 7.1.1):

Cuando el `Comunicador` reporta que la pasarela no es alcanzable (timeout de 5 segundos, configurable), la estación omite al participante `ProcesadorPagos` del 2PC. El alquiler se resuelve solo con el voto del `Slot`. Al aceptarlo, la estación:
1. Registra el alquiler en `alquileres_propios` con `preauth_id = None`.
2. Persiste un `PagoPendiente` en disco.
3. Confirma el alquiler al usuario.
4. **No reporta el alquiler al líder** (por la regla anterior: el líder nunca debe conocer un alquiler sin preauth).
5. Cuando el `Comunicador` recupera el acceso a la pasarela, procesa cada `PagoPendiente`: hace la pre-autorización; al obtenerla, completa el alquiler con el `preauth_id` real y recién entonces lo reporta al líder (`AlquilerAbierto`). **El cobro no ocurre acá**: sucede cuando la bici se devuelve, por el flujo normal de CU2.

**Nota de consistencia con la sección 7.1.** En modo offline se relaja deliberadamente la atomicidad estricta del 2PC: la bici puede entregarse antes de que exista una pre-autorización confirmada. Es una decisión consciente que prioriza la disponibilidad (que el usuario pueda sacar la bici aunque la estación no alcance al banco) por sobre la atomicidad. Si la pre-autorización diferida fracasa (p. ej. tarjeta sin fondos), la pasarela marca el cobro como `CobroFallido` (ver sección 12.3); ese estado es dominio de la pasarela, no de la estación.

**Nuevo CU6 — Alquiler offline y posterior reconexión:**

```mermaid
sequenceDiagram
    participant U as Usuario
    participant E as Estación A (sin pasarela)
    participant S as Slot 3

    U->>E: SolicitudAlquiler (TCP local)
    Note over E: Comunicador reporta<br/>pasarela inalcanzable
    E->>S: PrepareLiberacion(tx_id)
    S-->>E: Voto(Yes)
    E->>S: CommitLiberacion(tx_id)
    S-->>E: BiciLiberada
    Note over E: Graba alquiler (preauth_id=None)<br/>Persiste PagoPendiente<br/>NO reporta al líder
    E->>U: AlquilerConfirmado (TCP local)
```

```mermaid
sequenceDiagram
    participant E as Estación A
    participant P as Pasarela
    participant L as Líder

    Note over E: Comunicador detecta<br/>pasarela alcanzable de nuevo

    loop Por cada PagoPendiente
        E->>P: PreparePreauth(tarjeta, monto_propuesto)
        P-->>E: Voto(Yes, preauth_id)
        E->>P: CommitPreauth(preauth_id)
        P-->>E: PreauthConfirmada
        Note over E: Completa el alquiler con preauth_id real<br/>Elimina PagoPendiente del disco
        E->>L: AlquilerAbierto(rental_id, ..., preauth_id)
        L-->>E: ACK
    end

    Note over E,L: El cobro NO ocurre acá:<br/>se ejecuta al devolver la bici (CU2)
```

El segundo diagrama solo se ocupa de regularizar el **alquiler** (obtener preauth y reportar al líder). El cobro y el cierre suceden por el flujo normal de devolución, de modo que no hay que tratar el caso de un `T1` todavía desconocido.

**Tabla de timeouts actualizada** (se agrega a la de sección 7.1.1):

| Timeout | Valor default | Configurable |
|---|---|---|
| Prepare → Voto | 3 segundos | Si |
| Voto Yes → Commit/Abort | 10 segundos | Si |
| Detección de transacción huérfana | 1 hora | Si |
| Intento de conexión a pasarela (antes de resolver el alquiler solo con el Slot) | 5 segundos | Si |

---

### 12.3 Corrección de responsabilidades en el flujo de devolución

**Problema identificado:** en el flujo de devolución original (CU2), es el líder quien llama directamente a la pasarela para ejecutar el cobro (`ProcesarCobro`). Esto mezcla dos responsabilidades distintas en el líder: gestionar el registro de alquileres y coordinar cobros. El líder no debería tener lógica de pagos; eso es dominio de los actores que interactúan directamente con la pasarela (las estaciones). Además, hacer que el cierre del alquiler en la estación de origen dependa exclusivamente del líder abre una ventana de falla: si el líder se cae después de procesar el cobro pero antes de notificar al origen, el origen nunca se entera de que el alquiler se cerró.

**Cambio:** el flujo de devolución se rediseña con dos objetivos. Primero, que la estación que recibe la bici (Estación B) sea quien habla con la pasarela; el líder solo devuelve los datos del alquiler necesarios para cobrar. Segundo, sacar al líder del camino crítico del cierre: el líder le informa a B cuál fue la estación de origen, y **B notifica el cierre tanto al líder (para el registro) como a la estación de origen directamente**. Así, si el líder se cae en cualquier punto posterior a `DatosParaCobro`, B igual cobra y cierra al origen, sin depender de que el líder sobreviva.

**Nuevos tipos de mensaje entre estaciones** (se agregan al enum `MensajeEntreEstacionesTCP` de sección 5.4):

```rust
// Líder → Estación B: datos del alquiler para que Estación B cobre directamente.
// Incluye estacion_origen para que B pueda notificarle el cierre por su cuenta.
DatosParaCobro {
    event_id: EventId,
    rental_id: RentalId,
    preauth_id: String,
    T0: Instant,
    estacion_origen: EstacionId,
},

// Líder → Estación B: el líder todavía no tiene registrado el alquiler de esa bici
// (el reporte del origen no llegó, o el origen sigue offline). B reintenta más tarde.
NoRegistradoAun {
    event_id: EventId,
},
```

El mensaje `CierreAlquiler` (ya definido en la sección 5.4) ahora lo emite la **Estación B hacia la estación de origen**, en lugar del líder hacia el origen.

**Manejo de `NoRegistradoAun`.** Si el líder recibe una `NotificarDevolucion` para una bici cuyo alquiler todavía no figura en su registro, responde `NoRegistradoAun` y la estación que devuelve reintenta con backoff. Esto cubre la carrera en que la bici se devuelve antes de que el origen haya reportado el alquiler (típico tras un alquiler offline que aún no se regularizó). La regla aplica incluso cuando el líder es la propia estación de origen: hasta no haber completado su pre-autorización, responde `NoRegistradoAun`, forzando el orden correcto. Si tras suficientes reintentos el alquiler nunca aparece, la bici se trata como huérfana (sección 8.2.1).

**Nuevo flujo CU2** (reemplaza al diagrama de sección 8.2):

```mermaid
sequenceDiagram
    participant U as Usuario
    participant EB as Estación B
    participant SN as Slot N (B)
    participant L as Líder
    participant P as Pasarela
    participant EA as Estación A (origen)

    U->>EB: SolicitudDevolucion<br/>(TCP)
    EB->>SN: AceptarBici(X)
    SN-->>EB: BiciAsegurada
    EB->>U: DevolucionAceptada<br/>(usuario puede irse)

    Note over EB: Genera event_id=E_001

    EB->>L: NotificarDevolucion(E_001, X, T1)<br/>(TCP)
    alt Líder aún no tiene el alquiler
        L-->>EB: NoRegistradoAun(E_001)
        Note over EB: Reintenta con backoff
    else Líder tiene el alquiler
        L-->>EB: DatosParaCobro(E_001, R1, p123, T0, origen=A)<br/>(TCP)
    end

    EB->>P: ProcesarCobro(p123, T0, T1)<br/>(TCP)
    Note over P: Calcula monto=tarifa(T1-T0)
    P-->>EB: CobroConfirmado(p123, monto=Y)

    par B notifica a ambos (cada uno con reintento idempotente hasta su ACK)
        EB->>L: DevolucionProcesada(E_001, R1, monto=Y)<br/>(TCP)
        L-->>EB: ACK<br/>(marca R1 como Cerrado en el registro)
    and
        EB->>EA: CierreAlquiler(R1, T1, Y)<br/>(TCP)
        EA-->>EB: ACK<br/>(marca R1 como Cerrado localmente)
    end

    EB->>U: DevolucionCompletada(R1, Y, tiempo)<br/>(TCP)
```

**Por qué esto cierra la ventana de falla.** Una vez que B recibió `DatosParaCobro`, tiene todo lo necesario (`preauth_id`, `T0`, `estacion_origen`) para terminar la devolución por su cuenta. Si el líder se cae después de ese punto, B igual cobra a la pasarela, igual le manda `CierreAlquiler` directo a la estación de origen (reintentando hasta el ACK), e igual reintenta `DevolucionProcesada` contra el líder cuando vuelva o contra el nuevo líder electo. La estación de origen queda cerrada sí o sí, sin depender de la supervivencia del líder. Ambas notificaciones son idempotentes (por `rental_id` / `event_id`), así que los reintentos son seguros.

**Cambio en `ProcesadorPagos`** (sección 4.2.1): el origen del mensaje `ProcesarCobro` cambia de "Líder (TCP)" a "Estación destino (TCP)". La pasarela ya no recibe mensajes del líder. Además, se agrega la variante `CobroFallido` a `EstadoPreAuth`, para el caso en que un cobro (por ejemplo, el diferido de un alquiler offline) no pueda completarse por fondos insuficientes. Este estado vive únicamente en la pasarela; las estaciones y el líder no llevan registro del resultado del cobro.

```rust
enum EstadoPreAuth {
    Preparada,
    Activa,
    Cobrada { monto_final: f64 },
    CobroFallido { motivo: String },
    Anulada,
}
```

**Tabla de participantes del CU2 actualizada:**

| Proceso | Rol |
|---|---|
| Usuario | Cliente |
| Estación B (destino) | Asegura la bici, consulta los datos al líder, cobra a la pasarela y notifica el cierre tanto al líder como a la estación de origen |
| Líder | Busca el alquiler en el registro y devuelve los datos de cobro (o `NoRegistradoAun`); actualiza el registro al recibir `DevolucionProcesada`. No ejecuta cobros ni propaga el cierre al origen |
| Pasarela | Calcula el monto y cobra a pedido de Estación B |
| Estación A (origen) | Recibe el `CierreAlquiler` directamente de Estación B |

**Tabla de manejo de errores del CU2 actualizada:**

| Falla | Recuperación |
|---|---|
| Líder caído al notificar devolución | Comunicador de B encola y reintenta. Si hubo re-elección, el nuevo líder responde (o `NoRegistradoAun` hasta terminar la reconstrucción del registro). El flujo continúa. |
| Líder se cae tras dar `DatosParaCobro` | B ya tiene todo lo necesario: cobra a la pasarela, manda `CierreAlquiler` directo a A, y reintenta `DevolucionProcesada` contra el líder (o el nuevo) hasta el ACK. A queda cerrado sin depender del líder. |
| Pasarela caída en `ProcesarCobro` | Estación B reintenta con backoff. La preauth es idempotente: si ya estaba cobrada, la pasarela devuelve el monto previo. |
| Estación B se cae entre `DatosParaCobro` y `ProcesarCobro` | Al reiniciar, B reenvía `NotificarDevolucion` con el mismo `event_id`. El líder responde de nuevo con `DatosParaCobro` y el flujo retoma. |
| Estación origen A caída al recibir el cierre | B reintenta `CierreAlquiler` hasta el ACK (asunción: las estaciones siempre vuelven eventualmente). |
| Alquiler no registrado en el líder (alquiler offline aún no regularizado, o reconstrucción incompleta) | Líder responde `NoRegistradoAun`; B reintenta. Si nunca aparece → bici huérfana (sección 8.2.1). |
| Bici "huérfana" | Ver sección 8.2.1. |
