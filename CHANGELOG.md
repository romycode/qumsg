# Changelog

Todos los cambios relevantes de este proyecto se documentan en este archivo.

El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y el proyecto usa [versionado semántico](https://semver.org/lang/es/). Mientras la versión sea `0.x`, una versión menor puede incluir cambios incompatibles en la API.

## [Sin publicar]

### Cambiado

- **Incompatible:** las rutas pasan del `Router` al bus: `bus.Route[T](transport)` y `bus.RouteTypes(transport, types...)`. Se eliminan `qumsg.Router`, `qumsg.NewRouter` y `qumsg.WithRouter`.
- **Incompatible:** un mensaje enrutado **solo se envía**: `Dispatch` ya no ejecuta antes sus handlers locales, que pasan a ejecutarse únicamente en `Consume`. Un mismo bus puede enviar y consumir sin ejecutar dos veces un handler, y pasar un tipo a asíncrono es añadir su `Route`. Desaparece la combinación con `ContinueOnError`: `Dispatch` devuelve el error de `Send` sin envolver. Para manejar en local y además enviar, el handler local despacha otro mensaje enrutado.
- Los alias son únicos en el bus entre tipos registrados y enrutados (`ErrDuplicateName`).

## [0.8.4] - 2026-10-14

El núcleo y los transportes comparten la versión `v0.8.4`.

### Corregido

- PostgreSQL: una reclamación lenta (por un bloqueo, un pool que abre conexiones o una máquina cargada) ya no cancela una entrega sana. El límite seguro se cuenta desde antes de la reclamación, pero el primer latido se programaba un intervalo después de su respuesta: si la reclamación tardaba más de `lease/3`, la entrega se cancelaba con `ErrLeaseLost` sin intentar renovar, y si tardaba menos, los reintentos tras un `55P03` tenían menos margen. Ahora el primer latido se programa un intervalo después del inicio de la reclamación, como los siguientes desde el inicio de su renovación.
- PostgreSQL: `Ack`, `Nack` y `Release` ya no devuelven un `ErrLeaseLost` falso cuando esperan a que la transacción `Complete` del propio handler confirme el borrado. Su comprobación de existencia leía la fila del snapshot anterior a la espera; ahora, en ese caso, vuelven a comprobarla y devuelven `nil` si el mensaje ya terminó.

### Añadido

- PostgreSQL: prueba de integración de estrés del latido y la liquidación (`TestPostgresHeartbeatStress`), con leases cortos, bloqueos aleatorios de `Own`/`Complete` y semilla fija configurable con `QUMSG_STRESS_SEED`.

## [0.8.3] - 2026-10-13

El núcleo y los transportes comparten la versión `v0.8.3`.

### Corregido

- PostgreSQL: el reintento de una renovación bloqueada (`55P03`) de `v0.8.2` es cada `lease/18` (un sexto del intervalo de latido, mínimo 10 ms), no cada sexto del lease como indicaban la documentación y el changelog de esa versión. Además, la espera entre reintentos no pasa de `safeUntil`, de modo que la cancelación llega en ese instante, y el siguiente latido se programa un intervalo después del inicio de la renovación, no de su final.

## [0.8.2] - 2026-10-12

El núcleo y los transportes comparten la versión `v0.8.2`.

### Corregido

- PostgreSQL: `v0.8.1` solo aplazaba la cancelación cuando la propia transacción del handler (`Own`/`Complete`) bloqueaba la fila durante la renovación (SQLSTATE `55P03`): la entrega seguía perdiéndose al agotarse el límite seguro. Ahora la renovación se reintenta cada `lease/18` (mínimo 10 ms; la nota de `v0.8.3` corrige el «sexto del lease» inicial) hasta que se logra, y un éxito mueve el límite seguro como cualquier renovación. Solo se cancela con `ErrLeaseLost` si se agota el límite seguro o la renovación falla por otro motivo.

## [0.8.1] - 2026-10-12

El núcleo y los transportes comparten la versión `v0.8.1`.

### Corregido

- PostgreSQL: una renovación bloqueada por la propia transacción del handler (`Own`/`Complete`, SQLSTATE `55P03`) ya no cancela la entrega mientras quede margen seguro. No cuenta como renovación ni mueve el límite seguro: se espera al siguiente tick y solo se cancela con `ErrLeaseLost` si el límite se agota o la renovación falla por cualquier otro motivo.

## [0.8.0] - 2026-10-12

Ciclo de vida de entregas: cancelación por pérdida de lease, liberación al apagar e intentos ilimitados. El núcleo y los transportes comparten la versión `v0.8.0`.

### Añadido

- Núcleo: `DeliveryContext`, `ReleasableDelivery`, `ErrDeliveryLost`, `Attempt(ctx)`, `WithUnlimitedAttempts()` y `WithCancelOnShutdown()`. La liquidación tiene un límite de cinco segundos y perder una entrega no detiene otros consumidores.
- PostgreSQL: `Own(ctx, db)` bloquea y comprueba la propiedad dentro de la transacción del handler; `Release` devuelve trabajo interrumpido sin contar intento. Cada reclamación usa un token único para impedir que un contexto antiguo vuelva a ser válido tras liberar y reclamar con el mismo intento.

### Corregido

- PostgreSQL cancela el contexto de la entrega ante renovación fallida o incierta, antes del vencimiento estimado del lease. Una fila bloqueada no cuenta como renovación: su transacción podría revertirse y dejar expuesto un lease caducado. Las renovaciones tienen deadline y se esperan al liquidar.

## [0.7.0] - 2026-10-12

Transporte PostgreSQL transaccional. El núcleo y los transportes Valkey y PostgreSQL comparten la versión `v0.7.0`; el workflow de release actualiza las dependencias de los transportes y publica sus etiquetas.

### Añadido

- Módulo `github.com/romycode/qumsg/postgres`: un transporte sobre una tabla de PostgreSQL con pgx/v5. `Send` puede insertar dentro de la transacción de la aplicación (`WithDB` con un ejecutor que tome la transacción del contexto) y avisa con `NOTIFY`; `Receive` reclama con `FOR UPDATE SKIP LOCKED`, espera con `LISTEN/NOTIFY` y sondeo, y recupera los mensajes cuyo lease caducó. Cada entrega renueva su lease mientras no se liquida, y `Ack`, `Nack` y `Complete` van cercados por dueño e intento (`ErrLeaseLost`). `Nack` reintenta con espera (`WithBackoff`). Incluye mensajes únicos por clave (`Deduplicated`), envío diferido (`ContextWithDelay`), `Schema` con el DDL de la tabla, `UndecodableMessage` y `Propagator` + `Complete` para terminar el mensaje en la transacción del handler.
- `AGENTS.md` con la guía del proyecto para agentes; `CLAUDE.md` apunta a él.

## [0.6.0] - 2026-10-09

El enrutado sale del bus a un `Router` compartido por productor y worker, y los transportes que guardan bytes aprenden los tipos de ese router. Incluye correcciones de `Consume`, del transporte en memoria y de Valkey, y CI en GitHub Actions. El módulo de Valkey se etiqueta como `valkey/v0.6.0` en el commit siguiente, que sube su `require` del núcleo.

### Corregido

- `Consume` ya no respeta `AllowNoHandler`: un mensaje recibido sin handlers (por ejemplo un `valkey.UndecodableMessage`) falla con `ErrNoHandler` y llega al `FailureHandler`, en vez de confirmarse y perderse en silencio. `Dispatch` no cambia.
- `memory.Transport.Send` con el contexto ya cancelado siempre devuelve su error, en vez de encolar o fallar al azar.
- `TypeOf` y `Register` con un tipo puntero definido (`type P *T`) ya no entran en pánico.
- `Router.Route` y `RouteTypes` rechazan con `ErrUncomparableTransport` un transporte cuyo tipo no es comparable, en vez de entrar en pánico al compararlo con `==`.
- `valkey.Transport.Receive` espacia los `XAUTOCLAIM`: como mucho uno por `WithBlock` (o `WithClaimIdle`, si es menor), y seguidos mientras encuentre mensajes. Un consumidor ocupado ya no hace un viaje extra a Valkey por mensaje.
- El README aclara que productor y worker comparten las opciones y el router, no los handlers del trabajo asíncrono: un handler registrado en los dos se ejecuta dos veces.

### Cambiado

- **Incompatible:** el enrutado pasa del bus a un `Router`. `bus.Route[T](t)` y `bus.RouteTypes(t, types...)` se sustituyen por `router.Route[T](t)` y `router.RouteTypes(t, types...)` sobre un `qumsg.NewRouter()`, que el bus recibe con la opción `WithRouter`.
- Un mensaje enrutado se envía después de todos sus handlers locales, como si la ruta fuera el último handler, en vez de en su posición de registro. Con la política por defecto, el error de un handler local impide el envío; con `ContinueOnError` se envía igualmente y los errores se unen.
- **Incompatible:** `valkey.Transport.Register[T]` y `RegisterTypes` desaparecen. El worker comparte el router con el productor y `Consume` pasa al transporte los tipos que recibe.

### Añadido

- CI en GitHub Actions para cada PR y cada push a `main`: formato, `go vet` y tests unitarios y de integración (con un servicio de Valkey) de los dos módulos, con `-race`.
- `Router` (`NewRouter`, `Route[T]`, `RouteTypes`) y la opción `WithRouter`. Cada tipo va a un solo transporte (`ErrAlreadyRouted`) y `RouteTypes` enruta todos los tipos o ninguno.
- `Consume` procesa en local lo que recibe aunque su tipo esté enrutado, así que productor y worker pueden crear el bus con las mismas opciones.
- Interfaz `Decoder` para transportes que necesitan conocer los tipos: `Consume` les pasa con `Accept` los tipos que el router les envía, o devuelve `ErrNoRoutes` si no hay ninguno. `valkey.Transport` la implementa.

## [0.5.0] - 2026-10-09

Metadatos de mensaje: los transportes mueven sobres (`Envelope`) con pares clave-valor que acompañan al mensaje, y los propagadores del bus los copian del contexto del productor al del worker. El módulo de Valkey se etiqueta como `valkey/v0.5.0` en el commit siguiente, que sube su `require` del núcleo.

### Cambiado

- **Incompatible:** los transportes trabajan con sobres. `Transport.Send` recibe un `Envelope` en vez del mensaje y `Delivery.Message()` pasa a ser `Delivery.Envelope()`. Quien llame a `Send` directamente envuelve el mensaje con `qumsg.NewEnvelope(msg)`.

### Añadido

- Metadatos de mensaje: `Envelope` envuelve un mensaje con pares clave-valor de tipo string (`NewEnvelope`, `With`, `Get`, `Metadata`) que viajan por el transporte.
- `Propagator` y la opción `WithPropagators`: `Route` y `RouteTypes` copian valores del contexto al sobre y `Consume` los devuelve al contexto del worker antes de despachar. `ContextValue` propaga un string guardado en el contexto.
- El transporte de Valkey guarda los metadatos del sobre en el campo `metadata` de la entrada, como objeto JSON, y los conserva en los reintentos. Las entradas sin ese campo se leen sin metadatos.

## [0.4.0] - 2026-10-08

El bus pasa a configurarse con opciones y deja de presuponer si despacha comandos o eventos. Incluye listas de tipos de mensaje para evitar repetir el registro tipo a tipo. El módulo de Valkey se etiqueta como `valkey/v0.4.0` en el commit siguiente, que sube su `require` del núcleo.

### Cambiado

- **Incompatible:** `New` recibe opciones en vez de middleware: `qumsg.New(logging)` pasa a ser `qumsg.New(qumsg.WithMiddleware(logging))`.

### Añadido

- Opciones del bus `AllowNoHandler` (un mensaje sin handlers no es un error) y `ContinueOnError` (se ejecutan todos los handlers y sus errores se unen con `errors.Join`; uno solo se devuelve sin envolver). El bus no distingue entre comandos y eventos: cada aplicación configura sus buses.
- `qumsg.Type` y `TypeOf[T]()` para tratar una familia de tipos de mensaje como una lista: `(*Bus).RegisterFor` registra un handler para todos ellos, `(*Bus).RouteTypes` los enruta a un transporte y `Type.Decode` permite a los transportes decodificarlos sin ser genéricos.
- `valkey.Transport.RegisterTypes`, que registra una lista de tipos de una vez, todos o ninguno. `Register[T]` pasa a ser un atajo de `RegisterTypes`.
- Error `ErrZeroType` para un `Type` no creado con `TypeOf`.
- `Makefile` para el entorno local: tests unitarios y de integración, cobertura, benchmarks, formato, `go vet`, `go mod tidy` de los dos módulos y gestión de los contenedores.

## [0.3.0] - 2026-10-08

Primer transporte externo: Valkey Streams, como módulo aparte (`github.com/romycode/qumsg/valkey`, etiqueta `valkey/v0.3.0`). El núcleo no cambia de API; ambos módulos comparten número de versión.

### Añadido

- Módulo `github.com/romycode/qumsg/valkey` con un transporte sobre Valkey Streams: grupo de consumidores compartido entre procesos, `Register[T]` para declarar los tipos que se pueden decodificar, el alias `Name()` como identificador del mensaje en el stream, codec JSON intercambiable, reintentos con el número de intento en el propio mensaje, recuperación con `XAUTOCLAIM` de los mensajes de consumidores caídos, `UndecodableMessage` para nombres desconocidos o payloads inválidos y recorte opcional del stream.
- Servicio `valkey` en `compose.yaml` para los tests de integración (`go test -tags integration ./valkey/...`).
- Workspace de Go (`go.work`) con el núcleo y el módulo de Valkey.

## [0.2.0] - 2026-10-08

Ejecución asíncrona mediante transportes y un primer transporte en memoria.

### Añadido

- Ejecución asíncrona mediante transportes: interfaces `Transport` y `Delivery`, con confirmación explícita (`Ack`/`Nack`) y número de intento.
- `(*Bus).Route[T](transport)` para registrar un handler que envía los mensajes de tipo `T` al transporte en vez de procesarlos.
- `(*Bus).Consume(ctx, transport, opts...)` para consumir mensajes con varios workers (`WithWorkers`), reintentos (`WithMaxAttempts`), un handler para los mensajes que agotan sus intentos (`WithFailureHandler`) y apagado ordenado al cancelar el contexto.
- Paquete `qumsg/memory` con un transporte en memoria: cola acotada que bloquea al llenarse, retraso de reintento configurable y cierre que deja vaciar la cola.
- Errores `ErrTransportClosed` y `ErrNilTransport`.

## [0.1.0] - 2026-10-08

Primera versión: bus de mensajes síncrono en proceso. Requiere Go 1.27+.

### Añadido

- `New(middlewares ...Middleware) *Bus` para crear un bus con su cadena de middleware. El primer middleware es el más externo y la cadena envuelve el dispatch completo, no cada handler.
- `(*Bus).Register[T](handler)` para registrar handlers tipados `func(context.Context, T) error`. `T` se infiere del handler y se admiten funciones, closures y métodos enlazados.
- `(*Bus).Dispatch(ctx, message)` con resolución por tipo exacto: `T` y `*T` son tipos distintos y no se buscan handlers por interfaces.
- Varios handlers por tipo, ejecutados en orden de registro y con parada en el primer error, que se devuelve sin envolver.
- Propagación de `context.Context` desde `Dispatch` hasta los handlers, pasando por el middleware, y comprobación de cancelación antes de cada handler.
- `NamedMessage` para dar a un tipo un alias opcional, único por bus y validado al registrar.
- Errores exportados, comparables con `errors.Is`: `ErrNoHandler`, `ErrNilMessage`, `ErrNilHandler`, `ErrInterfaceType`, `ErrEmptyName`, `ErrNamePanic` y `ErrDuplicateName`.
- Uso concurrente seguro una vez configurado, sin asignaciones de memoria en el camino de dispatch.
- Ejemplos del README verificados como tests (`example_test.go`) y benchmarks ejecutables en un contenedor con recursos limitados (`docker compose run --rm bench`).

[Sin publicar]: https://github.com/romycode/qumsg/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/romycode/qumsg/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/romycode/qumsg/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/romycode/qumsg/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/romycode/qumsg/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/romycode/qumsg/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/romycode/qumsg/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/romycode/qumsg/releases/tag/v0.1.0
