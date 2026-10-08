# Changelog

Todos los cambios relevantes de este proyecto se documentan en este archivo.

El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y el proyecto usa [versionado semántico](https://semver.org/lang/es/). Mientras la versión sea `0.x`, una versión menor puede incluir cambios incompatibles en la API.

## [Sin publicar]

### Cambiado

- **Incompatible:** los transportes trabajan con sobres. `Transport.Send` recibe un `Envelope` en vez del mensaje y `Delivery.Message()` pasa a ser `Delivery.Envelope()`. Quien llame a `Send` directamente envuelve el mensaje con `qumsg.NewEnvelope(msg)`.

### Añadido

- Metadatos de mensaje: `Envelope` envuelve un mensaje con pares clave-valor de tipo string (`NewEnvelope`, `With`, `Get`, `Metadata`) que viajan por el transporte.
- `Propagator` y la opción `WithPropagators`: `Route` y `RouteTypes` copian valores del contexto al sobre y `Consume` los devuelve al contexto del worker antes de despachar. `ContextValue` propaga un string guardado en el contexto.

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

[Sin publicar]: https://github.com/romycode/qumsg/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/romycode/qumsg/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/romycode/qumsg/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/romycode/qumsg/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/romycode/qumsg/releases/tag/v0.1.0
