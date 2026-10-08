# Changelog

Todos los cambios relevantes de este proyecto se documentan en este archivo.

El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y el proyecto usa [versionado semántico](https://semver.org/lang/es/). Mientras la versión sea `0.x`, una versión menor puede incluir cambios incompatibles en la API.

## [Sin publicar]

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

[Sin publicar]: https://github.com/romycode/qumsg/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/romycode/qumsg/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/romycode/qumsg/releases/tag/v0.1.0
