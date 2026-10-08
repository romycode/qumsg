# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Qué es

`github.com/romycode/qumsg` es un bus de mensajes en proceso, inspirado en Symfony Messenger: `Dispatch` es **síncrono** y, opcionalmente, los mensajes se enrutan a un `Transport` y se consumen de forma asíncrona. El `README.md` es el contrato público de la API: cualquier cambio de comportamiento debe reflejarse allí.

Requiere **Go 1.27+**, que permite métodos genéricos.

**Regla: métodos genéricos antes que funciones genéricas.** Si una operación genérica tiene un receptor natural, se expone como método (`bus.Register[T](h)`, `bus.Route[T](t)`), no como función del paquete (`qumsg.Register(bus, h)`). Las funciones genéricas quedan para helpers internos sin receptor (`messageName[T]()`) y para lo que Go no permite como método: un método genérico no puede formar parte de una interfaz. Prefiere también tipos parametrizados frente a `any` cuando aporten tipado real.

## Comandos

El `Makefile` agrupa los comandos habituales (`make` lista los objetivos): `make test`, `make test-integration` (levanta Valkey), `make check` (formato + vet + todos los tests, antes de cada commit), `make cover`, `make bench`, `make tidy` (incluye las credenciales para el módulo privado), `make up`/`down`/`clean`. Por debajo:

```sh
go test -race ./... ./valkey/...                                 # todos los tests unitarios

# Integración del módulo valkey (Valkey en Docker)
docker compose up -d --wait valkey
go test -race -tags integration ./valkey/...

go test -race ./...                                              # solo el núcleo
go test -race -run TestCancellationIsCheckedBetweenHandlers .    # un test
go vet ./... ./valkey/... && go vet -tags integration ./valkey/...
gofmt -l *.go memory valkey

# Benchmarks (bench_test.go) en un contenedor con recursos limitados (2 CPUs y 512m por defecto)
docker compose run --rm bench
BENCH_CPUS=4 BENCH_MEMORY=1g BENCH=Parallel BENCH_COUNT=10 docker compose run --rm bench
```

Go ajusta `GOMAXPROCS` al límite de CPU del contenedor; el sufijo `-N` de cada benchmark indica el valor usado.

`.zzz/` es código anterior aparcado (otro diseño con SQS) y no forma parte del módulo: Go ignora los directorios que empiezan por punto. No lo modifiques ni lo incluyas en commits salvo que se pida.

## Arquitectura

Es un **workspace de Go** (`go.work`) con dos módulos: el núcleo (raíz) y `valkey/` (`github.com/romycode/qumsg/valkey`, con su propio `go.mod` que requiere `qumsg` publicado y `valkey-go`). Desde la raíz `./...` no incluye `valkey/`. Si el módulo de Valkey necesita algo nuevo del núcleo, hay que publicar antes una versión del núcleo y subir el `require`; el `go.work` solo lo oculta en local. Para `go mod tidy` en `valkey/` (repo privado) usa `make tidy`, que pone `GOWORK=off GOPRIVATE='github.com/romycode/*'` y credenciales de `gh` por variables `GIT_CONFIG_*`, sin tocar la config global.

`bus.go` contiene el bus síncrono (`Dispatch` y las opciones de `New`); `types.go`, `qumsg.Type` (`TypeOf`, `RegisterFor`, `RouteTypes`); `transport.go`, la parte asíncrona (`Transport`, `Delivery`, `Route`, `Consume`); `envelope.go`, los metadatos (`Envelope`, `Propagator`, `WithPropagators`); `memory/`, el transporte en memoria. Los tests usan paquetes externos (`qumsg_test`, `memory_test`), así que solo ven la API pública:

- `bus_test.go`, `transport_test.go`, `memory/memory_test.go`: unitarios. Los que dependen de tiempo o bloqueos usan `testing/synctest` (reloj falso y `synctest.Wait`), nunca `time.Sleep` real.
- `example_test.go`: aceptación. Cada `Example` reproduce una sección del README con su `// Output:`; si cambias un ejemplo del README o el comportamiento que describe, actualiza ambos. Fija la salida de lo que no sea determinista (p. ej. `time` y `duration` del logger).
- Integración: `valkey/integration_test.go` y `valkey/example_test.go`, tras el build tag `integration`, contra el Valkey de `compose.yaml`.

- **Resolución por tipo exacto**: `Bus.routes` es `map[reflect.Type]route`. `Register[T]` usa `reflect.TypeFor[T]()` y `Dispatch` usa `reflect.TypeOf(message)`, así que `T` y `*T` son rutas distintas y no hay búsqueda por interfaces (por eso `Register` rechaza `T` interfaz).
- **Handlers tipados**: cada ruta es un `handlers[T]` (`[]func(context.Context, T) error`) que implementa la interfaz interna `route`. La aserción `message.(T)` se hace una vez por dispatch. Los handlers corren en orden de registro, se comprueba `ctx.Err()` antes de cada uno y el primer error corta la ejecución.
- **Agnóstico a comandos y eventos**: el bus no distingue entre ellos; sus opciones deciden la política. Por defecto, sin handlers → `ErrNoHandler` y el primer error corta; `AllowNoHandler` devuelve `nil` sin handlers y `ContinueOnError` ejecuta todos y une los errores con `errors.Join` (uno solo, sin envolver). No añadas APIs con semántica de comando o de evento (p. ej. un `Publish`): la aplicación crea un bus por política.
- **`qumsg.Type`**: `TypeOf[T]` captura `T` en closures (`register`, `decode`) para que una lista de tipos sustituya a N llamadas a métodos genéricos, que no pueden estar en interfaces. `RegisterFor`/`RouteTypes` validan toda la lista antes de registrar; `valkey.RegisterTypes` además es todo o nada. `checkType[T]` es la validación compartida por `Register` y `TypeOf`.
- **Middleware**: `New` compone una sola vez la cadena de `WithMiddleware` alrededor de `b.handle` (el primero es el más externo). Envuelve el dispatch completo, no cada handler, y por eso trabaja con `any`.
- **Alias (`NamedMessage`)**: se lee una vez en `Register` mediante `messageName[T]()`, que para tipos puntero llama a `Name()` sobre `new(Elem)` en vez de sobre `nil` y convierte un pánico de `Name()` en `ErrNamePanic`. `Bus.names` garantiza un alias único por tipo; `Dispatch` nunca resuelve por nombre.
- **Asíncrono**: `Route[T]` es solo `Register` con un handler que llama a `t.Send`, así que hereda orden, alias y middleware.
- **Metadatos**: los transportes mueven `Envelope` (mensaje + `map[string]string` inmutable; `With` copia). `Route`/`RouteTypes` crean el sobre y aplican `Inject` de los propagadores del bus; `Consume` aplica `Extract` sobre el `ctx` antes de `Dispatch`. Los handlers y el middleware nunca ven el sobre, solo el `ctx`. `Consume` lanza N workers que hacen `Receive → Dispatch → Ack/Nack`; el último intento fallido va al `FailureHandler` y se confirma. Al cancelar `ctx` deja de recibir, pero despacha y confirma lo recibido con `context.WithoutCancel`. Un error del transporte cancela al resto de workers con `context.WithCancelCause`.
- **`memory.Transport`**: canal acotado que nunca se cierra (para no entrar en pánico al enviar); el cierre se señala con el canal `closed`. `Nack` reencola desde una goroutine (controlada con el `WaitGroup` `retries`) para no bloquear al worker si la cola está llena; el mutex `mu` evita que se inicie un reintento mientras `Close` espera a ese `WaitGroup`.
- **`valkey.Transport`**: Streams con grupo de consumidores. Entrada = campos `name` (alias `Name()`, identificador en el cable), `payload` (codec, JSON por defecto), `attempt` y `metadata` (JSON siempre, `""` si no hay). `Receive` = `XAUTOCLAIM` (mensajes pendientes más de `claimIdle`; intento = `attempt` + entregas extra según `XPENDING`) y si no, `XREADGROUP` con `BLOCK` acotado (valkey-go no cancela comandos bloqueantes por `cancel`, solo por deadline). `Nack` = `XADD` con `attempt+1` + `XACK`. `Register[T]` es `RegisterTypes(qumsg.TypeOf[T]())`, que decodifica con `Type.Decode`. Nombres no registrados o payload inválido → `UndecodableMessage`, que acaba en el `FailureHandler` vía `ErrNoHandler`. Los tests de integración (`//go:build integration`) usan un stream aleatorio por test y provocan errores de Valkey convirtiendo la clave en string (`WRONGTYPE`).
- **Concurrencia**: `RWMutex` sobre `routes`/`names`. `Register` hace copy-on-write (`append(slices.Clip(current), h)`) para que un dispatch en curso conserve su slice. El contrato sigue siendo "registrar antes de despachar".

## Errores

Los errores del bus son variables `Err*` exportadas, envueltas con `%w` y el tipo afectado; los errores de los handlers se devuelven **sin envolver**. Mantén ambas propiedades: los tests las comprueban con `errors.Is`/`errors.As` y con igualdad directa.

## Git

Los mensajes de commit siguen [Conventional Commits](https://www.conventionalcommits.org/): `<tipo>[(ámbito)][!]: <descripción>`, con tipos como `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `build` o `chore`. Descripción en inglés, en imperativo y sin punto final; un cambio incompatible de la API pública lleva `!` y un pie `BREAKING CHANGE:`. Ejemplos:

```
feat: add typed middleware for a single message type
refactor!: make Register a generic method

BREAKING CHANGE: qumsg.Register(bus, h) is now bus.Register(h).
```

Cada versión se etiqueta como `vX.Y.Z` (etiqueta anotada) y se documenta en `CHANGELOG.md` (formato Keep a Changelog). Los cambios que aún no tienen versión van en `[Sin publicar]`. Todos los módulos comparten número de versión: cada release etiqueta el núcleo (`vX.Y.Z`) y cada módulo anidado con su ruta (`valkey/vX.Y.Z`) sobre el mismo commit, con un único CHANGELOG. Excepción: si un módulo anidado usa algo nuevo del núcleo, se etiqueta primero el núcleo, y la etiqueta del módulo va en el commit siguiente, el que sube su `require` a esa versión (ver `v0.4.0`).

El remoto `origin` usa SSH y la clave tiene passphrase, así que el push desde Claude Code falla. Usa HTTPS con las credenciales de `gh` solo para esa operación:

```sh
git -c credential.helper= -c credential.helper='!gh auth git-credential' push https://github.com/romycode/qumsg.git main
git update-ref refs/remotes/origin/main HEAD
```
