# AGENTS.md

Guía para agentes (Codex, Claude Code y otros) que trabajen en este repositorio. Es la fuente única: `CLAUDE.md` solo apunta aquí.

## Qué es

`github.com/romycode/qumsg` es un bus de mensajes en proceso, inspirado en Symfony Messenger: `Dispatch` es **síncrono** y, opcionalmente, los mensajes se enrutan a un `Transport` y se consumen de forma asíncrona. El `README.md` es el contrato público de la API: cualquier cambio de comportamiento debe reflejarse allí (y en el README del módulo afectado, `valkey/README.md` o `postgres/README.md`).

Requiere **Go 1.27+**, que permite métodos genéricos.

**Regla: métodos genéricos antes que funciones genéricas.** Si una operación genérica tiene un receptor natural, se expone como método (`bus.Register[T](h)`, `router.Route[T](t)`), no como función del paquete (`qumsg.Register(bus, h)`). Las funciones genéricas quedan para helpers internos sin receptor (`messageName[T]()`) y para lo que Go no permite como método: un método genérico no puede formar parte de una interfaz. Prefiere también tipos parametrizados frente a `any` cuando aporten tipado real.

Idioma: README, documentación, comentarios de los README y CHANGELOG en español; identificadores, comentarios de código Go y descripciones de commit en inglés.

## Comandos

El `Makefile` agrupa los comandos habituales (`make` lista los objetivos):

| Objetivo | Qué hace |
| --- | --- |
| `make test` | Tests unitarios de todos los módulos, con `-race`. No necesita servicios. |
| `make test-integration` | Levanta Valkey y PostgreSQL (`compose.yaml`) y ejecuta los tests con el tag `integration` de `valkey/` y `postgres/`. |
| `make test-valkey`, `make test-postgres` | La integración de un solo módulo. |
| `make check` | Formato + vet + todos los tests (unitarios e integración). Antes de cada commit. |
| `make cover`, `make bench` | Cobertura; benchmarks en un contenedor con recursos limitados. |
| `make tidy` | `go mod tidy` en los tres módulos, los anidados contra el núcleo publicado y con credenciales para el repo privado. |
| `make up` / `down` / `clean` | Servicios de `compose.yaml`; `clean` también borra volúmenes y la caché de tests. |

Por debajo:

```sh
go test -race ./... ./valkey/... ./postgres/...                  # todos los tests unitarios

# Integración (Valkey y PostgreSQL en Docker)
docker compose up -d --wait valkey postgres
go test -race -tags integration ./valkey/... ./postgres/...

go test -race ./...                                              # solo el núcleo
go test -race -run TestCancellationIsCheckedBetweenHandlers .    # un test
go vet ./... ./valkey/... ./postgres/... && go vet -tags integration ./valkey/... ./postgres/...
gofmt -l *.go memory valkey postgres
```

Variables: `VALKEY_ADDR` (por defecto `localhost:6379`) y `POSTGRES_URL` (por defecto `postgres://qumsg:qumsg@localhost:5432/qumsg?sslmode=disable`) apuntan los tests de integración a otro servidor; `VALKEY_PORT` y `POSTGRES_PORT` cambian el puerto publicado por `compose.yaml`. Para no chocar con otra copia del repo, usa un proyecto de compose propio: `docker compose -p qumsg-x up -d --wait postgres` y `docker compose -p qumsg-x down -v` al acabar.

Benchmarks (`bench_test.go`) en un contenedor con recursos limitados (2 CPUs y 512m por defecto): `docker compose run --rm bench`, o `BENCH_CPUS=4 BENCH_MEMORY=1g BENCH=Parallel BENCH_COUNT=10 docker compose run --rm bench`. Go ajusta `GOMAXPROCS` al límite de CPU del contenedor; el sufijo `-N` de cada benchmark indica el valor usado.

`.zzz/` es código anterior aparcado (otro diseño con SQS) y no forma parte del módulo: Go ignora los directorios que empiezan por punto. No lo modifiques ni lo incluyas en commits salvo que se pida.

## Arquitectura

Es un **workspace de Go** (`go.work`) con tres módulos: el núcleo (raíz), `valkey/` (`github.com/romycode/qumsg/valkey`, requiere `valkey-go`) y `postgres/` (`github.com/romycode/qumsg/postgres`, requiere `pgx/v5`). Cada módulo anidado tiene su propio `go.mod`, que requiere el núcleo **publicado**. Desde la raíz `./...` no incluye los módulos anidados. Si un módulo anidado necesita algo nuevo del núcleo, hay que publicar antes una versión del núcleo y subir el `require`; el `go.work` solo lo oculta en local (aun así Go descarga el `go.mod` de la versión requerida, que debe existir). Para `go mod tidy` en los módulos anidados (repo privado) usa `make tidy`, que pone `GOWORK=off GOPRIVATE='github.com/romycode/*'` y credenciales de `gh` por variables `GIT_CONFIG_*`, sin tocar la config global.

`bus.go` contiene el bus síncrono (`Dispatch` y las opciones de `New`); `types.go`, `qumsg.Type` (`TypeOf`, `RegisterFor`); `router.go`, el `Router` (`Route`, `RouteTypes`, `WithRouter`); `transport.go`, la parte asíncrona (`Transport`, `Delivery`, `Decoder`, `Consume`); `envelope.go`, los metadatos (`Envelope`, `Propagator`, `WithPropagators`); `memory/`, el transporte en memoria. Los tests usan paquetes externos (`qumsg_test`, `memory_test`, `valkey_test`, `postgres_test`), así que solo ven la API pública:

- `bus_test.go`, `transport_test.go`, `memory/memory_test.go`, `valkey/valkey_test.go`, `postgres/postgres_test.go`: unitarios. Los que dependen de tiempo o bloqueos usan `testing/synctest` (reloj falso y `synctest.Wait`), nunca `time.Sleep` real. Los unitarios de los módulos anidados no necesitan servidor: validan antes de tocar la red o usan un `DB` falso.
- `example_test.go`: aceptación. Cada `Example` reproduce una sección del README con su `// Output:`; si cambias un ejemplo del README o el comportamiento que describe, actualiza ambos. Fija la salida de lo que no sea determinista (p. ej. `time` y `duration` del logger).
- Integración: `valkey/integration_test.go`, `valkey/example_test.go`, `postgres/integration_test.go` y `postgres/example_test.go`, tras el build tag `integration`, contra los servicios de `compose.yaml`. Contra un servidor real el tiempo es real: usa esperas cortas y acotadas, nunca `synctest`.

Detalles:

- **Resolución por tipo exacto**: `Bus.routes` es `map[reflect.Type]route`. `Register[T]` usa `reflect.TypeFor[T]()` y `Dispatch` usa `reflect.TypeOf(message)`, así que `T` y `*T` son rutas distintas y no hay búsqueda por interfaces (por eso `Register` rechaza `T` interfaz).
- **Handlers tipados**: cada ruta es un `handlers[T]` (`[]func(context.Context, T) error`) que implementa la interfaz interna `route`. La aserción `message.(T)` se hace una vez por dispatch. Los handlers corren en orden de registro, se comprueba `ctx.Err()` antes de cada uno y el primer error corta la ejecución.
- **Agnóstico a comandos y eventos**: el bus no distingue entre ellos; sus opciones deciden la política. Por defecto, sin handlers → `ErrNoHandler` y el primer error corta; `AllowNoHandler` devuelve `nil` sin handlers y `ContinueOnError` ejecuta todos y une los errores con `errors.Join` (uno solo, sin envolver). No añadas APIs con semántica de comando o de evento (p. ej. un `Publish`): la aplicación crea un bus por política.
- **`qumsg.Type`**: `TypeOf[T]` captura `T` en closures (`register`, `decode`) para que una lista de tipos sustituya a N llamadas a métodos genéricos, que no pueden estar en interfaces. `RegisterFor` valida toda la lista antes de registrar; `Router.RouteTypes` y los `Accept` de `valkey` y `postgres` además son todo o nada. `checkType[T]` es la validación compartida por `Register` y `TypeOf`.
- **Middleware**: `New` compone una sola vez la cadena de `WithMiddleware` (el primero es el más externo) dos veces: `b.dispatch` alrededor de `b.sendOrHandle` (enruta o procesa) y `b.consume` alrededor de `b.handleReceived` (solo procesa, para `Consume`, e ignora `AllowNoHandler`: un mensaje recibido sin handlers siempre da `ErrNoHandler` para que llegue al `FailureHandler`). Envuelve el dispatch completo, no cada handler, y por eso trabaja con `any`.
- **Alias (`NamedMessage`)**: se lee una vez en `Register` mediante `messageName[T]()`, que para tipos puntero llama a `Name()` sobre `new(Elem)` en vez de sobre `nil` y convierte un pánico de `Name()` en `ErrNamePanic`. `Bus.names` garantiza un alias único por tipo; `Dispatch` nunca resuelve por nombre.
- **Router**: `map[reflect.Type]Transport`, un transporte por tipo, con alias únicos como el bus. Un bus con `WithRouter` ejecuta los handlers locales del tipo y, si está enrutado, lo envía después, como último handler: con error y sin `ContinueOnError` no envía; con `ContinueOnError` envía y une los errores; un tipo enrutado sin handlers no da `ErrNoHandler`. `Consume` despacha con `b.consume`, que nunca enruta, así que productor y worker comparten router sin bucles. Si el transporte es `Decoder`, `Consume` le pasa antes `router.typesFor(t)` (ordenados) o falla con `ErrNoRoutes`.
- **Metadatos**: los transportes mueven `Envelope` (mensaje + `map[string]string` inmutable; `With` copia). Al enviar, el bus crea el sobre y aplica `Inject` de los propagadores; `Consume` aplica `Extract` sobre el `ctx` antes de despachar. Los handlers y el middleware nunca ven el sobre, solo el `ctx`.
- **Asíncrono**: `Consume` lanza N workers que hacen `Receive → Dispatch → Ack/Nack`; el último intento fallido (`Attempt() >= WithMaxAttempts`) va al `FailureHandler` y se confirma. Al cancelar `ctx` deja de recibir, pero despacha y confirma lo recibido con `context.WithoutCancel`. Un error del transporte, también de `Ack`/`Nack`, cancela al resto de workers con `context.WithCancelCause` y `Consume` lo devuelve.
- **`memory.Transport`**: canal acotado que nunca se cierra (para no entrar en pánico al enviar); el cierre se señala con el canal `closed`. `Nack` reencola desde una goroutine (controlada con el `WaitGroup` `retries`) para no bloquear al worker si la cola está llena; el mutex `mu` evita que se inicie un reintento mientras `Close` espera a ese `WaitGroup`.
- **`valkey.Transport`**: Streams con grupo de consumidores. Entrada = campos `name` (alias `Name()`, identificador en el cable), `payload` (codec, JSON por defecto), `attempt` y `metadata` (JSON siempre, `""` si no hay). `Receive` = `XAUTOCLAIM` (mensajes pendientes más de `claimIdle`; intento = `attempt` + entregas extra según `XPENDING`) y si no, `XREADGROUP` con `BLOCK` acotado (valkey-go no cancela comandos bloqueantes por `cancel`, solo por deadline). `Nack` = `XADD` con `attempt+1` + `XACK`. `Accept` (de `qumsg.Decoder`) registra los alias de los tipos enrutados, que decodifica con `Type.Decode`. Nombres no registrados o payload inválido → `UndecodableMessage`, que acaba en el `FailureHandler` vía `ErrNoHandler`. Los tests de integración usan un stream aleatorio por test y provocan errores de Valkey convirtiendo la clave en string (`WRONGTYPE`).
- **`postgres.Transport`**: una fila por mensaje en una tabla que crea la aplicación (`postgres.Schema(table)` da el DDL; nombre validado, `ErrInvalidTable`). Columnas `name` (alias), `payload` (`bytea`, codec), `metadata` (`jsonb`), `dedup_key` (`UNIQUE`, de la interfaz `Deduplicated`), `attempt`, `available_at`, `locked_by`, `locked_at`. `Send` hace `INSERT … ON CONFLICT (dedup_key) DO NOTHING` y `pg_notify` con el `DB` de `WithDB` (por defecto el pool): con un ejecutor que toma la transacción del `ctx`, el mensaje se confirma o se revierte con ella. `ContextWithDelay` retrasa la primera entrega. `Receive` reclama con un solo `UPDATE … WHERE id = COALESCE(<lease caducado>, <disponible>)` con `FOR UPDATE SKIP LOCKED` en autocommit, que incrementa `attempt` y graba `locked_by`/`locked_at`; si no hay nada, espera a un `NOTIFY` (una conexión `LISTEN` propia, fuera del pool, abierta en el primer `Receive` y cerrada en `Close`, que difunde cerrando el canal `wake`), al próximo `available_at`, a que caduque el lease más antiguo o a `WithPollInterval`. Cada entrega renueva su lease cada `lease/3` en una goroutine (`FOR UPDATE NOWAIT`: un `55P03` no cuenta como renovado: se reintenta cada `lease/6` y solo cancela al agotarse el límite seguro) hasta `Ack`/`Nack`. Toda liquidación va cercada por `id + locked_by + attempt`: `Ack` = `DELETE`, `Nack` = liberar con `available_at = now() + WithBackoff(attempt)` y `NOTIFY`; si la fila sigue con otro dueño → `ErrLeaseLost`, si ya no existe → `nil`. `Propagator()` lleva el lease (entrada de metadatos `qumsg.postgres.lease`, `Inject` no hace nada) al `ctx` del handler y `Complete(ctx, db)` borra el mensaje dentro de la transacción del handler. Nombres no aceptados o payload inválido → `postgres.UndecodableMessage`. Los tests de integración crean una tabla aleatoria por test con `Schema`.
- **Concurrencia**: `RWMutex` sobre `routes`/`names`. `Register` hace copy-on-write (`append(slices.Clip(current), h)`) para que un dispatch en curso conserve su slice. El contrato sigue siendo "registrar antes de despachar".

## Errores

Desde el trabajo posterior a v0.7.0: `DeliveryContext` permite cancelar el handler al perder propiedad; `ErrDeliveryLost` en liquidación no para otros consumidores. `WithCancelOnShutdown` opta por cancelar y `Release` (o `Nack` si no existe), con liquidación separada limitada a cinco segundos; el apagado por defecto sigue drenando. `WithUnlimitedAttempts` y `Attempt(ctx)` permiten políticas terminales transaccionales en el handler. PostgreSQL usa token único por reclamación (prefijo `WithOwner` más sufijo aleatorio), `Release` descuenta intento sin permitir ABA, y `Own` bloquea sin borrar dentro de la transacción. El latido cancela ante cualquier renovación fallida; una fila bloqueada (`55P03`) no cuenta como renovación, se reintenta cada `lease/6` (mínimo 10 ms) y solo cancela cuando se agota el límite seguro. Estas reglas sustituyen los detalles anteriores sobre dueño fijo, NOWAIT como renovación y errores de lease que paran el consumidor.

Los errores del bus y de los transportes son variables `Err*` exportadas, envueltas con `%w` y el tipo o mensaje afectado; los errores de los handlers se devuelven **sin envolver**. Mantén ambas propiedades: los tests las comprueban con `errors.Is`/`errors.As` y con igualdad directa.

## Git

Los mensajes de commit siguen [Conventional Commits](https://www.conventionalcommits.org/): `<tipo>[(ámbito)][!]: <descripción>`, con tipos como `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `build`, `ci` o `chore`. Descripción en inglés, en imperativo y sin punto final; un cambio incompatible de la API pública lleva `!` y un pie `BREAKING CHANGE:`. Ejemplos:

```
feat: add typed middleware for a single message type
feat(postgres): add a transport over a PostgreSQL table
refactor!: make Register a generic method

BREAKING CHANGE: qumsg.Register(bus, h) is now bus.Register(h).
```

Cada versión se etiqueta como `vX.Y.Z` (etiqueta anotada) y se documenta en `CHANGELOG.md` (formato Keep a Changelog). Los cambios que aún no tienen versión van en `[Sin publicar]`. Todos los módulos comparten número de versión: cada release etiqueta el núcleo (`vX.Y.Z`) y cada módulo anidado con su ruta (`valkey/vX.Y.Z`, `postgres/vX.Y.Z`) sobre el mismo commit, con un único CHANGELOG. Excepción: si un módulo anidado usa algo nuevo del núcleo, se etiqueta primero el núcleo, y la etiqueta del módulo va en el commit siguiente, el que sube su `require` a esa versión (ver `v0.4.0`).

Para publicar, solo la primera mitad es manual: un commit `chore: release vX.Y.Z` en `main` que mueve `[Sin publicar]` a `[X.Y.Z] - fecha` y actualiza el estado del README, la etiqueta anotada `vX.Y.Z` sobre ese commit y el push de la etiqueta. El workflow `Release` (`.github/workflows/release.yml`) hace el resto: sube el `require` de cada módulo anidado a `vX.Y.Z` con `go get` y `go mod tidy` sin `go.work`, ejecuta `go vet` y los tests unitarios del módulo contra el núcleo publicado, hace commit en `main` si el `require` cambió y etiqueta `<módulo>/vX.Y.Z` sobre ese commit (o sobre el mismo, si no cambió) y, por último, crea la release de GitHub `vX.Y.Z` con `.github/scripts/github-release.sh`, que usa la sección de esa versión del CHANGELOG como notas (si no existe ya) y marca como Latest solo la etiqueta core más alta. El workflow manual `GitHub releases` (`.github/workflows/github-releases.yml`, `workflow_dispatch`) crea las releases que falten para todas las etiquetas `vX.Y.Z`, de la más antigua a la más nueva. Después, `git pull` en local.

El remoto `origin` usa SSH y la clave tiene passphrase, así que el push desde un agente falla. Usa HTTPS con las credenciales de `gh` solo para esa operación, y solo cuando el usuario lo pida:

```sh
git -c credential.helper= -c credential.helper='!gh auth git-credential' push https://github.com/romycode/qumsg.git main
git update-ref refs/remotes/origin/main HEAD
```
