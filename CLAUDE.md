# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Qué es

`github.com/romycode/qumsg` es un bus de mensajes **síncrono** en proceso, inspirado en Symfony Messenger. El `README.md` es el contrato público de la API: cualquier cambio de comportamiento debe reflejarse allí.

Requiere **Go 1.27+**: `(*Bus).Register[T]` es un método genérico. Prefiere métodos genéricos y tipos parametrizados frente a `any` cuando aporten tipado real.

## Comandos

```sh
go test -race ./...                                              # todos los tests
go test -race -run TestCancellationIsCheckedBetweenHandlers .    # un test
go vet ./...
gofmt -l *.go

# Benchmarks (bench_test.go) en un contenedor con recursos limitados (2 CPUs y 512m por defecto)
docker compose run --rm bench
BENCH_CPUS=4 BENCH_MEMORY=1g BENCH=Parallel BENCH_COUNT=10 docker compose run --rm bench
```

Go ajusta `GOMAXPROCS` al límite de CPU del contenedor; el sufijo `-N` de cada benchmark indica el valor usado.

`.zzz/` es código anterior aparcado (otro diseño con SQS) y no forma parte del módulo: Go ignora los directorios que empiezan por punto. No lo modifiques ni lo incluyas en commits salvo que se pida.

## Arquitectura

Todo está en `bus.go`. Los tests usan el paquete externo `qumsg_test`, así que solo ven la API pública:

- `bus_test.go`: unitarios.
- `example_test.go`: aceptación. Cada `Example` reproduce una sección del README con su `// Output:`; si cambias un ejemplo del README o el comportamiento que describe, actualiza ambos. Fija la salida de lo que no sea determinista (p. ej. `time` y `duration` del logger).
- Integración: no hay, porque el paquete no tiene dependencias externas. Si se añade un transporte, sus tests irán tras un build tag `integration`.

- **Resolución por tipo exacto**: `Bus.routes` es `map[reflect.Type]route`. `Register[T]` usa `reflect.TypeFor[T]()` y `Dispatch` usa `reflect.TypeOf(message)`, así que `T` y `*T` son rutas distintas y no hay búsqueda por interfaces (por eso `Register` rechaza `T` interfaz).
- **Handlers tipados**: cada ruta es un `handlers[T]` (`[]func(context.Context, T) error`) que implementa la interfaz interna `route`. La aserción `message.(T)` se hace una vez por dispatch. Los handlers corren en orden de registro, se comprueba `ctx.Err()` antes de cada uno y el primer error corta la ejecución.
- **Middleware**: `New` compone la cadena una sola vez alrededor de `b.handle` (el primero es el más externo). Envuelve el dispatch completo, no cada handler, y por eso trabaja con `any`.
- **Alias (`NamedMessage`)**: se lee una vez en `Register` mediante `messageName[T]()`, que para tipos puntero llama a `Name()` sobre `new(Elem)` en vez de sobre `nil` y convierte un pánico de `Name()` en `ErrNamePanic`. `Bus.names` garantiza un alias único por tipo; `Dispatch` nunca resuelve por nombre.
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

El remoto `origin` usa SSH y la clave tiene passphrase, así que el push desde Claude Code falla. Usa HTTPS con las credenciales de `gh` solo para esa operación:

```sh
git -c credential.helper= -c credential.helper='!gh auth git-credential' push https://github.com/romycode/qumsg.git main
git update-ref refs/remotes/origin/main HEAD
```
